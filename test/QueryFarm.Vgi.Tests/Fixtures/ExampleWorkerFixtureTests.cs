using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.ExampleWorker.Table;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests.Fixtures;

/// <summary>The example worker's <c>nested_sequence</c> must honour <c>history_size</c> the way the
/// reference Python fixture does: row <c>n</c>'s history is the last <c>history_size</c> values
/// ending at <c>n</c> (default 20). It used to emit all of <c>0..n</c>, 50 million list values for
/// <c>filter_pushdown.test</c>'s 10,000-row calls.</summary>
public class NestedSequenceFixtureTests
{
    private static Int64Array Int64(long value) => new Int64Array.Builder().Append(value).Build();

    private static TableArguments Args(long count, long? historySize = null)
    {
        var named = new Dictionary<string, IArrowArray> { ["batch_size"] = Int64(7) };
        if (historySize is { } size)
        {
            named["history_size"] = Int64(size);
        }

        return new TableArguments([Int64(count)], named);
    }

    /// <summary>Runs the fixture to completion and returns each row's <c>history</c>.</summary>
    private static List<long[]> Histories(TableArguments arguments)
    {
        var function = new NestedSequenceFunction();
        var producer = function.CreateProducer(new TableInitParams
        {
            FunctionName = function.Name,
            Arguments = arguments,
            OutputSchema = function.OutputSchema,
        });

        var histories = new List<long[]>();
        var finished = false;
        while (!finished)
        {
            using var output = new OutputCollector(function.OutputSchema);
            producer.Produce(output);
            if (output.EmittedBatch is { } batch)
            {
                var history = (ListArray)batch.Column("history");
                for (var row = 0; row < batch.Length; row++)
                {
                    var values = (Int64Array)history.GetSlicedValues(row);
                    histories.Add([.. Enumerable.Range(0, values.Length).Select(i => values.GetValue(i)!.Value)]);
                }
            }

            finished = output.Finished;
        }

        return histories;
    }

    private static long[] Range(long first, long last) => [.. Enumerable.Range((int)first, (int)(last - first + 1)).Select(v => (long)v)];

    [Fact]
    public void History_IsTheLastTwentyValues_ByDefault()
    {
        var histories = Histories(Args(30));

        Assert.Equal(30, histories.Count);
        Assert.Equal(Range(0, 0), histories[0]);
        Assert.Equal(Range(0, 19), histories[19]);
        Assert.Equal(Range(1, 20), histories[20]);
        Assert.Equal(Range(10, 29), histories[29]);
    }

    [Fact]
    public void History_HonoursHistorySize()
    {
        var histories = Histories(Args(10, historySize: 3));

        Assert.Equal(Range(0, 0), histories[0]);
        Assert.Equal(Range(0, 2), histories[2]);
        Assert.Equal(Range(7, 9), histories[9]);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Bind_RejectsAHistorySizeBelowOne(long historySize)
    {
        var function = new NestedSequenceFunction();

        var error = Assert.Throws<InvalidOperationException>(() => function.Bind(new TableBindParams
        {
            FunctionName = function.Name,
            Arguments = Args(10, historySize),
            Secrets = new SecretsAccessor(null, isRetry: false),
        }));
        Assert.Equal("Argument 'history_size' must be >= 1", error.Message);
    }
}

/// <summary>A queue-partitioned scan must declare no more parallel readers for a call than it has
/// work items — the per-call <c>max_workers</c> the reference Python fixture returns from its init.
/// With a fixed eight, a 10,000-row <c>partitioned_sequence</c> (one 10,000-row chunk) got eight
/// readers, seven of which only found the queue empty.</summary>
public class PartitionedFixtureReaderTests
{
    private static Int64Array Int64(long value) => new Int64Array.Builder().Append(value).Build();

    private static TableInitParams InitParams(ITableFunction function, long count) => new()
    {
        FunctionName = function.Name,
        Arguments = new TableArguments([Int64(count)], new Dictionary<string, IArrowArray>()),
        OutputSchema = function.OutputSchema,
    };

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(10_000, 1)]
    [InlineData(10_001, 2)]
    [InlineData(20_000, 2)]
    [InlineData(100_000, 8)]
    public void PartitionedSequence_DeclaresOneReaderPerChunk_UpToEight(long count, int readers)
    {
        var function = new PartitionedSequenceFunction();

        Assert.Equal(readers, function.MaxWorkersForCall(InitParams(function, count)));
        Assert.Equal(8, function.MaxWorkers);
    }

    [Theory]
    [InlineData(100, 1)]
    [InlineData(1_000, 1)]
    [InlineData(5_000, 5)]
    [InlineData(200_000, 8)]
    public void FilterEchoPartitioned_DeclaresOneReaderPerChunk_UpToEight(long count, int readers)
    {
        var function = new FilterEchoPartitionedFunction();

        Assert.Equal(readers, function.MaxWorkersForCall(InitParams(function, count)));
        Assert.Equal(8, function.MaxWorkers);
    }

    /// <summary>The init stream header — what DuckDB sizes its readers by — carries the per-call
    /// answer, not the catalog declaration.</summary>
    [Theory]
    [InlineData(10_000, 1)]
    [InlineData(100_000, 8)]
    public async Task InitHeader_CarriesThePerCallReaderCount(long count, long readers)
    {
        var registry = new CatalogRegistry();
        registry.RegisterTable(new PartitionedSequenceFunction());
        var service = new VgiServiceImpl(registry);
        var attach = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });

        var argsType = new StructType([new Field("positional_0", Int64Type.Default, nullable: true)]);
        var args = new RecordBatch(
            new Schema([new Field("args", argsType, nullable: false)], metadata: null),
            [new StructArray(argsType, 1, [Int64(count)], ArrowBuffer.Empty, nullCount: 0)],
            1);
        var bindRequest = new BindRequest
        {
            FunctionName = "partitioned_sequence",
            FunctionType = FunctionType.Table,
            Arguments = RecordBatchIpc.Write(args),
            AttachOpaqueData = attach.AttachOpaqueData,
        };

        var stream = await service.InitAsync(new InitRequest { BindCall = EmbeddedIpc.Encode(bindRequest) });

        Assert.Equal(readers, Assert.IsType<GlobalInitResponse>(stream.Header).MaxWorkers);
    }
}
