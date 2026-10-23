using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;
using QueryFarm.VgiRpc.Wire;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// Continued table-in-out output (<see cref="ContinuedOutput"/>): a reply saying
/// <c>vgi.status: HAVE_MORE_OUTPUT</c> is followed by continuation ticks the exchange answers through
/// <see cref="ITableInOutProcessor.Resume"/>, and the exchange refuses every input out of turn.
/// Also the registration checks on <see cref="ITableInOutFunction.ContinuesOutput"/> and
/// <see cref="ITableInOutFunction.InputRowsPerCall"/>.
/// </summary>
public class ContinuedOutputTests
{
    private static readonly Schema s_input = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);
    private static readonly Schema s_output = new([new Field("i", Int64Type.Default, nullable: true)], metadata: null);

    /// <summary>Emits the values 0..total-1, two per reply.</summary>
    private sealed class Pager(bool continues = true, long? maxWorkers = null, bool inputFromArgs = true, long? rowsPerCall = null)
        : ITableInOutFunction
    {
        public Dictionary<string, string>? ResumeMetadata { get; set; }

        public string Name => "pager";

        public bool InputFromArgs => inputFromArgs;

        public bool ContinuesOutput => continues;

        public int? MaxWorkers => (int?)maxWorkers;

        public long? InputRowsPerCall => rowsPerCall;

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("n", Int64Type.Default)], metadata: null);

        public Schema OutputSchema => s_output;

        public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor(this);

        private sealed class Processor(Pager function) : ITableInOutProcessor
        {
            private long _total;
            private long _next;

            public void Process(RecordBatch input, OutputCollector output)
            {
                _total = ((Int64Array)input.Column(0)).GetValue(0) ?? 0;
                _next = 0;
                EmitPage(output, new Dictionary<string, string> { ["vgi.cache.ttl"] = "5" });
            }

            public void Resume(OutputCollector output) => EmitPage(output, function.ResumeMetadata);

            private void EmitPage(OutputCollector output, Dictionary<string, string>? metadata)
            {
                var builder = new Int64Array.Builder();
                var count = 0;
                for (; _next < _total && count < 2; _next++, count++)
                {
                    builder.Append(_next);
                }

                output.Emit(new RecordBatch(s_output, [builder.Build()], count), metadata, more: _next < _total);
            }
        }
    }

    private static AnnotatedBatch Input(long n) =>
        new(new RecordBatch(s_input, [new Int64Array.Builder().Append(n).Build()], 1), null);

    private static AnnotatedBatch Tick(int rows = 0)
    {
        var builder = new Int64Array.Builder();
        for (var r = 0; r < rows; r++)
        {
            builder.Append(r);
        }

        return new(new RecordBatch(s_input, [builder.Build()], rows),
            new Dictionary<string, string> { [ContinuedOutput.StatusKey] = ContinuedOutput.Continue });
    }

    private static TableInOutExchangeStreamState Open(Pager function) =>
        new(function, function.CreateProcessor(null!));

    /// <summary>Runs one exchange turn, returning the reply's values and metadata.</summary>
    private static (List<long> Values, IReadOnlyDictionary<string, string>? Metadata) Turn(TableInOutExchangeStreamState state, AnnotatedBatch input)
    {
        using var output = new OutputCollector(s_output);
        state.ExchangeAsync(input, output, null, CancellationToken.None).GetAwaiter().GetResult();
        var batch = Assert.IsType<RecordBatch>(output.EmittedBatch);
        var values = ((Int64Array)batch.Column(0)).Values.ToArray().ToList();
        return (values, output.EmittedMetadata);
    }

    private static bool SaysMore(IReadOnlyDictionary<string, string>? metadata) =>
        metadata is not null && metadata.TryGetValue(ContinuedOutput.StatusKey, out var s) && s == ContinuedOutput.HaveMoreOutput;

    [Fact]
    public void ReplyIsContinuedByTicksUntilTheLastSaysNoMore()
    {
        var state = Open(new Pager());

        var first = Turn(state, Input(5));
        Assert.Equal(new long[] { 0L, 1L }, first.Values);
        Assert.True(SaysMore(first.Metadata));
        Assert.Equal("5", first.Metadata!["vgi.cache.ttl"]);

        var second = Turn(state, Tick());
        Assert.Equal(new long[] { 2L, 3L }, second.Values);
        Assert.True(SaysMore(second.Metadata));

        var last = Turn(state, Tick());
        Assert.Equal(new long[] { 4L }, last.Values);
        Assert.False(SaysMore(last.Metadata));

        // The next input is real input again.
        var next = Turn(state, Input(1));
        Assert.Equal(new long[] { 0L }, next.Values);
        Assert.False(SaysMore(next.Metadata));
    }

    [Fact]
    public void TickWithNoOutputPendingIsRefused()
    {
        var state = Open(new Pager());
        var error = Assert.Throws<InvalidOperationException>(() => Turn(state, Tick()));
        Assert.Contains("no output is pending", error.Message);
    }

    [Fact]
    public void TickWithRowsIsRefused()
    {
        var state = Open(new Pager());
        Turn(state, Input(5));
        var error = Assert.Throws<InvalidOperationException>(() => Turn(state, Tick(rows: 1)));
        Assert.Contains("must have no rows", error.Message);
    }

    [Fact]
    public void RealInputWhileOutputIsPendingIsRefused()
    {
        var state = Open(new Pager());
        Turn(state, Input(5));
        var error = Assert.Throws<InvalidOperationException>(() => Turn(state, Input(5)));
        Assert.Contains("expected a continuation tick", error.Message);
    }

    [Fact]
    public void CacheControlOnATicksReplyIsRefused()
    {
        var function = new Pager { ResumeMetadata = new() { ["vgi.cache.ttl"] = "5" } };
        var state = Open(function);
        Turn(state, Input(5));
        var error = Assert.Throws<InvalidOperationException>(() => Turn(state, Tick()));
        Assert.Contains("cache control is only valid on an input's first reply", error.Message);
    }

    [Fact]
    public void MoreFromAFunctionNotDeclaringContinuesOutputIsRefused()
    {
        var state = Open(new Pager(continues: false));
        var error = Assert.Throws<InvalidOperationException>(() => Turn(state, Input(5)));
        Assert.Contains("does not declare ContinuesOutput", error.Message);
    }

    [Fact]
    public void MoreOutsideATableInOutExchangeIsRefused()
    {
        using var output = new OutputCollector(s_output);
        var batch = new RecordBatch(s_output, [new Int64Array.Builder().Build()], 0);
        var error = Assert.Throws<InvalidOperationException>(() => output.Emit(batch, null, more: true));
        Assert.Contains("only valid in a table-in-out function's Process or Resume", error.Message);

        // more: false is a plain emit anywhere.
        output.Emit(batch, null, more: false);
        Assert.Same(batch, output.EmittedBatch);
    }

    [Fact]
    public void RegistrationRefusesContinuedOutputWithOneSharedWorker()
    {
        var error = Assert.Throws<ArgumentException>(() => CatalogRegistry.CheckTableInOutContract(new Pager(maxWorkers: 1)));
        Assert.Contains("MaxWorkers = 1", error.Message);
        CatalogRegistry.CheckTableInOutContract(new Pager(maxWorkers: 4));
        CatalogRegistry.CheckTableInOutContract(new Pager(continues: false, maxWorkers: 1));
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-3L)]
    public void RegistrationRefusesANonPositiveInputRowsPerCall(long rows)
    {
        var error = Assert.Throws<ArgumentException>(() => CatalogRegistry.CheckTableInOutContract(new Pager(rowsPerCall: rows)));
        Assert.Contains("must be a positive row count", error.Message);
    }

    [Fact]
    public void RegistrationRefusesInputRowsPerCallOnANonBlendedFunction()
    {
        var error = Assert.Throws<ArgumentException>(() =>
            CatalogRegistry.CheckTableInOutContract(new Pager(inputFromArgs: false, rowsPerCall: 3)));
        Assert.Contains("applies only to a blended", error.Message);
        CatalogRegistry.CheckTableInOutContract(new Pager(rowsPerCall: 3));
    }
}
