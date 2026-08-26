using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>double_sequence(count [, batch_size, increment])</c> — <see cref="SequenceFunction"/>'s
/// floating-point sibling. Backs <c>double_sequence.test</c>.
/// </summary>
public sealed class DoubleSequenceFunction : ITableFunction
{
    public string Name => "double_sequence";

    public string Description => "Generates a sequence of floating-point numbers from 0 to n-1";

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("count", Int64Type.Default),
            TableArgFields.Named("batch_size", Int64Type.Default),
            TableArgFields.Named("increment", DoubleType.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", DoubleType.Default, nullable: true)], metadata: null);

    /// <summary>Backs <c>table/table_function_statistics.test</c> — <c>n</c> ranges over
    /// <c>[0, (count-1)*increment]</c>, same shape as <see cref="SequenceFunction.Statistics"/>.</summary>
    public IReadOnlyDictionary<string, Catalog.ColumnStatisticsInput>? Statistics(TableBindParams bindParams)
    {
        var count = bindParams.Arguments.Int64(0);
        if (count <= 0)
        {
            return null;
        }

        var increment = bindParams.Arguments.DoubleNamed("increment", 1.0);
        return new Dictionary<string, Catalog.ColumnStatisticsInput>
        {
            ["n"] = new()
            {
                Min = 0.0,
                Max = (count - 1) * increment,
                HasNull = false,
                HasNotNull = true,
                DistinctCount = count,
            },
        };
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var batchSize = initParams.Arguments.Int64Named("batch_size", 1000);
        var increment = initParams.Arguments.DoubleNamed("increment", 1.0);
        return new Producer(count, Math.Max(1, batchSize), increment, initParams.OutputSchema);
    }

    private sealed class Producer(long count, long batchSize, double increment, Schema outputSchema) : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= count)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(batchSize, count - _next);
            var builder = new DoubleArray.Builder();
            builder.Reserve(rows);
            for (var i = 0; i < rows; i++)
            {
                builder.Append((_next + i) * increment);
            }

            _next += rows;
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
