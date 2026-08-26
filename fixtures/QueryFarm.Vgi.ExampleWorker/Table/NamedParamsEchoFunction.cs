using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>named_params_echo(count [, greeting, multiplier, scale, enabled])</c> — echoes each named
/// parameter's (possibly-defaulted) value into an output column per row, proving VARCHAR/BIGINT/
/// DOUBLE/BOOLEAN named arguments round-trip correctly. Backs <c>named_params.test</c>.
/// </summary>
public sealed class NamedParamsEchoFunction : ITableFunction
{
    public string Name => "named_params_echo";

    public string Description => "Echoes named parameter values in output columns";

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("count", Int64Type.Default),
            TableArgFields.Named("greeting", StringType.Default),
            TableArgFields.Named("multiplier", Int64Type.Default),
            TableArgFields.Named("scale", DoubleType.Default),
            TableArgFields.Named("enabled", BooleanType.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("greeting", StringType.Default, nullable: true),
            new Field("value", Int64Type.Default, nullable: true),
            new Field("float_value", DoubleType.Default, nullable: true),
            new Field("enabled", BooleanType.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var greeting = initParams.Arguments.StringNamed("greeting", "hello");
        var multiplier = initParams.Arguments.Int64Named("multiplier", 1);
        var scale = initParams.Arguments.DoubleNamed("scale", 1.0);
        var enabled = initParams.Arguments.BoolNamed("enabled", true);
        return new Producer(count, greeting, multiplier, scale, enabled, initParams.OutputSchema);
    }

    private sealed class Producer(long count, string greeting, long multiplier, double scale, bool enabled, Schema outputSchema)
        : ITableFunctionProducer
    {
        private const int BatchSize = 1000;
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= count)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, count - _next);
            var idBuilder = new Int64Array.Builder();
            var greetingBuilder = new StringArray.Builder();
            var valueBuilder = new Int64Array.Builder();
            var floatBuilder = new DoubleArray.Builder();
            var enabledBuilder = new BooleanArray.Builder();

            for (var i = 0; i < rows; i++)
            {
                var id = _next + i;
                idBuilder.Append(id);
                greetingBuilder.Append(greeting);
                valueBuilder.Append(id * multiplier);
                floatBuilder.Append(id * scale);
                enabledBuilder.Append(enabled);
            }

            _next += rows;
            output.Emit(new RecordBatch(
                outputSchema,
                [idBuilder.Build(), greetingBuilder.Build(), valueBuilder.Build(), floatBuilder.Build(), enabledBuilder.Build()],
                rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
