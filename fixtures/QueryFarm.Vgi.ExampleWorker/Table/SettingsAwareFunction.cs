using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>settings_aware(count)</c> — demonstrates that DuckDB session settings reach a table
/// function: always emits <c>id</c>/<c>greeting</c>(from the <c>greeting</c> setting)/<c>value</c>
/// (<c>id * 2.5 * multiplier</c>, from the <c>multiplier</c> setting), and additionally a
/// <c>details</c> column when the <c>vgi_verbose_mode</c> setting is true — the OUTPUT SCHEMA
/// itself is therefore setting-dependent, resolved once at bind time. Backs
/// <c>settings/settings.test</c>.
/// </summary>
public sealed class SettingsAwareFunction : ITableFunction
{
    private const int BatchSize = 1000;

    public string Name => "settings_aware";

    public string Description => "Generates data demonstrating settings are passed";

    public IReadOnlyList<string> Categories => ["generator", "settings"];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = BaseSchema();

    public IReadOnlyList<string> RequiredSettings => ["vgi_verbose_mode", "greeting", "multiplier"];

    public Schema ResolveOutputSchema(TableBindParams bindParams) =>
        IsVerbose(bindParams.Settings) ? VerboseSchema() : BaseSchema();

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var settings = ScalarArgCodec.DecodeSettings(initParams.Settings);
        var verbose = IsVerbose(initParams.Settings);
        var greeting = settings.TryGetValue("greeting", out var g) ? ScalarArgCodec.ReadScalar(g) as string ?? "Hello" : "Hello";
        var multiplier = settings.TryGetValue("multiplier", out var m) && ScalarArgCodec.ReadScalar(m) is { } mv ? Convert.ToInt64(mv) : 1L;
        return new Producer(count, greeting, multiplier, verbose, initParams.OutputSchema);
    }

    private static bool IsVerbose(byte[]? settingsBytes)
    {
        var settings = ScalarArgCodec.DecodeSettings(settingsBytes);
        if (!settings.TryGetValue("vgi_verbose_mode", out var array))
        {
            return false;
        }

        var value = ScalarArgCodec.ReadScalar(array);
        return value switch
        {
            bool b => b,
            string s => s == "true",
            _ => false,
        };
    }

    private static Schema BaseSchema() => new(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("greeting", StringType.Default, nullable: true),
            new Field("value", DoubleType.Default, nullable: true),
        ],
        metadata: null);

    private static Schema VerboseSchema() => new(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("greeting", StringType.Default, nullable: true),
            new Field("value", DoubleType.Default, nullable: true),
            new Field("details", StringType.Default, nullable: true),
        ],
        metadata: null);

    private sealed class Producer(long count, string greeting, long multiplier, bool verbose, Schema outputSchema) : ITableFunctionProducer
    {
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
            var valueBuilder = new DoubleArray.Builder();
            var detailsBuilder = verbose ? new StringArray.Builder() : null;

            for (var i = 0; i < rows; i++)
            {
                var id = _next + i;
                idBuilder.Append(id);
                greetingBuilder.Append(greeting);
                valueBuilder.Append(id * 2.5 * multiplier);
                detailsBuilder?.Append($"row_{id}");
            }

            _next += rows;

            IArrowArray[] columns = detailsBuilder is null
                ? [idBuilder.Build(), greetingBuilder.Build(), valueBuilder.Build()]
                : [idBuilder.Build(), greetingBuilder.Build(), valueBuilder.Build(), detailsBuilder.Build()];

            output.Emit(new RecordBatch(outputSchema, columns, rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
