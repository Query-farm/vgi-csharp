using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>struct_settings(count)</c> — generates a sequence configured entirely by a single
/// STRUCT-typed <c>config</c> DuckDB session setting (fields <c>start: int64</c>,
/// <c>step: int64</c>, <c>label: string</c>) rather than individual scalar settings — demonstrates
/// a struct-shaped setting default round-trips through <c>catalog_attach</c>. Backs
/// <c>settings/struct_settings.test</c>.
/// </summary>
public sealed class StructSettingsFunction : ITableFunction
{
    private const int BatchSize = 1000;

    public string Name => "struct_settings";

    public string Description => "Generate a sequence configured by a struct setting";

    public IReadOnlyList<string> Categories => ["generator", "settings"];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("label", StringType.Default, nullable: true),
        ],
        metadata: null);

    public IReadOnlyList<string> RequiredSettings => ["config"];

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var (start, step, label) = ReadConfig(initParams.Settings);
        return new Producer(count, start, step, label, initParams.OutputSchema);
    }

    private static (long Start, long Step, string Label) ReadConfig(byte[]? settingsBytes)
    {
        var settings = ScalarArgCodec.DecodeSettings(settingsBytes);
        if (!settings.TryGetValue("config", out var array) || array is not StructArray config || config.IsNull(0))
        {
            return (0, 1, "item");
        }

        var structType = (StructType)config.Data.DataType;
        long start = 0, step = 1;
        var label = "item";
        for (var i = 0; i < structType.Fields.Count; i++)
        {
            switch (structType.Fields[i].Name)
            {
                case "start" when config.Fields[i] is Int64Array startArray:
                    start = startArray.GetValue(0) ?? 0;
                    break;
                case "step" when config.Fields[i] is Int64Array stepArray:
                    step = stepArray.GetValue(0) ?? 1;
                    break;
                case "label" when config.Fields[i] is StringArray labelArray:
                    label = labelArray.GetString(0) ?? "item";
                    break;
            }
        }

        return (start, step, label);
    }

    private sealed class Producer(long count, long start, long step, string label, Schema outputSchema) : ITableFunctionProducer
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
            var nBuilder = new Int64Array.Builder();
            var labelBuilder = new StringArray.Builder();

            for (var i = 0; i < rows; i++)
            {
                var index = _next + i;
                nBuilder.Append(start + index * step);
                labelBuilder.Append($"{label}_{index}");
            }

            _next += rows;
            output.Emit(new RecordBatch(outputSchema, [nBuilder.Build(), labelBuilder.Build()], rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
