using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// <c>filter_by_setting(data TABLE)</c> — filters input rows where the <c>value</c> column is
/// &gt;= the <c>threshold</c> DuckDB session setting. Demonstrates a table-in-out function reading
/// a setting inside <see cref="ITableInOutProcessor.Process"/> (not just at bind time). Backs
/// <c>settings/filter_by_setting.test</c>.
/// </summary>
public sealed class FilterBySettingFunction : ITableInOutFunction
{
    public string Name => "filter_by_setting";

    public string Description => "Filter rows where value column >= threshold setting";

    public IReadOnlyList<string> Categories => ["transform", "settings"];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public IReadOnlyList<string> RequiredSettings => ["threshold"];

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => bindParams.InputSchema;

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) =>
        new Processor(initParams.Settings, initParams.OutputSchema);

    private sealed class Processor(byte[]? settingsBytes, Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var valueIndex = outputSchema.GetFieldIndex("value");
            if (valueIndex < 0 || input.Length == 0)
            {
                output.Emit(input);
                return;
            }

            var settings = ScalarArgCodec.DecodeSettings(settingsBytes);
            var threshold = settings.TryGetValue("threshold", out var t) && ScalarArgCodec.ReadScalar(t) is { } tv
                ? Convert.ToInt64(tv)
                : 0L;

            var valueColumn = input.Column(valueIndex);
            var keep = new List<int>();
            for (var i = 0; i < input.Length; i++)
            {
                var raw = ScalarArgCodec.ReadScalar(valueColumn, i);
                if (raw is not null && Convert.ToInt64(raw) >= threshold)
                {
                    keep.Add(i);
                }
            }

            if (keep.Count == input.Length)
            {
                output.Emit(input);
                return;
            }

            var columns = Enumerable.Range(0, input.ColumnCount)
                .Select(c => RowSelector.Select(input.Column(c), keep))
                .ToList();
            output.Emit(new RecordBatch(outputSchema, columns, keep.Count));
        }
    }
}
