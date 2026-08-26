using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;

namespace QueryFarm.Vgi.ExampleWorker.Accumulate;

/// <summary>
/// <c>accumulate_clear(name VARCHAR)</c> — deletes a collection (schema pin + every segment),
/// returning <c>(name, rows_cleared)</c>. Clearing an unknown (or already-cleared) name is not an
/// error — it just reports <c>rows_cleared = 0</c>.
/// </summary>
public sealed class AccumulateClearFunction : ITableFunction
{
    public string Name => "accumulate_clear";

    public string Description => "Deletes an accumulated collection, reporting rows removed";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("name", StringType.Default, nullable: false)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("name", StringType.Default, nullable: false),
            new Field("rows_cleared", Int64Type.Default, nullable: false),
        ], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var name = initParams.Arguments.StringPositional(0);
        var removed = new AccumulateStore(initParams.AttachOpaqueData, name).Clear();

        var batch = new RecordBatch(
            OutputSchema,
            [
                new StringArray.Builder().Append(name).Build(),
                new Int64Array.Builder().Append(removed).Build(),
            ], 1);

        return new BatchListProducer([batch]);
    }
}
