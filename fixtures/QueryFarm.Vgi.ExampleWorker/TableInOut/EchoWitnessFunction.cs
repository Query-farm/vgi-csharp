using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// Worker-side instrumentation fixture for table-in-out projection pushdown
/// (<c>table_in_out/echo/pushdown_witness.test</c>): every output column carries the integer
/// count of columns the worker actually observed in its narrowed
/// <see cref="TableInOutInitParams.ProjectedSchema"/> for that call — i.e. proof that DuckDB's
/// projection narrowing reached the wire, not just DuckDB's own above-operator column dropping.
/// Column NAMES mirror the input's own column names (so <c>SELECT a FROM
/// echo_witness(...)</c> still resolves), but every VALUE is the witness count, not the
/// original input data.
/// </summary>
public sealed class EchoWitnessFunction : ITableInOutFunction
{
    public string Name => "echo_witness";

    public string Description => "Emits, per column, the count of columns the worker actually received after projection narrowing";

    public IReadOnlyList<string> Categories => ["utility", "debug"];

    public bool? ProjectionPushdown => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) =>
        new(bindParams.InputSchema.FieldsList.Select(f => new Field(f.Name, Int64Type.Default, nullable: false)), metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new WitnessProcessor(initParams.ProjectedSchema);

    private sealed class WitnessProcessor(Schema projectedSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var rows = input.Length;
            var witnessValue = projectedSchema.FieldsList.Count;

            var columns = projectedSchema.FieldsList
                .Select(_ =>
                {
                    var builder = new Int64Array.Builder();
                    builder.Reserve(rows);
                    for (var i = 0; i < rows; i++)
                    {
                        builder.Append(witnessValue);
                    }

                    return (IArrowArray)builder.Build();
                })
                .ToList();

            output.Emit(new RecordBatch(projectedSchema, columns, rows));
        }
    }
}
