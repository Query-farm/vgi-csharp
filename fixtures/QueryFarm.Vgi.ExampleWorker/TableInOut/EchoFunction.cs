using Apache.Arrow;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// The M4 table-in-out anchor fixture (mirrors M1's <c>upper_case</c>/M3's <c>sequence</c> role):
/// the simplest possible streaming table-in-out function — passes every input batch straight
/// through unchanged, no finalize. Proves the bare bind→init(INPUT)→exchange wire protocol
/// end-to-end before layering on finalize/buffering.
/// </summary>
public sealed class EchoFunction : ITableInOutFunction
{
    public string Name => "echo";

    public string Description => "Passthrough function that emits each input batch unchanged";

    public IReadOnlyList<string> Categories => ["utility", "debug"];

    public IReadOnlyDictionary<string, string> Tags => new Dictionary<string, string>
    {
        ["category"] = "debug",
        ["type"] = "passthrough",
    };

    // ProjectionPushdown=true: the wire-declared output schema is narrowed automatically
    // (VgiServiceImpl.InitTableInOut), but `input` still arrives FULL WIDTH (every column the
    // subquery below this operator produced) — a naive output.Emit(input) passthrough would then
    // echo the full batch against a narrowed declared schema, misaligning column positions (DuckDB
    // reads column 0 expecting the requested column, gets whichever column happens to be first).
    // EchoProcessor closes this by selecting `initParams.ProjectionIds` (indices into
    // InputSchema/OutputSchema, which are IDENTICAL for a pure passthrough) out of `input` before
    // emitting — same technique EchoBufferingFunction/EchoWitnessFunction already use for the
    // table-buffering and witness-diagnostic siblings of this fixture, ported to the plain
    // streaming Process path. See table_in_out/echo/projection_filters.test.
    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public bool? ProjectionPushdown => true;

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => bindParams.InputSchema;

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new EchoProcessor(initParams.ProjectionIds);

    private sealed class EchoProcessor(IReadOnlyList<long>? projectionIds) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            if (projectionIds is null)
            {
                output.Emit(input);
                return;
            }

            var fields = projectionIds.Select(i => input.Schema.GetFieldByIndex((int)i)).ToList();
            var columns = projectionIds.Select(i => input.Column((int)i)).ToList();
            output.Emit(new RecordBatch(new Schema(fields, metadata: null), columns, input.Length));
        }
    }
}
