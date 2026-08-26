using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Splits;

/// <summary>
/// <c>split_echo_filters(splits)</c> — reports what <c>plan()</c> itself received (not what
/// per-split <c>init</c> receives — that's <see cref="SplitDynamicFilterFunction"/>'s claim), by
/// baking it into each split's payload at plan time: <c>saw_filters</c> (whether any pushdown
/// filter reached the plan call at all) and <c>n_projection</c> (how many projected column ids it
/// carried). One row per split, ordered by <c>split_ordinal</c>. Backs <c>pushdown.test</c>.
/// </summary>
public sealed class SplitEchoFiltersFunction : ITableFunction
{
    public string Name => "split_echo_filters";

    public string Description => "Reports the pushdown/projection state plan() itself received, one row per split";

    public bool SupportsSplits => true;

    public bool? FilterPushdown => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Named("splits", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("split_ordinal", Int64Type.Default, nullable: true),
            new Field("saw_filters", BooleanType.Default, nullable: true),
            new Field("n_projection", Int64Type.Default, nullable: true),
        ],
        metadata: null);

    public PlanResult Plan(TableBindParams bindParams, PlanRequest request)
    {
        var splits = bindParams.Arguments.Int64Named("splits", 1);
        var sawFilters = PushdownFilterCodec.Decode(request.PushdownFilters) is not null;
        var nProjection = request.ProjectionIds?.Count ?? 0;

        var scanSplits = new List<ScanSplit>();
        for (var i = 0L; i < splits; i++)
        {
            scanSplits.Add(ScanSplit.Of(Encode(i, sawFilters, nProjection)));
        }

        return PlanResult.Of(scanSplits);
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var payloads = SplitOnlyGuard.RequireSingle(initParams, Name);
        var (ordinal, sawFilters, nProjection) = Decode(payloads[0]);

        // plan()'s pushdown_filters answers "did the WORKER see a filter" (baked into the
        // payload above, at plan time — that's this fixture's whole point). Whether the row this
        // split emits actually satisfies it is a SEPARATE question this worker still has to
        // answer correctly, or "the filter narrows the answer" assertion below would fail: DuckDB
        // does not itself re-check a filter a function declared FilterPushdown for. The SAME
        // filter also reaches this split's own init (independently of what plan() saw), so it's
        // re-decoded here rather than threaded through the payload.
        var initDecoded = PushdownFilterCodec.Decode(initParams.PushdownFilters);
        return new Producer(ordinal, sawFilters, nProjection, initDecoded, initParams.OutputSchema);
    }

    private static byte[] Encode(long ordinal, bool sawFilters, long nProjection)
    {
        var bytes = new byte[17];
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(0, 8), ordinal);
        bytes[8] = (byte)(sawFilters ? 1 : 0);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(9, 8), nProjection);
        return bytes;
    }

    private static (long Ordinal, bool SawFilters, long NProjection) Decode(byte[] payload) => (
        BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(0, 8)),
        payload[8] != 0,
        BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(9, 8)));

    private sealed class Producer(long ordinal, bool sawFilters, long nProjection, DecodedFilters? initDecoded, Schema outputSchema)
        : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted)
            {
                output.Finish();
                return;
            }

            _emitted = true;

            var row = new Dictionary<string, object?> { ["split_ordinal"] = ordinal };
            if (!PushdownFilterEvaluator.Matches(initDecoded, row))
            {
                output.Finish();
                return;
            }

            var ordinalBuilder = new Int64Array.Builder();
            ordinalBuilder.Append(ordinal);
            var filtersBuilder = new BooleanArray.Builder();
            filtersBuilder.Append(sawFilters);
            var projectionBuilder = new Int64Array.Builder();
            projectionBuilder.Append(nProjection);

            output.Emit(new RecordBatch(
                outputSchema, [ordinalBuilder.Build(), filtersBuilder.Build(), projectionBuilder.Build()], 1));
            output.Finish();
        }
    }
}
