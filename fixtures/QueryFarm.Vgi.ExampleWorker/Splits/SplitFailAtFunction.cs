using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Splits;

/// <summary>
/// <c>split_fail_at(n, splits, fail_at, fail_in_init := false)</c> — a range-partitioned split
/// scan where the split whose ordinal equals <c>fail_at</c> deliberately fails, either mid-stream
/// (after emitting at least one row, when the range has one to give — <c>errors.test</c>'s "the
/// scan is genuinely partial when it dies" case) or synchronously inside <c>init</c> itself
/// (<paramref name="fail_in_init"/> — <c>poisoned_conn.test</c>'s "a failed init must not return
/// its connection to the pool" case). <c>fail_at := -1</c> means no failure at all — every ordinal
/// is non-negative, so it never matches.
///
/// The error messages are pinned by those tests verbatim: "failed mid-stream" and "refuses to
/// initialize".
/// </summary>
public sealed class SplitFailAtFunction : ITableFunction
{
    public string Name => "split_fail_at";

    public string Description => "Split scan whose designated split fails mid-stream or at init, for error-recovery coverage";

    public bool SupportsSplits => true;

    public int? MaxWorkers => 8;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Named("n", Int64Type.Default),
            TableArgFields.Named("splits", Int64Type.Default),
            TableArgFields.Named("fail_at", Int64Type.Default),
            TableArgFields.Named("fail_in_init", BooleanType.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public PlanResult Plan(TableBindParams bindParams, PlanRequest request)
    {
        var n = bindParams.Arguments.Int64Named("n", 0);
        var splits = bindParams.Arguments.Int64Named("splits", 1);
        var ranges = SplitRanges.Even(n, splits);
        var scanSplits = ranges.Select((r, i) => ScanSplit.Of(SplitPayloadCodec.Encode(i, r.Start, r.End))).ToList();
        return PlanResult.Of(scanSplits);
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var payloads = SplitOnlyGuard.RequireSingle(initParams, Name);
        var (ordinal, start, end) = SplitPayloadCodec.Decode(payloads[0]);
        var failAt = initParams.Arguments.Int64Named("fail_at", -1);
        var failInInit = initParams.Arguments.BoolNamed("fail_in_init", false);

        if (ordinal == failAt && failInInit)
        {
            throw new InvalidOperationException($"split {ordinal} refuses to initialize (fail_in_init=true)");
        }

        return new Producer(ordinal, failAt, start, end, initParams.OutputSchema);
    }

    private sealed class Producer(long ordinal, long failAt, long start, long end, Schema outputSchema) : ITableFunctionProducer
    {
        private const int BatchSize = 500;
        private long _next = start;
        private bool _emittedBeforeFailure;

        public void Produce(OutputCollector output)
        {
            if (ordinal == failAt)
            {
                // Emit one row of real progress first when the range has any, so the scan is
                // genuinely mid-stream (not merely "before any capture began") when it dies —
                // then fail unconditionally on the next tick.
                if (!_emittedBeforeFailure && _next < end)
                {
                    _emittedBeforeFailure = true;
                    var builder = new Int64Array.Builder();
                    builder.Append(_next);
                    _next++;
                    output.Emit(new RecordBatch(outputSchema, [builder.Build()], 1));
                    return;
                }

                throw new InvalidOperationException($"split {ordinal} failed mid-stream (fail_at={failAt})");
            }

            if (_next >= end)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, end - _next);
            var b = new Int64Array.Builder();
            b.Reserve(rows);
            for (var i = 0; i < rows; i++)
            {
                b.Append(_next + i);
            }

            _next += rows;
            output.Emit(new RecordBatch(outputSchema, [b.Build()], rows));
            if (_next >= end)
            {
                output.Finish();
            }
        }
    }
}
