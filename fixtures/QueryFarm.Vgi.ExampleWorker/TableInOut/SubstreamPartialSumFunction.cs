using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// Backs <c>parallel_finalize.test</c>: a streaming table-in-out function WITH a finalize step,
/// accumulating each SUBSTREAM's rows during INPUT (emitting nothing) and emitting exactly ONE
/// partial-sum row per substream at FINALIZE. DuckDB unions every substream's finalize output, so an
/// outer <c>SUM()</c> re-aggregates to the true total — this is the per-substream finalize contract
/// (deliberately NOT globally correct on its own, unlike <see cref="Buffering.SumAllColumnsFunction"/>'s
/// table-buffering Combine-phase aggregate; see that fixture's doc comment for the distinction).
/// </summary>
public sealed class SubstreamPartialSumFunction : ITableInOutFunction
{
    public string Name => "substream_partial_sum";

    public string Description => "Accumulates a per-substream partial sum, emitted once at finalize";

    public IReadOnlyList<string> Categories => ["aggregation"];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public bool HasFinalize => true;

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor(initParams.OutputSchema);

    private sealed class Processor(Schema outputSchema) : ITableInOutProcessor
    {
        private double _sum;
        private bool _emitted;

        public void Process(RecordBatch input, OutputCollector output)
        {
            var column = input.Column(0);
            for (var i = 0; i < input.Length; i++)
            {
                var v = NumericArrayMath.ReadAsDouble(column, i);
                if (v is not null)
                {
                    _sum += v.Value;
                }
            }

            // No output during INPUT — the partial sum only ever appears at FINALIZE.
            output.Emit(new RecordBatch(outputSchema, [new Int64Array.Builder().Build()], 0));
        }

        public void Finalize(OutputCollector output)
        {
            if (_emitted)
            {
                output.Finish();
                return;
            }

            output.Emit(new RecordBatch(outputSchema, [new Int64Array.Builder().Append((long)_sum).Build()], 1));
            _emitted = true;
            output.Finish();
        }
    }
}
