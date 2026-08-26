using Apache.Arrow;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Accumulate;

/// <summary>Streams a pre-materialized list of batches, one per tick, then finishes — shared by
/// every accumulate probe that already has its full answer in hand before the producer starts
/// (unlike a lazily-computed producer such as <c>SumAllColumnsFunction</c>'s <c>SumProducer</c>).</summary>
internal sealed class BatchListProducer(IReadOnlyList<RecordBatch> batches) : ITableFunctionProducer
{
    private int _next;

    public void Produce(OutputCollector output)
    {
        if (_next >= batches.Count)
        {
            output.Finish();
            return;
        }

        output.Emit(batches[_next]);
        _next++;
        if (_next >= batches.Count)
        {
            output.Finish();
        }
    }
}
