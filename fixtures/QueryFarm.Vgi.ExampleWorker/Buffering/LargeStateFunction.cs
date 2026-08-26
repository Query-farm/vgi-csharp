using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// Backs <c>table_buffering_large_state.test</c>/<c>table_buffering_pool_rotation.test</c>:
/// <c>large_state(data TABLE)</c> appends ~1 MiB of durable state per <see cref="Process"/> call
/// (via <see cref="TableBufferingProcessParams.Storage"/> — cross-process by construction, never an
/// in-memory field, so a Combine/finalize worker that lands on a DIFFERENT process than the Sink
/// workers still sees every byte), exercising Arrow IPC large-message handling on the response
/// path. <see cref="Combine"/> always collapses every state_id to the single shared execution id
/// (like <see cref="BufferInputFunction"/>) so the total is read back and emitted as ONE row
/// regardless of how many Sink threads/processes contributed — <c>table_buffering_pool_rotation.test</c>'s
/// <c>count(*) = 1</c> assertion holds even under <c>threads=4</c> parallel Sink ingest.
/// </summary>
public sealed class LargeStateFunction : ITableBufferingFunction
{
    private const string RawNamespace = "raw";
    private const string RawKey = "chunk";

    /// <summary>~1 MiB per <see cref="Process"/> call — with the default 2048-row chunking, a
    /// 100000-row input accumulates ~49 chunks (~49 MiB total), comfortably clearing both
    /// <c>table_buffering_large_state.test</c>'s 32 MiB floor and <c>table_buffering_pool_rotation.test</c>'s
    /// 1 MiB floor.</summary>
    private const int ChunkBytes = 1024 * 1024;

    public string Name => "large_state";

    public string Description => "Accumulates ~1MB of cross-process state per process() call; finalize emits one row with the total byte count";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("v", Int64Type.Default, nullable: true)], metadata: null);

    public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams)
    {
        processParams.Storage.Append(RawNamespace, RawKey, new byte[ChunkBytes]);
        return processParams.ExecutionId;
    }

    public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams) =>
        [combineParams.ExecutionId];

    public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
        new Producer(finalizeParams);

    private sealed class Producer(TableBufferingFinalizeParams finalizeParams) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted)
            {
                output.Finish();
                return;
            }

            var total = finalizeParams.Storage.ScanLog(RawNamespace, RawKey).Sum(chunk => (long)chunk.Length);
            output.Emit(new RecordBatch(finalizeParams.OutputSchema, [new Int64Array.Builder().Append(total).Build()], 1));
            _emitted = true;
            output.Finish();
        }
    }
}
