using Apache.Arrow;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// Backs <c>table_buffering_input_batch_index.test</c>: <c>batch_index_buffer_input(data TABLE)</c>
/// declares <see cref="RequiresInputBatchIndex"/>, tags each stored batch with DuckDB's
/// globally-unique <see cref="TableBufferingProcessParams.BatchIndex"/>, and the FINALIZE producer
/// sorts by that index before replaying — reconstructing source order even when several Sink
/// threads/processes raced to write batches in arbitrary arrival order. Raises if a batch ever
/// arrives with no batch_index (the C++ operator either supplies DuckDB's own partition index or
/// synthesizes one itself — see <c>vgi_table_buffering_impl.cpp</c>'s <c>synthesize_batch_index</c>
/// path — so this should never actually happen once the function opts in via the metadata flag; the
/// check is a backstop against a quietly-wrong answer rather than a case this fixture expects to hit).
/// </summary>
public sealed class BatchIndexBufferInputFunction : ITableBufferingFunction
{
    private const string RawNamespace = "raw";
    private const string RawKey = "data";

    public string Name => "batch_index_buffer_input";

    public string Description => "Buffered passthrough that reconstructs source order via DuckDB's per-chunk batch_index";

    public bool RequiresInputBatchIndex => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => bindParams.InputSchema;

    public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams)
    {
        var batchIndex = processParams.BatchIndex
            ?? throw new InvalidOperationException(
                "batch_index_buffer_input declared Meta.requires_input_batch_index=true but received a " +
                "process() call with no batch_index.");

        var ipc = RecordBatchIpc.Write(batch);
        var blob = new byte[8 + ipc.Length];
        BitConverter.GetBytes(batchIndex).CopyTo(blob, 0);
        ipc.CopyTo(blob, 8);
        processParams.Storage.Append(RawNamespace, RawKey, blob);
        return processParams.ExecutionId;
    }

    public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams) =>
        [combineParams.ExecutionId];

    public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
        new Producer(finalizeParams);

    private sealed class Producer(TableBufferingFinalizeParams finalizeParams) : ITableFunctionProducer
    {
        private Queue<RecordBatch>? _pending;

        public void Produce(OutputCollector output)
        {
            if (_pending is null)
            {
                var sorted = finalizeParams.Storage.ScanLog(RawNamespace, RawKey)
                    .Select(blob => (Index: BitConverter.ToInt64(blob, 0), Batch: RecordBatchIpc.Read(blob[8..])))
                    .OrderBy(entry => entry.Index)
                    .Select(entry => entry.Batch);
                _pending = new Queue<RecordBatch>(sorted);
            }

            if (_pending.Count == 0)
            {
                output.Finish();
                return;
            }

            output.Emit(_pending.Dequeue());
            if (_pending.Count == 0)
            {
                output.Finish();
            }
        }
    }
}
