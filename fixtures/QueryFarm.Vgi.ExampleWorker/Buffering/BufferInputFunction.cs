using Apache.Arrow;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// The generic table-buffering passthrough backing <c>buffer_input</c> (plain, parallel-Sink) and
/// <c>ordered_buffer_input</c> (<see cref="SinkOrderDependent"/>) — every input row survives
/// unchanged, buffered through <see cref="IFunctionStorage"/> so a real cross-process worker
/// rotation (see <c>table_buffering_pool_rotation.test</c>) still reads back everything the Sink
/// phase wrote. <see cref="Combine"/> always collapses every Sink call's (identically duplicated)
/// state_id down to the single shared <c>execution_id</c> — every table-buffering fixture that
/// wants "one accumulator/replay for the whole execution" does this (see
/// <see cref="ITableBufferingFunction.Process"/>'s doc comment) — so
/// <c>table_buffering_parallel.test</c>'s "distinct state_id count == 1 regardless of thread count"
/// invariant holds.
/// </summary>
public sealed class BufferInputFunction(string name, string description, bool sinkOrderDependent = false) : ITableBufferingFunction
{
    private const string RawNamespace = "raw";
    private const string RawKey = "data";

    public string Name => name;

    public string Description => description;

    public bool SinkOrderDependent => sinkOrderDependent;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => bindParams.InputSchema;

    public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams)
    {
        processParams.Storage.Append(RawNamespace, RawKey, RecordBatchIpc.Write(batch));
        return processParams.ExecutionId;
    }

    public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams) =>
        [combineParams.ExecutionId];

    public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
        new Producer(finalizeParams);

    private sealed class Producer(TableBufferingFinalizeParams finalizeParams) : ITableFunctionProducer
    {
        private IEnumerator<byte[]>? _pending;

        public void Produce(OutputCollector output)
        {
            _pending ??= finalizeParams.Storage.ScanLog(RawNamespace, RawKey).GetEnumerator();
            if (_pending.MoveNext())
            {
                output.Emit(RecordBatchIpc.Read(_pending.Current));
                return;
            }

            output.Finish();
        }
    }
}
