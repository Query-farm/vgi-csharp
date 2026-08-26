using Apache.Arrow;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// Global-function probe (test/sql/integration/global_functions/*.test): a table-buffering
/// (Sink+Source) passthrough — every input row survives unchanged, exercising the fourth function
/// kind global publication supports alongside scalar/table/aggregate.
/// </summary>
public sealed class GlobalBufferedFunction : ITableBufferingFunction
{
    private const string RawNamespace = "raw";
    private const string RawKey = "data";

    public string Name => "global_buffered";

    public string Description => "Global-function probe (table-buffering passthrough)";

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
        new EchoProducer(finalizeParams);

    private sealed class EchoProducer(TableBufferingFinalizeParams finalizeParams) : ITableFunctionProducer
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
