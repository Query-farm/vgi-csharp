using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.DocsExamples;

public sealed class CollectFunction : ITableBufferingFunction
{
    private const string StorageNamespace = "collect";
    private const string StorageKey = "partial-sums";

    public string Name => "collect_sum";

    public string Description => "Buffer all input before returning its sum";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new(
        [new Field("sum", Int64Type.Default, nullable: false)],
        metadata: null);

    public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams)
    {
        var values = (Int64Array)batch.Column(0);
        long partialSum = 0;
        for (var row = 0; row < values.Length; row++)
        {
            if (!values.IsNull(row))
            {
                partialSum += values.GetValue(row)!.Value;
            }
        }

        processParams.Storage.Append(StorageNamespace, StorageKey, BitConverter.GetBytes(partialSum));
        return processParams.ExecutionId;
    }

    public IReadOnlyList<byte[]> Combine(
        IReadOnlyList<byte[]> stateIds,
        TableBufferingCombineParams combineParams) => [combineParams.ExecutionId];

    public ITableFunctionProducer CreateFinalizeProducer(
        byte[] finalizeStateId,
        TableBufferingFinalizeParams finalizeParams) => new Producer(finalizeParams);

    private sealed class Producer(TableBufferingFinalizeParams finalizeParams) : ITableFunctionProducer
    {
        private bool _finished;

        public void Produce(OutputCollector output)
        {
            if (_finished)
            {
                output.Finish();
                return;
            }

            var total = finalizeParams.Storage
                .ScanLog(StorageNamespace, StorageKey)
                .Sum(state => BitConverter.ToInt64(state));
            var values = new Int64Array.Builder().Append(total).Build();
            output.Emit(new RecordBatch(finalizeParams.OutputSchema, [values], 1));
            output.Finish();
            _finished = true;
        }
    }
}
