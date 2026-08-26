using Apache.Arrow;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary><c>cached_sum_all(data TABLE)</c> — a whole-input (table-buffering/reduce) sum over the
/// TABLE argument's single numeric column, cache-metadata-tagged at Source/finalize emit. Same
/// Sink→Combine→Source shape as <c>ExampleWorker.Buffering.SumAllColumnsFunction</c>, simplified to
/// exactly one output column (the query aliases it <c>AS total</c>). Backs
/// <c>exchange_buffered.test</c>'s binary (whole-result) hit/miss caching proof.</summary>
public sealed class CachedSumAllFunction : ITableBufferingFunction
{
    private const string RawNamespace = "cached_sum_all";
    private const string RawKey = "data";

    public string Name => "cached_sum_all";

    public string SchemaName => "main";

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
        new SumProducer(finalizeParams);

    private sealed class SumProducer(TableBufferingFinalizeParams finalizeParams) : ITableFunctionProducer
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
            var schema = finalizeParams.OutputSchema;
            long sum = 0;
            foreach (var raw in finalizeParams.Storage.ScanLog(RawNamespace, RawKey))
            {
                var batch = RecordBatchIpc.Read(raw);
                var column = batch.Column(0);
                for (var r = 0; r < batch.Length; r++)
                {
                    var v = NumericArrayMath.ReadAsDouble(column, r);
                    if (v is not null)
                    {
                        sum += (long)v.Value;
                    }
                }
            }

            var builder = new Int64Array.Builder();
            builder.Append(sum);
            output.Emit(new RecordBatch(schema, [builder.Build()], 1), CacheMetadata.Ttl(300));
            output.Finish();
        }
    }
}
