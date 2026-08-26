using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// Backs <c>same_name_schemas.test</c>'s table-buffering half: <c>test_same_name_buffered(data
/// TABLE)</c> registered once per schema, each tagging every stored row with its own schema name at
/// the SINK phase (<see cref="Process"/>) — proving the Sink-side worker resolved the right
/// implementation, independent of the table-in-out dispatch path (buffering acquires its runtime
/// connections through the buffering operator's own params, not the table-in-out bind site).
/// </summary>
public sealed class SameNameBufferedFunction(string schemaName, string description) : ITableBufferingFunction
{
    private const string RawNamespace = "raw";
    private const string RawKey = "data";

    public string Name => "test_same_name_buffered";

    public string SchemaName => schemaName;

    public string Description => description;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("tag", StringType.Default, nullable: true)], metadata: null);

    public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams)
    {
        // Tag at the SINK phase (not Source/finalize) — the dispatch coverage this fixture proves
        // is that Process() itself resolved the right (schema-specific) implementation.
        var column = batch.Column(0);
        var builder = new StringArray.Builder();
        for (var i = 0; i < batch.Length; i++)
        {
            var v = NumericArrayMath.ReadAsDouble(column, i);
            if (v is null)
            {
                builder.AppendNull();
                continue;
            }

            builder.Append($"{schemaName}:{(long)v.Value}");
        }

        var tagged = new RecordBatch(OutputSchema, [builder.Build()], batch.Length);
        processParams.Storage.Append(RawNamespace, RawKey, RecordBatchIpc.Write(tagged));
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
            _pending ??= new Queue<RecordBatch>(finalizeParams.Storage.ScanLog(RawNamespace, RawKey).Select(RecordBatchIpc.Read));

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
