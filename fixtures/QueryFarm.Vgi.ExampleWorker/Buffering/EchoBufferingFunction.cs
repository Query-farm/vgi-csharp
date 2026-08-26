using Apache.Arrow;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// Backs <c>table_buffering_projection_filters.test</c>: <c>echo_buffering(data TABLE)</c> is a
/// <see cref="ITableBufferingFunction"/> with both <see cref="ProjectionPushdown"/> and
/// <see cref="FilterPushdown"/> declared. Unlike <c>TableInOut/EchoFunction.cs</c>'s documented
/// projection-narrowing gap (that fixture can't safely narrow its passthrough data without
/// reshaping <c>Process</c>), table-buffering's Sink/Source split makes this safe to do correctly:
/// <see cref="Process"/> stores WHATEVER batch DuckDB actually hands it (already column_ids-narrowed
/// — includes every projected output column PLUS every filter-referenced column, per the C++
/// operator's own scan-column selection), and the FINALIZE producer re-derives both the row filter
/// (via <see cref="PushdownFilterEvaluator"/>) and the output projection (by column NAME, against
/// <see cref="TableBufferingFinalizeParams.ProjectedSchema"/>) from that same stored batch on
/// replay — so there's no "narrowed declared schema vs. full emitted data" mismatch to create.
/// Because the C++ Sink+Source operator installs no residual post-scan filter once
/// <see cref="FilterPushdown"/> is advertised (fully materializing operator — nothing left to
/// re-check afterward), this producer MUST actually drop non-matching rows itself.
/// </summary>
public sealed class EchoBufferingFunction : ITableBufferingFunction
{
    private const string RawNamespace = "raw";
    private const string RawKey = "data";

    public string Name => "echo_buffering";

    public string Description => "Buffered passthrough with projection + filter pushdown";

    public bool? ProjectionPushdown => true;

    public bool? FilterPushdown => true;

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
        private DecodedFilters? _decoded;
        private bool _decodedLoaded;

        public void Produce(OutputCollector output)
        {
            _pending ??= finalizeParams.Storage.ScanLog(RawNamespace, RawKey).GetEnumerator();
            if (!_decodedLoaded)
            {
                _decoded = PushdownFilterCodec.Decode(finalizeParams.PushdownFilters, finalizeParams.JoinKeys);
                _decodedLoaded = true;
            }

            var projectedSchema = finalizeParams.ProjectedSchema;
            var row = new Dictionary<string, object?>();

            while (_pending.MoveNext())
            {
                var batch = RecordBatchIpc.Read(_pending.Current);
                var matchingRows = new List<int>();
                for (var r = 0; r < batch.Length; r++)
                {
                    row.Clear();
                    for (var c = 0; c < batch.Schema.FieldsList.Count; c++)
                    {
                        row[batch.Schema.GetFieldByIndex(c).Name] = ScalarArgCodec.ReadScalar(batch.Column(c), r);
                    }

                    if (PushdownFilterEvaluator.Matches(_decoded, row))
                    {
                        matchingRows.Add(r);
                    }
                }

                if (matchingRows.Count == 0)
                {
                    // This stored batch had no surviving rows — keep draining subsequent stored
                    // batches within the SAME tick rather than emitting an empty one.
                    continue;
                }

                var columns = new IArrowArray[projectedSchema.FieldsList.Count];
                for (var c = 0; c < projectedSchema.FieldsList.Count; c++)
                {
                    var field = projectedSchema.GetFieldByIndex(c);
                    var srcIndex = batch.Schema.GetFieldIndex(field.Name);
                    var values = new List<object?>(matchingRows.Count);
                    foreach (var r in matchingRows)
                    {
                        values.Add(srcIndex >= 0 ? ScalarArgCodec.ReadScalar(batch.Column(srcIndex), r) : null);
                    }

                    columns[c] = AnyArrayBuilder.Build(field.DataType, values);
                }

                output.Emit(new RecordBatch(projectedSchema, columns, matchingRows.Count));
                return;
            }

            output.Finish();
        }
    }
}
