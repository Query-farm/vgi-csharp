using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Logging;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// The M4 table-buffering anchor fixture: <c>sum_all_columns(data TABLE, logging BOOL)</c> —
/// column-wise sums across EVERY input batch (from every substream/worker), emitting one summary
/// row at finalize. Mirrors vgi-python/vgi-java's <c>SumAllColumnsFunction</c>. Registered under two
/// names (<c>sum_all_columns</c> and <c>sum_all_columns_simple_distributed</c>) since this
/// implementation is already correct regardless of how many worker processes contributed batches —
/// unlike a per-substream table-in-out FINALIZE, table-buffering's Combine phase always sees every
/// Process call's result before Source ever runs, so there is no separate "distributed" variant to
/// write.
///
/// Sink phase (<see cref="Process"/>) appends each input batch's IPC bytes to a durable log
/// (<see cref="IFunctionStorage"/>, keyed by <c>execution_id</c>); Combine collapses every batch's
/// state_id (always the execution id itself) down to one; the Source phase's producer reads every
/// logged batch back and sums each numeric output column.
/// </summary>
public class SumAllColumnsFunction(string name, bool includeLoggingArg = true, string? description = null) : ITableBufferingFunction
{
    protected const string RawNamespace = "raw";
    protected const string RawKey = "data";

    public string Name => name;

    public string Description => description ?? "Computes column-wise sums across all batches";

    public IReadOnlyList<string> Categories => ["aggregation", "numeric"];

    public Schema ArgumentsSchema { get; } = includeLoggingArg
        ? new([TableArgFields.Table("data"), TableArgFields.Named("logging", BooleanType.Default)], metadata: null)
        : new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams)
    {
        var fields = new List<Field>();
        foreach (var field in bindParams.InputSchema.FieldsList)
        {
            if (field.DataType is IntegerType)
            {
                fields.Add(new Field(field.Name, Int64Type.Default, nullable: true));
            }
            else if (field.DataType is FloatingPointType or Decimal128Type)
            {
                fields.Add(new Field(field.Name, DoubleType.Default, nullable: true));
            }
        }

        if (fields.Count == 0)
        {
            var described = string.Join(", ", bindParams.InputSchema.FieldsList.Select(f => $"{f.Name}: {f.DataType}"));
            throw new InvalidOperationException(
                $"sum_all_columns requires at least one numeric (integer, floating-point, or decimal) input column, got [{described}]");
        }

        return new Schema(fields, metadata: null);
    }

    public virtual byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams)
    {
        if (processParams.Arguments.BoolNamed("logging", false))
        {
            processParams.Ctx?.EmitLog(VgiLogLevel.Info, $"Processing batch with {batch.Length} rows");
        }

        processParams.Storage.Append(RawNamespace, RawKey, RecordBatchIpc.Write(batch));
        return processParams.ExecutionId;
    }

    public virtual IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams)
    {
        if (combineParams.Arguments.BoolNamed("logging", false))
        {
            combineParams.Ctx?.EmitLog(VgiLogLevel.Info, $"Combining {stateIds.Count} state_ids");
        }

        return [combineParams.ExecutionId];
    }

    public virtual ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
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

            var schema = finalizeParams.OutputSchema;
            var sums = new double[schema.FieldsList.Count];
            var seenAny = new bool[schema.FieldsList.Count];

            foreach (var raw in finalizeParams.Storage.ScanLog(RawNamespace, RawKey))
            {
                var batch = RecordBatchIpc.Read(raw);
                for (var c = 0; c < schema.FieldsList.Count; c++)
                {
                    var srcIndex = batch.Schema.GetFieldIndex(schema.GetFieldByIndex(c).Name);
                    if (srcIndex < 0)
                    {
                        continue;
                    }

                    var column = batch.Column(srcIndex);
                    for (var r = 0; r < batch.Length; r++)
                    {
                        // NumericArrayMath.ReadAsDouble doesn't cover Decimal128Array (decimal
                        // columns are summed here, not via that shared int/float helper).
                        double? v = column is Decimal128Array decimalColumn
                            ? decimalColumn.IsNull(r) ? null : (double)decimalColumn.GetValue(r)!.Value
                            : NumericArrayMath.ReadAsDouble(column, r);
                        if (v is null)
                        {
                            continue;
                        }

                        sums[c] += v.Value;
                        seenAny[c] = true;
                    }
                }
            }

            var arrays = new IArrowArray[schema.FieldsList.Count];
            for (var c = 0; c < schema.FieldsList.Count; c++)
            {
                var field = schema.GetFieldByIndex(c);
                arrays[c] = field.DataType is Int64Type
                    ? new Int64Array.Builder().Append((long)sums[c]).Build()
                    : new DoubleArray.Builder().Append(sums[c]).Build();
            }

            output.Emit(new RecordBatch(schema, arrays, 1));
            _emitted = true;
            output.Finish();
        }
    }
}
