using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// Backs <c>multi_batch_finalize.test</c>: a streaming table-in-out FINALIZE that emits MANY
/// batches — one per input row the substream saw (the first batch carries the substream's running
/// total, every subsequent one carries 0), so a broken multi-batch-flush continuation shows up as a
/// wrong <c>COUNT(*)</c> even when <c>SUM(n)</c> still comes out right (see the <c>.test</c> file's
/// own doc comment for why the fixture is shaped this way).
/// </summary>
public sealed class MultiBatchFinishFunction : ITableInOutFunction
{
    public string Name => "multi_batch_finish";

    public string Description => "Emits one finalize batch per input row seen, to exercise multi-batch flush";

    public IReadOnlyList<string> Categories => ["aggregation"];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public bool HasFinalize => true;

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor(initParams.OutputSchema);

    private sealed class Processor(Schema outputSchema) : ITableInOutProcessor
    {
        private long _rowCount;
        private long _sum;
        private long _emitted;

        public void Process(RecordBatch input, OutputCollector output)
        {
            var column = (Int64Array)input.Column(0);
            for (var i = 0; i < input.Length; i++)
            {
                if (column.IsNull(i))
                {
                    continue;
                }

                _sum += column.GetValue(i)!.Value;
                _rowCount++;
            }

            // No output during INPUT — every emitted row happens during FINALIZE.
            output.Emit(new RecordBatch(outputSchema, [new Int64Array.Builder().Build()], 0));
        }

        public void Finalize(OutputCollector output)
        {
            if (_emitted >= _rowCount)
            {
                output.Finish();
                return;
            }

            var value = _emitted == 0 ? _sum : 0L;
            var builder = new Int64Array.Builder();
            builder.Append(value);
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], 1));
            _emitted++;
            if (_emitted >= _rowCount)
            {
                output.Finish();
            }
        }
    }
}
