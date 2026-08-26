using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// Backs <c>table_buffering_source_order.test</c>: <c>ordered_source(data TABLE)</c> ignores its
/// Sink input entirely and declares <see cref="SourceOrderDependent"/>, which forces the C++
/// operator's Source phase into single-threaded FIXED_ORDER draining of <see cref="Combine"/>'s
/// returned <c>finalize_state_id</c>s, in that exact order (see
/// <c>src/include/vgi_table_buffering_impl.hpp:241-246</c>). <see cref="Combine"/> deliberately
/// IGNORES its <c>stateIds</c> argument and always returns 16 fixed ids
/// (<c>0..15</c>, ascending) — each FINALIZE producer decodes its own id back to an integer and
/// emits exactly one row containing it. Without <see cref="SourceOrderDependent"/> the Source
/// phase's parallel drainers would race across those 16 ids and the output order would be
/// nondeterministic; with it, the output MUST come back as strictly ascending <c>0..15</c> under
/// any thread count.
/// </summary>
public sealed class OrderedSourceFunction : ITableBufferingFunction
{
    private const int Count = 16;

    public string Name => "ordered_source";

    public string Description => "Ignores its Sink input; Combine returns 16 fixed finalize_state_ids to exercise FIXED_ORDER Source draining";

    public bool SourceOrderDependent => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("v", Int64Type.Default, nullable: true)], metadata: null);

    public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams) => processParams.ExecutionId;

    public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams) =>
        Enumerable.Range(0, Count).Select(i => BitConverter.GetBytes((long)i)).ToList();

    public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
        new Producer(BitConverter.ToInt64(finalizeStateId, 0), finalizeParams.OutputSchema);

    private sealed class Producer(long value, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted)
            {
                output.Finish();
                return;
            }

            output.Emit(new RecordBatch(outputSchema, [new Int64Array.Builder().Append(value).Build()], 1));
            _emitted = true;
            output.Finish();
        }
    }
}
