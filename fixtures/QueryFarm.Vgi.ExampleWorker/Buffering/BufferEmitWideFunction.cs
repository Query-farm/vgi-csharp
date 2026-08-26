using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// Backs <c>table_buffering_large_batch.test</c>: <c>buffer_emit_wide(rows BIGINT, data TABLE)</c>
/// ignores its table input entirely and emits exactly ONE finalize batch of <c>rows</c> sequential
/// integers (column <c>n</c>, values <c>0..rows-1</c>) in a single <see cref="OutputCollector.Emit"/>
/// call — regression coverage for a Source-path truncation bug where the C++ operator's
/// <c>GetDataInternal</c> capped cardinality at <c>STANDARD_VECTOR_SIZE</c> (2048) instead of
/// draining a large single batch across multiple <c>GetData</c> ticks the way the streaming
/// table-in-out path does via a persisted chunk offset (see that test's header comment for the
/// full regression description; already fixed on the C++ side — this fixture just needs to
/// legitimately try to emit more than 2048 rows in one call to exercise it).
/// </summary>
public sealed class BufferEmitWideFunction : ITableBufferingFunction
{
    public string Name => "buffer_emit_wide";

    public string Description => "Emits a single finalize batch of `rows` sequential integers (Source large-batch regression)";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("rows", Int64Type.Default), TableArgFields.Table("data")],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams) => processParams.ExecutionId;

    public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams) =>
        [combineParams.ExecutionId];

    public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
        new Producer(finalizeParams);

    private sealed class Producer(TableBufferingFinalizeParams finalizeParams) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted)
            {
                output.Finish();
                return;
            }

            var rows = finalizeParams.Arguments.Int64(0);
            var builder = new Int64Array.Builder();
            builder.Reserve((int)rows);
            for (long i = 0; i < rows; i++)
            {
                builder.Append(i);
            }

            output.Emit(new RecordBatch(finalizeParams.OutputSchema, [builder.Build()], (int)rows));
            _emitted = true;
            output.Finish();
        }
    }
}
