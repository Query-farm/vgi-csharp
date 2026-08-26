using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Splits;

/// <summary>
/// <c>split_batch_index(n, splits)</c> — like <see cref="SplitRangeFunction"/>'s plain
/// <c>split_sequence</c> shape, but also advertises <see cref="ITableFunction.SupportsBatchIndex"/>
/// and tags every emitted batch with <c>vgi_batch_index</c> metadata.
///
/// A batch index must be monotonic per READER, and a reader's own claimed split ordinals are
/// themselves strictly ascending (the client's <c>next_split.fetch_add(1)</c> claim loop
/// guarantees that — see <c>batch_index.test</c>'s own doc comment). So <c>ordinal * Stride +
/// localBatchOrdinal</c> is monotone per reader by construction: <see cref="Stride"/> just has to
/// exceed the largest possible batch count any one split ever produces, which 1,000,000 comfortably
/// does relative to this fixture's row batching (500 rows/batch) and DuckDB's own
/// 10^13 per-pipeline batch-index cap.
/// </summary>
public sealed class SplitBatchIndexFunction : ITableFunction
{
    private const long Stride = 1_000_000;

    public string Name => "split_batch_index";

    public string Description => "Split scan whose batch_index stays monotonic per reader across split boundaries";

    public bool SupportsSplits => true;

    public bool SupportsBatchIndex => true;

    public int? MaxWorkers => 8;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Named("n", Int64Type.Default), TableArgFields.Named("splits", Int64Type.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public PlanResult Plan(TableBindParams bindParams, PlanRequest request)
    {
        var n = bindParams.Arguments.Int64Named("n", 0);
        var splits = bindParams.Arguments.Int64Named("splits", 1);
        var ranges = SplitRanges.Even(n, splits);
        var scanSplits = ranges.Select((r, i) => ScanSplit.Of(SplitPayloadCodec.Encode(i, r.Start, r.End))).ToList();
        return PlanResult.Of(scanSplits);
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var payloads = SplitOnlyGuard.RequireSingle(initParams, Name);
        var (ordinal, start, end) = SplitPayloadCodec.Decode(payloads[0]);
        return new Producer(ordinal, start, end, initParams.OutputSchema);
    }

    private sealed class Producer(long ordinal, long start, long end, Schema outputSchema) : ITableFunctionProducer
    {
        private const int BatchSize = 500;
        private long _next = start;
        private long _localBatch;

        public void Produce(OutputCollector output)
        {
            if (_next >= end)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, end - _next);
            var builder = new Int64Array.Builder();
            builder.Reserve(rows);
            for (var i = 0; i < rows; i++)
            {
                builder.Append(_next + i);
            }

            _next += rows;
            var batchIndex = (ordinal * Stride) + _localBatch;
            _localBatch++;
            output.Emit(
                new RecordBatch(outputSchema, [builder.Build()], rows),
                new Dictionary<string, string> { ["vgi_batch_index"] = batchIndex.ToString() });

            if (_next >= end)
            {
                output.Finish();
            }
        }
    }
}
