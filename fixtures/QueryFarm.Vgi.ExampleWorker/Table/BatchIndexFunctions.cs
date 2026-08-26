using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>partitioned_batch_index(count)</c> — a <see cref="ITableFunction.SupportsBatchIndex"/> twin of
/// <see cref="PartitionedSequenceFunction"/>: divides <c>0..count-1</c> into fixed 1000-row chunks
/// claimed from a <see cref="CrossProcessWorkQueue"/> shared by every parallel reader of one logical
/// scan, tagging each emitted batch's <c>vgi_batch_index</c> metadata with its chunk's 0-based
/// ordinal — <c>partition_id = start / ChunkSize</c>, which <see cref="CrossProcessWorkQueue.ClaimChunk"/>
/// guarantees is always a whole multiple of <see cref="ChunkSize"/> regardless of claim order across
/// readers. Backs <c>table/batch_index.test</c>/<c>batch_index_pushdown.test</c>/
/// <c>batch_index_stress.test_slow</c>.
/// </summary>
public sealed class PartitionedBatchIndexFunction : ITableFunction
{
    private const long ChunkSize = 1000;
    private const long BatchSize = 1000;

    public string Name => "partitioned_batch_index";

    public string Description => "Partitioned sequence tagging each emitted batch with vgi_batch_index";

    public bool SupportsBatchIndex => true;

    public bool? ProjectionPushdown => true;

    public VgiOrderPreservation? OrderPreservation => VgiOrderPreservation.FixedOrder;

    public int? MaxWorkers => 8;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("count", Int64Type.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public long? Cardinality(TableBindParams bindParams) => bindParams.Arguments.Int64(0);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, count, initParams.OutputSchema);
    }

    private sealed class Producer(string key, long count, Schema outputSchema) : ITableFunctionProducer
    {
        private long _partitionId;
        private long _idx;
        private long _end;
        private bool _haveRange;

        public void Produce(OutputCollector output)
        {
            if (!_haveRange || _idx >= _end)
            {
                var rows = CrossProcessWorkQueue.ClaimChunk(key, ChunkSize, count, out var start);
                if (rows == 0)
                {
                    output.Finish();
                    return;
                }

                _partitionId = start / ChunkSize;
                _idx = start;
                _end = start + rows;
                _haveRange = true;
            }

            var batchRows = (int)Math.Min(BatchSize, _end - _idx);
            var builder = new Int64Array.Builder();
            builder.Reserve(batchRows);
            for (var i = 0; i < batchRows; i++)
            {
                builder.Append(_idx + i);
            }

            _idx += batchRows;

            output.Emit(
                new RecordBatch(outputSchema, [builder.Build()], batchRows),
                new Dictionary<string, string> { ["vgi_batch_index"] = _partitionId.ToString() });
        }
    }
}

/// <summary>
/// <c>partitioned_batch_index_marked(count, chunk_size := 1000)</c> — like
/// <see cref="PartitionedBatchIndexFunction"/> but exposes the partition boundary directly as output
/// columns (<c>partition_id</c>, <c>seq</c> — the row's 0-based offset within its own partition)
/// instead of a single opaque value column, so tests can assert on partition/row ordering directly.
/// Emits sub-batches of at most <see cref="BatchSize"/> (256) rows so a large <c>chunk_size</c>
/// still produces MANY Arrow batches sharing one <c>vgi_batch_index</c> value — exercising
/// multi-batch-per-partition reassembly (<c>batch_index_stress.test_slow</c>'s own scenario).
/// <see cref="ProjectionPushdown"/> is deliberately <see langword="false"/>: this fixture does no
/// column pruning of its own, so it must not claim to.
/// </summary>
public sealed class PartitionedBatchIndexMarkedFunction : ITableFunction
{
    private const long DefaultChunkSize = 1000;
    private const long BatchSize = 256;

    public string Name => "partitioned_batch_index_marked";

    public string Description => "Partitioned sequence exposing partition_id/seq directly, tagged with vgi_batch_index";

    public bool SupportsBatchIndex => true;

    public bool? ProjectionPushdown => false;

    public VgiOrderPreservation? OrderPreservation => VgiOrderPreservation.FixedOrder;

    public int? MaxWorkers => 8;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("count", Int64Type.Default),
            TableArgFields.Named("chunk_size", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("partition_id", Int64Type.Default, nullable: true),
            new Field("seq", Int64Type.Default, nullable: true),
        ],
        metadata: null);

    public long? Cardinality(TableBindParams bindParams) => bindParams.Arguments.Int64(0);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var chunkSize = Math.Max(1, initParams.Arguments.Int64Named("chunk_size", DefaultChunkSize));
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, count, chunkSize, initParams.OutputSchema);
    }

    private sealed class Producer(string key, long count, long chunkSize, Schema outputSchema) : ITableFunctionProducer
    {
        private long _partitionId;
        private long _partitionStart;
        private long _idx;
        private long _end;
        private bool _haveRange;

        public void Produce(OutputCollector output)
        {
            if (!_haveRange || _idx >= _end)
            {
                var rows = CrossProcessWorkQueue.ClaimChunk(key, chunkSize, count, out var start);
                if (rows == 0)
                {
                    output.Finish();
                    return;
                }

                _partitionId = start / chunkSize;
                _partitionStart = start;
                _idx = start;
                _end = start + rows;
                _haveRange = true;
            }

            var batchRows = (int)Math.Min(BatchSize, _end - _idx);
            var partitionIdBuilder = new Int64Array.Builder();
            var seqBuilder = new Int64Array.Builder();
            partitionIdBuilder.Reserve(batchRows);
            seqBuilder.Reserve(batchRows);
            for (var i = 0; i < batchRows; i++)
            {
                partitionIdBuilder.Append(_partitionId);
                seqBuilder.Append(_idx + i - _partitionStart);
            }

            _idx += batchRows;

            output.Emit(
                new RecordBatch(outputSchema, [partitionIdBuilder.Build(), seqBuilder.Build()], batchRows),
                new Dictionary<string, string> { ["vgi_batch_index"] = _partitionId.ToString() });
        }
    }
}

/// <summary>
/// Deliberately-broken <c>supports_batch_index=true</c> fixtures backing
/// <c>table/batch_index_contract.test</c> — each violates exactly one of the C++ extension's
/// <c>InstallBatch</c> contract checks so the test can assert on the resulting typed
/// <c>IOException</c> message. See that file's own doc comment for the three violations.
/// </summary>
public static class BrokenBatchIndexFunctions
{
    /// <summary><c>broken_missing_batch_index_tag(count)</c> — emits a data batch via the
    /// no-metadata <see cref="OutputCollector.Emit(RecordBatch)"/> overload despite advertising
    /// <see cref="ITableFunction.SupportsBatchIndex"/>, so the batch carries no <c>vgi_batch_index</c>
    /// tag at all.</summary>
    public sealed class MissingTag : ITableFunction
    {
        public string Name => "broken_missing_batch_index_tag";

        public string Description => "Deliberately violates the batch_index contract: emits with no vgi_batch_index tag";

        public bool SupportsBatchIndex => true;

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

        public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
            new Producer(initParams.Arguments.Int64(0), initParams.OutputSchema);

        private sealed class Producer(long count, Schema outputSchema) : ITableFunctionProducer
        {
            public void Produce(OutputCollector output)
            {
                var builder = new Int64Array.Builder();
                for (var i = 0; i < count; i++)
                {
                    builder.Append(i);
                }

                output.Emit(new RecordBatch(outputSchema, [builder.Build()], (int)count));
                output.Finish();
            }
        }
    }

    /// <summary><c>broken_non_monotone_batch_index(count)</c> — emits one batch tagged
    /// <c>vgi_batch_index=10</c>, then a second (on the SAME stream) tagged <c>vgi_batch_index=3</c>
    /// — a decrease, violating per-stream monotonicity.</summary>
    public sealed class NonMonotone : ITableFunction
    {
        public string Name => "broken_non_monotone_batch_index";

        public string Description => "Deliberately violates the batch_index contract: emits a decreasing vgi_batch_index on one stream";

        public bool SupportsBatchIndex => true;

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

        public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
            new Producer(initParams.Arguments.Int64(0), initParams.OutputSchema);

        private sealed class Producer(long count, Schema outputSchema) : ITableFunctionProducer
        {
            private int _call;

            public void Produce(OutputCollector output)
            {
                _call++;
                if (_call == 1)
                {
                    var builder = new Int64Array.Builder();
                    for (var i = 0; i < count; i++)
                    {
                        builder.Append(i);
                    }

                    output.Emit(
                        new RecordBatch(outputSchema, [builder.Build()], (int)count),
                        new Dictionary<string, string> { ["vgi_batch_index"] = "10" });
                    return;
                }

                var second = new Int64Array.Builder();
                second.Append(42);
                output.Emit(
                    new RecordBatch(outputSchema, [second.Build()], 1),
                    new Dictionary<string, string> { ["vgi_batch_index"] = "3" });
                output.Finish();
            }
        }
    }

    /// <summary><c>broken_batch_index_overflow(count)</c> — emits a batch tagged
    /// <c>vgi_batch_index = 2^60</c>, well above DuckDB's <c>10^13</c> per-pipeline cap.</summary>
    public sealed class Overflow : ITableFunction
    {
        public string Name => "broken_batch_index_overflow";

        public string Description => "Deliberately violates the batch_index contract: emits a vgi_batch_index above DuckDB's per-pipeline cap";

        public bool SupportsBatchIndex => true;

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

        public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
            new Producer(initParams.Arguments.Int64(0), initParams.OutputSchema);

        private sealed class Producer(long count, Schema outputSchema) : ITableFunctionProducer
        {
            public void Produce(OutputCollector output)
            {
                var builder = new Int64Array.Builder();
                for (var i = 0; i < count; i++)
                {
                    builder.Append(i);
                }

                output.Emit(
                    new RecordBatch(outputSchema, [builder.Build()], (int)count),
                    new Dictionary<string, string> { ["vgi_batch_index"] = (1L << 60).ToString() });
                output.Finish();
            }
        }
    }
}
