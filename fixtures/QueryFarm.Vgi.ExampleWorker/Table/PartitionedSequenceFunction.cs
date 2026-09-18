using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>partitioned_sequence(count [, increment])</c> — a <see cref="ITableFunction.MaxWorkers"/>-
/// capable generator: every parallel <c>init</c> call for the SAME logical scan (identified by
/// <c>InitRequest.ExecutionId</c> — the primary connection mints one, DuckDB threads it through to
/// every secondary connection it opens up to <see cref="MaxWorkers"/>) claims successive fixed-size
/// chunks from a SHARED work queue keyed by that execution id, so the union of every connection's
/// output is exactly <c>0..count-1</c> with no gaps or duplicates regardless of how many readers
/// DuckDB decides to open. Coordinates via <see cref="CrossProcessWorkQueue"/> — see its doc
/// comment for why an in-process-only counter doesn't work here (subprocess transport spawns a
/// SEPARATE OS process per parallel reader). Backs <c>partitioned_sequence.test</c>.
/// </summary>
public sealed class PartitionedSequenceFunction : ITableFunction
{
    private const long ChunkSize = 10000;

    public string Name => "partitioned_sequence";

    public string Description => "Generates a partitioned sequence for multi-worker execution";

    private const int MaxReaders = 8;

    public int? MaxWorkers => MaxReaders;

    /// <summary>No more readers than chunks: a 10,000-row call is one chunk, and used to get eight
    /// readers, seven of which found the queue empty. Same rule as the reference Python fixture,
    /// which declares <c>max_workers</c> equal to its work items.</summary>
    public int? MaxWorkersForCall(TableInitParams initParams) =>
        ChunkCount(initParams.Arguments.Int64(0), ChunkSize, MaxReaders);

    /// <summary><c>ceil(count / chunkSize)</c>, clamped to <c>[1, cap]</c>.</summary>
    internal static int ChunkCount(long count, long chunkSize, int cap) =>
        (int)Math.Clamp((Math.Max(0, count) + chunkSize - 1) / chunkSize, 1, cap);

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("count", Int64Type.Default),
            TableArgFields.Named("increment", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var increment = initParams.Arguments.Int64Named("increment", 1);
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, count, increment, initParams.OutputSchema);
    }

    private sealed class Producer(string key, long count, long increment, Schema outputSchema) : ITableFunctionProducer
    {
        public void Produce(OutputCollector output)
        {
            var rows = CrossProcessWorkQueue.ClaimChunk(key, ChunkSize, count, out var start);
            if (rows == 0)
            {
                output.Finish();
                return;
            }

            var builder = new Int64Array.Builder();
            for (var i = 0; i < rows; i++)
            {
                builder.Append((start + i) * increment);
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], (int)rows));
        }
    }
}
