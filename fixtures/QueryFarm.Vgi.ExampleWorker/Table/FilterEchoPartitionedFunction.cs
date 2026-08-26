using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>filter_echo_partitioned(count)</c> — combines <see cref="PartitionedSequenceFunction"/>'s
/// shared (cross-process — see <see cref="CrossProcessWorkQueue"/>) work-queue partitioning with
/// <see cref="FilterEchoFunction"/>'s real filter-pushdown application: every parallel worker
/// claims successive 1000-row chunks and independently applies the SAME pushed-down filter, so the
/// union across however many workers DuckDB opens is still exactly the filtered row set with no
/// gaps or duplicates. Backs <c>filter_echo_partitioned.test</c>.
/// </summary>
public sealed class FilterEchoPartitionedFunction : ITableFunction
{
    private const long ChunkSize = 1000;

    public string Name => "filter_echo_partitioned";

    public string Description => "Multi-worker partitioned sequence that echoes pushed-down filters";

    public int? MaxWorkers => 8;

    public bool? FilterPushdown => true;

    public bool? ProjectionPushdown => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("pushed_filters", StringType.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        var filterText = PushdownFilterFormatter.Format(decoded);
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, count, decoded, filterText, initParams.ProjectedSchema, initParams.ProjectionIds);
    }

    private sealed class Producer(
        string key, long count, DecodedFilters? decoded, string filterText, Schema projectedSchema, IReadOnlyList<long>? projectionIds)
        : ITableFunctionProducer
    {
        public void Produce(OutputCollector output)
        {
            var ns = new List<long>();
            var row = new Dictionary<string, object?>();

            long claimedRows;
            while (ns.Count == 0 && (claimedRows = CrossProcessWorkQueue.ClaimChunk(key, ChunkSize, count, out var start)) > 0)
            {
                for (var i = 0; i < claimedRows; i++)
                {
                    var n = start + i;
                    row["n"] = n;
                    if (PushdownFilterEvaluator.Matches(decoded, row))
                    {
                        ns.Add(n);
                    }
                }
            }

            if (ns.Count == 0)
            {
                output.Finish();
                return;
            }

            var rows = ns.Count;
            var indices = projectionIds ?? [0, 1];

            IArrowArray BuildColumn(long fullIndex)
            {
                if (fullIndex == 0)
                {
                    var nBuilder = new Int64Array.Builder();
                    foreach (var n in ns)
                    {
                        nBuilder.Append(n);
                    }

                    return nBuilder.Build();
                }

                var pBuilder = new StringArray.Builder();
                for (var i = 0; i < rows; i++)
                {
                    pBuilder.Append(filterText);
                }

                return pBuilder.Build();
            }

            var columns = indices.Select(BuildColumn).ToList();
            output.Emit(new RecordBatch(projectedSchema, columns, rows));
        }
    }
}
