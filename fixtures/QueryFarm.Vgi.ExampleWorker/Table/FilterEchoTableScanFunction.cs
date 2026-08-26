using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// The backing scan for the <c>example.data.filter_echo_table</c> catalog table — a fixed, no-arg
/// twin of <see cref="FilterEchoFunction"/> (100 rows: <c>n</c> in 0..99, <c>s</c> = <c>"row_&lt;n&gt;"</c>)
/// that additionally advertises <see cref="SupportedExpressionFilters"/> so a constant-prefix
/// <c>LIKE</c>/<c>starts_with</c> predicate on <c>s</c> can be exercised. Backs
/// <c>table/filter_pushdown_through_view.test</c> — a table (not a bare function call) is required
/// there specifically to exercise pushdown surviving through a catalog VIEW wrapping it.
/// </summary>
public sealed class FilterEchoTableScanFunction : ITableFunction
{
    private const long RowCount = 100;
    private const long BatchSize = 2048;

    public string Name => "filter_echo_table_scan";

    public string Description => "Backing scan for the data.filter_echo_table catalog table";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("s", StringType.Default, nullable: true),
            new Field("pushed_filters", StringType.Default, nullable: true),
        ],
        metadata: null);

    public bool? FilterPushdown => true;

    public bool? ProjectionPushdown => true;

    public IReadOnlyList<string> SupportedExpressionFilters => ["prefix", "starts_with"];

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        var filterText = PushdownFilterFormatter.Format(decoded);
        return new Producer(filterText, decoded, initParams.ProjectedSchema, initParams.ProjectionIds);
    }

    private sealed class Producer(string filterText, DecodedFilters? decoded, Schema projectedSchema, IReadOnlyList<long>? projectionIds)
        : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            var ns = new List<long>();
            var ss = new List<string>();
            var row = new Dictionary<string, object?>();

            while (ns.Count == 0 && _next < RowCount)
            {
                var candidateRows = (int)Math.Min(BatchSize, RowCount - _next);
                var start = _next;
                _next += candidateRows;

                for (var i = 0; i < candidateRows; i++)
                {
                    var n = start + i;
                    var s = $"row_{n}";
                    row["n"] = n;
                    row["s"] = s;
                    if (PushdownFilterEvaluator.Matches(decoded, row))
                    {
                        ns.Add(n);
                        ss.Add(s);
                    }
                }
            }

            if (ns.Count == 0)
            {
                output.Finish();
                return;
            }

            var rows = ns.Count;

            IArrowArray BuildColumn(int fullIndex)
            {
                switch (fullIndex)
                {
                    case 0:
                        var nBuilder = new Int64Array.Builder();
                        foreach (var n in ns)
                        {
                            nBuilder.Append(n);
                        }

                        return nBuilder.Build();
                    case 1:
                        var sBuilder = new StringArray.Builder();
                        foreach (var s in ss)
                        {
                            sBuilder.Append(s);
                        }

                        return sBuilder.Build();
                    default:
                        var pBuilder = new StringArray.Builder();
                        for (var i = 0; i < rows; i++)
                        {
                            pBuilder.Append(filterText);
                        }

                        return pBuilder.Build();
                }
            }

            var indices = projectionIds ?? [0, 1, 2];
            var columns = indices.Select(id => BuildColumn((int)id)).ToList();
            output.Emit(new RecordBatch(projectedSchema, columns, rows));

            if (_next >= RowCount)
            {
                output.Finish();
            }
        }
    }
}
