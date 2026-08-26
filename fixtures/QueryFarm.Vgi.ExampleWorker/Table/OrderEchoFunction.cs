using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>order_echo(count)</c> — echoes whatever <c>ORDER BY ... LIMIT</c> pushdown hint DuckDB's
/// Top-N optimizer (<c>row_group_pruner.cpp</c>) sent on <c>init</c>
/// (<see cref="TableInitParams.OrderByColumnName"/>/<c>OrderByDirection</c>/<c>OrderByNullOrder</c>/
/// <c>OrderByLimit</c>), one column each, plus real filter pushdown (needed for
/// <c>order_pushdown.test</c>'s WHERE+ORDER BY+LIMIT interaction case). Backs
/// <c>order_pushdown.test</c>.
/// </summary>
public sealed class OrderEchoFunction : ITableFunction
{
    public string Name => "order_echo";

    public string Description => "Echoes pushed-down ORDER BY and LIMIT hints";

    public bool? FilterPushdown => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("s", StringType.Default, nullable: true),
            new Field("order_column", StringType.Default, nullable: true),
            new Field("order_direction", StringType.Default, nullable: true),
            new Field("order_null_order", StringType.Default, nullable: true),
            new Field("order_limit", Int64Type.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        return new Producer(
            count,
            decoded,
            initParams.OrderByColumnName ?? "(none)",
            FormatDirection(initParams.OrderByDirection),
            FormatNullOrder(initParams.OrderByNullOrder),
            initParams.OrderByLimit ?? -1,
            initParams.OutputSchema);
    }

    private static string FormatDirection(VgiOrderByDirection? direction) => direction switch
    {
        VgiOrderByDirection.Asc => "ASC",
        VgiOrderByDirection.Desc => "DESC",
        _ => "(none)",
    };

    private static string FormatNullOrder(VgiNullOrder? nullOrder) => nullOrder switch
    {
        VgiNullOrder.NullsFirst => "NULLS_FIRST",
        VgiNullOrder.NullsLast => "NULLS_LAST",
        _ => "(none)",
    };

    private sealed class Producer(
        long count, DecodedFilters? decoded, string orderColumn, string orderDirection, string orderNullOrder, long orderLimit, Schema outputSchema)
        : ITableFunctionProducer
    {
        private const int BatchSize = 2048;
        private long _next;

        public void Produce(OutputCollector output)
        {
            var ns = new List<long>();
            var row = new Dictionary<string, object?>();
            while (ns.Count == 0 && _next < count)
            {
                var candidateRows = (int)Math.Min(BatchSize, count - _next);
                var start = _next;
                _next += candidateRows;
                for (var i = 0; i < candidateRows; i++)
                {
                    var n = start + i;
                    row["n"] = n;
                    row["s"] = $"row_{n}";
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
            var nBuilder = new Int64Array.Builder();
            var sBuilder = new StringArray.Builder();
            var colBuilder = new StringArray.Builder();
            var dirBuilder = new StringArray.Builder();
            var nullBuilder = new StringArray.Builder();
            var limitBuilder = new Int64Array.Builder();

            foreach (var n in ns)
            {
                nBuilder.Append(n);
                sBuilder.Append($"row_{n}");
                colBuilder.Append(orderColumn);
                dirBuilder.Append(orderDirection);
                nullBuilder.Append(orderNullOrder);
                limitBuilder.Append(orderLimit);
            }

            output.Emit(new RecordBatch(
                outputSchema,
                [nBuilder.Build(), sBuilder.Build(), colBuilder.Build(), dirBuilder.Build(), nullBuilder.Build(), limitBuilder.Build()],
                rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
