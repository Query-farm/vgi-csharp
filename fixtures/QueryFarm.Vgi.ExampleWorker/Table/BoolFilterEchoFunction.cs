using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>bool_filter_echo(count)</c> — <c>count</c> rows with a nullable BOOLEAN <c>flag</c> column
/// cycling TRUE, FALSE, NULL, plus a <c>pushed_filters</c> column echoing the rendering of
/// whatever DuckDB pushed down. Backs <c>filter_pushdown/boolean_column_predicate.test</c>.
///
/// <para>It exists because nothing else can express the shape. <c>WHERE flag</c> and
/// <c>WHERE NOT flag</c> are pushed down as a bare <c>column_ref</c>, not rewritten to
/// <c>flag = true</c>, and to see one at all you need a TABLE FUNCTION with a boolean column:
/// <see cref="FilterEchoFunction"/> has none, so the predicate cannot be written against it, and
/// the table-in-out echo path is never handed a bare boolean column as a pushed predicate, so a
/// case written there passes whether or not the worker understands one.</para>
///
/// <para>Echoing <c>pushed_filters</c> covers the half a row count cannot see: a shape that
/// decodes and evaluates correctly but renders no SQL shows up there as <c>(none)</c>, and for a
/// worker that builds a WHERE clause from it that is silently wrong rows — DuckDB does not
/// re-apply a predicate it pushed into a table function.</para>
/// </summary>
public sealed class BoolFilterEchoFunction : ITableFunction
{
    public string Name => "bool_filter_echo";

    public string Description => "Rows with a nullable BOOLEAN column, echoing pushed-down filters";

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("count", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("flag", BooleanType.Default, nullable: true),
            new Field("pushed_filters", StringType.Default, nullable: true),
        ],
        metadata: null);

    public bool? FilterPushdown => true;

    public bool? ProjectionPushdown => true;

    public long? Cardinality(TableBindParams bindParams) => bindParams.Arguments.Int64(0);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys, initParams.OutputSchema);
        var filterText = PushdownFilterFormatter.Format(decoded);
        return new Producer(count, filterText, decoded, initParams.ProjectedSchema, initParams.ProjectionIds);
    }

    private sealed class Producer(
        long count, string filterText, DecodedFilters? decoded, Schema projectedSchema, IReadOnlyList<long>? projectionIds)
        : ITableFunctionProducer
    {
        private bool _done;

        public void Produce(OutputCollector output)
        {
            if (_done)
            {
                output.Finish();
                return;
            }

            _done = true;

            var ns = new List<long>();
            var flags = new List<bool?>();
            var row = new Dictionary<string, object?>();

            for (var n = 0L; n < count; n++)
            {
                // TRUE, FALSE, NULL. The NULL row is the point: it is what distinguishes
                // `WHERE flag` from `WHERE flag IS NOT FALSE`, and `WHERE NOT flag` from
                // `WHERE flag IS NOT TRUE`.
                bool? flag = (n % 3) switch
                {
                    0 => true,
                    1 => false,
                    _ => null,
                };
                row["n"] = n;
                row["flag"] = flag;
                if (PushdownFilterEvaluator.Matches(decoded, row))
                {
                    ns.Add(n);
                    flags.Add(flag);
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
                        var flagBuilder = new BooleanArray.Builder();
                        foreach (var flag in flags)
                        {
                            if (flag is null)
                            {
                                flagBuilder.AppendNull();
                            }
                            else
                            {
                                flagBuilder.Append(flag.Value);
                            }
                        }

                        return flagBuilder.Build();
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
            output.Finish();
        }
    }
}
