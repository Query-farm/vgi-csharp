using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// The M3 anchor fixture (mirrors M1's <c>upper_case</c> role): the simplest possible producer —
/// emits integers <c>0, increment, 2*increment, ..., (count-1)*increment</c> in caller-controlled
/// <c>batch_size</c> chunks. Backs <c>test/sql/integration/table/sequence.test</c> and (via its
/// argument-constraint validation) <c>arg_validation.test</c>.
///
/// Advertises <see cref="ITableFunction.FilterPushdown"/> (required so DuckDB's Top-N optimizer
/// attaches a Dynamic Filter to this scan at all — see <c>table/dynamic_filter.test</c>'s EXPLAIN
/// assertion, which reads <c>PhysicalTableScan::dynamic_filters</c>, populated only when
/// <c>get.function.filter_pushdown</c> is true, per
/// <c>~/Development/vgi/duckdb/src/optimizer/join_filter_pushdown_optimizer.cpp</c>'s
/// <c>GetPushdownFilterTargets</c> LOGICAL_GET case) — so, per the same contract
/// <see cref="NestedSequenceFunction"/>'s doc comment documents (DuckDB never installs a residual
/// post-scan filter for a pushdown-capable function), this producer MUST actually apply whatever
/// STATIC filters/join-keys reach <c>init</c>. It deliberately does NOT read the per-tick DYNAMIC
/// filter metadata (<c>output.InputMetadata["vgi_pushdown_filters"]</c>) at all: Top-N's own
/// heap-based algorithm re-derives the true top-N from every row the scan emits regardless of
/// whether the scan pre-filtered on the boundary, so ignoring the (purely-optional) dynamic
/// component can only under-optimize, never mis-answer — see <c>dynamic_filter_echo</c>/
/// <see cref="DynamicFilterEchoFunction"/> for a fixture that DOES surface it.
/// </summary>
public sealed class SequenceFunction : ITableFunction
{
    public string Name => "sequence";

    public string Description => "Generates a sequence of integers from 0 to n-1";

    public IReadOnlyList<string> Categories => ["generator", "utility"];

    public bool? FilterPushdown => true;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("count", Int64Type.Default),
            TableArgFields.Named("batch_size", Int64Type.Default),
            TableArgFields.Named("increment", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public void Bind(TableBindParams bindParams) => Validate(bindParams.Arguments);

    /// <summary>Backs <c>table/table_function_statistics.test</c>: <c>n</c> ranges over
    /// <c>[0, (count-1)*increment]</c>, letting the optimizer fold an out-of-range filter to
    /// <c>EMPTY_RESULT</c> at plan time.</summary>
    public IReadOnlyDictionary<string, Catalog.ColumnStatisticsInput>? Statistics(TableBindParams bindParams)
    {
        var count = bindParams.Arguments.Int64(0);
        if (count <= 0)
        {
            return null;
        }

        var increment = bindParams.Arguments.Int64Named("increment", 1);
        return new Dictionary<string, Catalog.ColumnStatisticsInput>
        {
            ["n"] = new()
            {
                Min = 0L,
                Max = (count - 1) * increment,
                HasNull = false,
                HasNotNull = true,
                DistinctCount = count,
            },
        };
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        Validate(initParams.Arguments);
        var count = initParams.Arguments.Int64(0);
        var batchSize = initParams.Arguments.Int64Named("batch_size", 1000);
        var increment = initParams.Arguments.Int64Named("increment", 1);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        return new SequenceProducer(count, Math.Max(1, batchSize), increment, decoded, initParams.OutputSchema);
    }

    /// <summary>Enforces <c>count</c>/<c>batch_size</c>/<c>increment</c> constraints DuckDB itself
    /// doesn't validate for a table function's SQL arguments — a zero/negative <c>batch_size</c>
    /// would otherwise hang the producer (a batch of 0 rows never advances <c>_next</c>), and a
    /// zero/negative <c>increment</c> is nonsensical for a monotonic sequence. Mirrors the
    /// reference workers' <c>ArgumentValidationError</c> contract (see
    /// <c>test/sql/integration/table/arg_validation.test</c>).</summary>
    private static void Validate(TableArguments args)
    {
        var countArray = args.PositionalArray(0);
        if (countArray is null || countArray.IsNull(0))
        {
            throw new InvalidOperationException("Argument 'count' cannot be NULL");
        }

        RequirePositiveNamed(args, "batch_size");
        RequirePositiveNamed(args, "increment");
    }

    private static void RequirePositiveNamed(TableArguments args, string name)
    {
        var array = args.NamedArray(name);
        if (array is null)
        {
            return;
        }

        if (array.IsNull(0))
        {
            throw new InvalidOperationException($"Argument '{name}' cannot be NULL");
        }

        if (Convert.ToInt64(args.Named(name)) < 1)
        {
            throw new InvalidOperationException($"Argument '{name}' must be >= 1");
        }
    }

    private sealed class SequenceProducer(long count, long batchSize, long increment, DecodedFilters? decoded, Schema outputSchema)
        : ITableFunctionProducer
    {
        private long _next;
        private readonly Dictionary<string, object?> _row = new(1);

        public void Produce(OutputCollector output)
        {
            var ns = new List<long>();

            // Loop across candidate chunks (rather than emitting one possibly-empty batch per
            // tick) so a highly-selective pushed-down filter doesn't need one round trip per
            // `batchSize` candidates skipped — mirrors FilterEchoFunction's pattern.
            while (ns.Count == 0 && _next < count)
            {
                var candidateRows = (int)Math.Min(batchSize, count - _next);
                var start = _next;
                _next += candidateRows;

                for (var i = 0; i < candidateRows; i++)
                {
                    var n = (start + i) * increment;
                    _row["n"] = n;
                    if (PushdownFilterEvaluator.Matches(decoded, _row))
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

            var builder = new Int64Array.Builder();
            builder.Reserve(ns.Count);
            foreach (var n in ns)
            {
                builder.Append(n);
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], ns.Count));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
