using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>expression_filter_test(count)</c> — one of <c>table/function_registration.test</c>'s 162-name
/// roster fixtures (its own real behavioral coverage lives in <c>table/expression_filter.test</c>,
/// the non-spatial half). Emits <c>count</c> rows: <c>id</c> = 0..count-1, <c>name</c> =
/// <c>"item_&lt;id&gt;"</c>, <c>tags</c> = <c>[tag_(id%5), tag_((id+1)%5)]</c>, <c>score</c> =
/// <c>id*1.1</c>.
///
/// <para><b>Known limitation — no expression (function-call) filter pushdown.</b>
/// <c>table/expression_filter.test</c> is gated behind a file-level <c>require spatial</c> (it
/// shares one file with <see cref="SpatialFilterExampleFunction"/>'s coverage), which this
/// environment doesn't have installed, so the WHOLE file — including this function's own
/// non-spatial <c>list_contains</c>/<c>starts_with</c>/<c>contains</c> pushdown assertions — is
/// skipped here, never exercised. This worker only implements the existing
/// column-comparison-shaped <see cref="Internal.PushdownFilterCodec"/>/
/// <see cref="Internal.PushdownFilterEvaluator"/> pushdown machinery (see <c>FilterEchoFunction</c>),
/// which has no representation for a function-call predicate like <c>list_contains(tags, 'x')</c>;
/// genuine expression-filter pushdown (recognizing <c>list_contains</c>/<c>starts_with</c>/
/// <c>contains</c> specifically and proving no residual FILTER node remains, per the test's EXPLAIN
/// assertions) would need new wire-protocol/codec support this port doesn't have yet. Results are
/// still fully CORRECT without it (DuckDB applies the predicate locally after receiving every row) —
/// only the "no FILTER node in the plan" EXPLAIN assertions would fail if this file's <c>require
/// spatial</c> gate were ever satisfied. Registered as a no-<see cref="ITableFunction.FilterPushdown"/>
/// generator accordingly.</para></summary>
public sealed class ExpressionFilterTestFunction : ITableFunction
{
    public string Name => "expression_filter_test";

    public string Description => "Rows with list/string columns for expression-filter-pushdown testing";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: false),
            new Field("tags", new ListType(new Field("item", StringType.Default, nullable: true)), nullable: false),
            new Field("score", DoubleType.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        return new Producer(count, initParams.OutputSchema);
    }

    private sealed class Producer(long count, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted || count <= 0)
            {
                output.Finish();
                return;
            }

            _emitted = true;

            var id = new Int64Array.Builder();
            var name = new StringArray.Builder();
            var score = new DoubleArray.Builder();
            var tagsBuilder = new ListArray.Builder(StringType.Default);
            var tagValues = (StringArray.Builder)tagsBuilder.ValueBuilder;

            for (var i = 0L; i < count; i++)
            {
                id.Append(i);
                name.Append($"item_{i}");
                score.Append(i * 1.1);
                tagsBuilder.Append();
                tagValues.Append($"tag_{i % 5}");
                tagValues.Append($"tag_{(i + 1) % 5}");
            }

            output.Emit(new RecordBatch(outputSchema, [id.Build(), name.Build(), tagsBuilder.Build(), score.Build()], (int)count));
            output.Finish();
        }
    }
}
