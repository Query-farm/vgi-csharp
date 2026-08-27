using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
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
/// <para><b>Genuine expression-filter pushdown.</b> Declares <see cref="SupportedExpressionFilters"/>
/// for <c>list_contains</c>, <c>starts_with</c>, and <c>contains</c> — matching the test file's
/// non-spatial half exactly, including its "unsupported function ⇒ residual FILTER stays" negative
/// assertion (<c>length(name) &gt; 7</c> is deliberately NOT declared, so DuckDB correctly leaves a
/// residual FILTER for it). Pushed predicates are decoded by <see cref="Internal.PushdownFilterCodec"/>
/// and evaluated by <see cref="Internal.ExpressionFilterEvaluator"/> — an embedded DuckDB connection,
/// not hand-written C# reimplementations of these functions (see that class's doc comment for why).
/// This file is gated behind a file-level <c>require spatial</c> (it shares one file with
/// <see cref="SpatialFilterExampleFunction"/>'s spatial half), so exercising even this non-spatial
/// half locally needs a <c>spatial</c>-capable DuckDB build — see <c>ci/README.md</c>.</para></summary>
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

    public bool? FilterPushdown => true;

    public bool FiltersExactlyApplied => true;

    public IReadOnlyList<string> SupportedExpressionFilters => ["list_contains", "starts_with", "contains"];

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        return new Producer(count, initParams.OutputSchema, decoded);
    }

    private sealed class Producer(long count, Schema outputSchema, DecodedFilters? decoded) : ITableFunctionProducer
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

            var batch = new RecordBatch(outputSchema, [id.Build(), name.Build(), tagsBuilder.Build(), score.Build()], (int)count);
            var mask = ExpressionFilterEvaluator.EvaluateMask(decoded, batch, outputSchema);
            output.Emit(ExpressionFilterEvaluator.ApplyMask(batch, mask));
            output.Finish();
        }
    }
}
