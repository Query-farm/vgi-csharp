using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>spatial_filter_example(count)</c> — one of <c>table/function_registration.test</c>'s 162-name
/// roster fixtures; its real behavioral coverage lives in <c>table/expression_filter.test</c>'s
/// spatial half, which is gated behind a file-level <c>require spatial</c> this environment doesn't
/// satisfy (see <see cref="ExpressionFilterTestFunction"/>'s doc comment for the same gate on that
/// file's non-spatial half — the whole file, spatial and non-spatial sections alike, is skipped
/// here, unexercised). A 10x10 grid of points in <c>[0,1)x[0,1)</c> for <c>count=100</c>: point
/// <c>i</c> has <c>x=(i%10)/10</c>, <c>y=(i/10)/10</c>, so <c>x,y ∈ {0.0, 0.1, ..., 0.9}</c>.
/// <c>geom</c> reuses <see cref="GeoPointsTable"/>'s Arrow-native-GeoArrow-point struct
/// representation (<c>STRUCT(x DOUBLE, y DOUBLE)</c> with <c>ARROW:extension:name =
/// "geoarrow.point"</c> field metadata, which the C++ side resolves to DuckDB's GEOMETRY type). No
/// <see cref="ITableFunction.FilterPushdown"/> — see <see cref="ExpressionFilterTestFunction"/>'s
/// doc comment for why genuine (spatial or list-function) expression-filter pushdown is out of
/// scope for this port; results are correct either way (DuckDB applies WHERE locally), only the
/// unreachable "no residual FILTER node" EXPLAIN assertions would be affected.
/// </summary>
public sealed class SpatialFilterExampleFunction : ITableFunction
{
    private static readonly StructType PointStruct = new(
    [
        new Field("x", DoubleType.Default, nullable: false),
        new Field("y", DoubleType.Default, nullable: false),
    ]);

    private static readonly Field GeomField = new(
        "geom",
        PointStruct,
        nullable: false,
        metadata: new Dictionary<string, string> { ["ARROW:extension:name"] = "geoarrow.point" });

    public string Name => "spatial_filter_example";

    public string Description => "10x10 grid of points for spatial-filter-pushdown testing";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: false),
            new Field("x", DoubleType.Default, nullable: false),
            new Field("y", DoubleType.Default, nullable: false),
            GeomField,
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

            var n = new Int64Array.Builder();
            var x = new DoubleArray.Builder();
            var y = new DoubleArray.Builder();
            var geomX = new DoubleArray.Builder();
            var geomY = new DoubleArray.Builder();

            for (var i = 0L; i < count; i++)
            {
                var px = (i % 10) / 10.0;
                var py = (i / 10) / 10.0;
                n.Append(i);
                x.Append(px);
                y.Append(py);
                geomX.Append(px);
                geomY.Append(py);
            }

            var geom = new StructArray(PointStruct, (int)count, [geomX.Build(), geomY.Build()], ArrowBuffer.Empty, nullCount: 0);

            output.Emit(new RecordBatch(outputSchema, [n.Build(), x.Build(), y.Build(), geom], (int)count));
            output.Finish();
        }
    }
}
