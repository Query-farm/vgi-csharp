using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>spatial_filter_example(count)</c> — one of <c>table/function_registration.test</c>'s 162-name
/// roster fixtures; its real behavioral coverage lives in <c>table/expression_filter.test</c>'s
/// spatial half, which is gated behind a file-level <c>require spatial</c> this local environment
/// doesn't satisfy (no spatial extension built here — see
/// <see cref="ExpressionFilterTestFunction"/>'s doc comment for the same gate on that file's
/// non-spatial half). A 10x10 grid of points in <c>[0,1)x[0,1)</c> for <c>count=100</c>: point
/// <c>i</c> has <c>x=(i%10)/10</c>, <c>y=(i/10)/10</c>, so <c>x,y ∈ {0.0, 0.1, ..., 0.9}</c>.
///
/// <para><b>Encoding, corrected after a real CI crash</b> (haybarn CI, which DOES have the spatial
/// extension built, unlike this local environment): the first version used the native GeoArrow
/// <c>geoarrow.point</c> extension type — an Arrow <c>STRUCT(x DOUBLE, y DOUBLE)</c> field, which
/// <c>~/Development/vgi/src/vgi_extension.cpp</c> registers a
/// <c>CastFunctionSet::GetCastFunction(STRUCT, GEOMETRY)</c>-based conversion path for. That path
/// crashed DuckDB itself (`INTERNAL Error: Attempted to dereference unique_ptr that is NULL!`,
/// inside <c>vgi.duckdb_extension</c>, on the simplest possible
/// <c>SELECT COUNT(*) FROM spatial_filter_example(100)</c> — no filter involved) — i.e. the cast
/// this path depends on doesn't resolve for a worker-declared <c>geoarrow.point</c> struct field on
/// the published extension build. Checking <c>~/Development/vgi-python</c>'s equivalent fixture
/// (<c>vgi/_test_fixtures/table/filters.py</c>'s <c>SpatialFilterExampleFunction</c>) confirmed the
/// canonical reference doesn't use this path at all: it encodes <c>geom</c> as
/// <c>geoarrow.wkb</c> — a plain Arrow <c>binary</c> column of raw WKB (Well-Known Binary) point
/// bytes, the well-established GeoArrow encoding every other port already uses successfully. This
/// fixture now matches that exactly (same WKB byte layout: 1-byte little-endian marker, 4-byte
/// LE geometry-type=1 (Point), 8-byte LE x, 8-byte LE y) rather than debugging the untested native-
/// struct cast path further — no other language port exercises it, so there's no working reference
/// to diff against, and the WKB path is proven.</para>
///
/// <para><b>Genuine expression-filter pushdown.</b> Declares <see cref="SupportedExpressionFilters"/>
/// for <c>&amp;&amp;</c> (the spatial-extension bbox-intersection operator) and
/// <c>st_intersects_extent</c>, matching vgi-python's reference fixture. The DuckDB optimizer then
/// pushes a bound <c>geom &amp;&amp; ST_MakeEnvelope(...)</c>/<c>st_intersects_extent(geom, ...)</c>
/// predicate down as an <c>"expression"</c> pushdown-filter node (see
/// <see cref="Internal.ExpressionFilterEvaluator"/>'s doc comment for how it's evaluated — an
/// embedded DuckDB connection with the <c>spatial</c> extension loaded, not hand-written C#
/// geometry math), and — because pushdown is genuinely applied — DuckDB leaves no residual
/// <c>FILTER</c> node in the physical plan (<c>table/expression_filter.test</c>'s EXPLAIN
/// assertions).</para>
/// </summary>
public sealed class SpatialFilterExampleFunction : ITableFunction
{
    private static readonly Field GeomField = new(
        "geom",
        BinaryType.Default,
        nullable: false,
        metadata: new Dictionary<string, string>
        {
            ["ARROW:extension:name"] = "geoarrow.wkb",
            ["ARROW:extension:metadata"] = "{}",
        });

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

    public bool? FilterPushdown => true;

    public bool FiltersExactlyApplied => true;

    public IReadOnlyList<string> SupportedExpressionFilters => ["&&", "st_intersects_extent"];

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        return new Producer(count, initParams.OutputSchema, decoded);
    }

    /// <summary>Little-endian WKB point: byte_order(1)=1, geometry_type(u32)=1 (Point), x(f64), y(f64)
    /// — matches vgi-python's <c>_make_wkb_point</c> exactly (<c>struct.pack("&lt;bI", 1, 1) +
    /// struct.pack("&lt;dd", x, y)</c>).</summary>
    private static byte[] MakeWkbPoint(double x, double y)
    {
        var bytes = new byte[21];
        bytes[0] = 1; // byte order: little-endian
        BitConverter.TryWriteBytes(bytes.AsSpan(1, 4), 1u); // geometry type: Point
        BitConverter.TryWriteBytes(bytes.AsSpan(5, 8), x);
        BitConverter.TryWriteBytes(bytes.AsSpan(13, 8), y);
        return bytes;
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

            var n = new Int64Array.Builder();
            var x = new DoubleArray.Builder();
            var y = new DoubleArray.Builder();
            var geom = new BinaryArray.Builder();

            for (var i = 0L; i < count; i++)
            {
                var px = (i % 10) / 10.0;
                var py = (i / 10) / 10.0;
                n.Append(i);
                x.Append(px);
                y.Append(py);
                geom.Append(MakeWkbPoint(px, py));
            }

            var batch = new RecordBatch(outputSchema, [n.Build(), x.Build(), y.Build(), geom.Build()], (int)count);
            var mask = ExpressionFilterEvaluator.EvaluateMask(decoded, batch, outputSchema);
            output.Emit(ExpressionFilterEvaluator.ApplyMask(batch, mask));
            output.Finish();
        }
    }
}
