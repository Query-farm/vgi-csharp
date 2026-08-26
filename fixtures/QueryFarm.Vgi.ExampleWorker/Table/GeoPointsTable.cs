using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>example.data.geo_points</c> — backs <c>table/column_statistics.test</c>'s "Geometry column
/// statistics" section: a GEOMETRY-typed column whose declared statistics report a bounding-box
/// extent (<c>vgi_table_statistics()</c> renders it as <c>BOX(0 0, 4 4)</c>). Excluded from
/// <c>table/comments.test</c>'s table listing ("geo_points requires spatial extension" — that
/// comment is about the reference workers' use of <c>ST_Point</c> to CONSTRUCT the geometry
/// values, not about the GEOMETRY logical type itself, which is a DuckDB CORE type; no spatial
/// extension needs to be loaded here).
///
/// <para>The <c>geom</c> column is declared as an Arrow <c>STRUCT(x DOUBLE, y DOUBLE)</c> field
/// carrying <c>ARROW:extension:name = "geoarrow.point"</c> metadata — the native GeoArrow point
/// extension type <c>vgi_extension.cpp</c> registers at load time
/// (<c>config.RegisterArrowExtension(...)</c>), which the C++ side resolves to DuckDB's built-in
/// <see cref="LogicalTypeId.GEOMETRY"/> (mirrors how <c>ConstantColumnsFunction</c> preserves an
/// incoming struct field's own <c>ARROW:extension:name</c> annotation).</para>
///
/// <para>GEOMETRY's internal/physical representation in DuckDB core IS raw WKB bytes
/// (<c>duckdb/common/types/geometry.cpp</c>: "We are currently using WKB internally, so just copy
/// as-is!"). <see cref="Internal.ColumnStatisticsCodec"/>'s value union has no STRUCT member, so
/// the <c>geom</c> column's declared Min/Max statistics are shipped as plain WKB-encoded
/// <see cref="byte"/>[] (the union's <c>bin</c>/BLOB member) — the C++ side's
/// <c>BuildColumnStatistics</c> tries a registered cast from BLOB to the column's declared
/// GEOMETRY type first, and falls back to reinterpreting the BLOB bytes as GEOMETRY when physical
/// representations match (<c>vgi_catalog_api.cpp</c>'s <c>cast_value</c> lambda) — exactly the WKB
/// bytes constructed here, so no cast is even needed.</para>
/// </summary>
public static class GeoPointsTable
{
    private const string SchemaName = "data";

    public static CatalogTable Table { get; } = Build();

    /// <summary>Minimal 21-byte ISO WKB POINT encoding: 1 byte order (1 = little-endian) + 4-byte
    /// uint32 geometry type (1 = POINT) + 8-byte double X + 8-byte double Y — the format DuckDB's
    /// GEOMETRY internal representation copies as-is (see the class doc comment).</summary>
    internal static byte[] WkbPoint(double x, double y)
    {
        var bytes = new byte[21];
        bytes[0] = 1; // little-endian byte order marker
        BitConverter.GetBytes((uint)1).CopyTo(bytes, 1); // geometry type: POINT
        BitConverter.GetBytes(x).CopyTo(bytes, 5);
        BitConverter.GetBytes(y).CopyTo(bytes, 13);
        return bytes;
    }

    private static CatalogTable Build()
    {
        var pointStruct = new StructType(
        [
            new Field("x", DoubleType.Default, nullable: false),
            new Field("y", DoubleType.Default, nullable: false),
        ]);
        var geomField = new Field(
            "geom",
            pointStruct,
            nullable: false,
            metadata: new Dictionary<string, string> { ["ARROW:extension:name"] = "geoarrow.point" });

        var schema = new Schema([new Field("id", Int64Type.Default, nullable: false), geomField], metadata: null);

        return new CatalogTable
        {
            Name = "geo_points",
            SchemaName = SchemaName,
            Comment = "Point geometries with spatial bounding-box statistics",
            // Declared columns only — deliberately NO ScanFunction. Neither test that touches this
            // fixture (table/column_statistics.test's vgi_table_statistics() diagnostic,
            // table/comments.test's listing, which explicitly excludes this table) ever scans it,
            // and table/function_registration.test's pinned 162-function inventory does NOT count
            // a "geo_points"-named table function — so this table must NOT also register a callable
            // scan function the way every other CatalogTable.ScanFunction here does (see
            // CatalogRegistry.RegisterCatalogTable's doc comment on that dedup-by-reference
            // convention). CatalogTable.ResolveColumns() falls back to Columns when ScanFunction is
            // null, so the catalog listing/statistics paths work fine without one.
            Columns = schema,
            NotNullColumns = ["id", "geom"],
            Statistics = new Dictionary<string, ColumnStatisticsInput>
            {
                ["id"] = new() { Min = 1L, Max = 2L, HasNull = false, HasNotNull = true, DistinctCount = 2 },
                ["geom"] = new()
                {
                    Min = WkbPoint(0.0, 0.0),
                    Max = WkbPoint(4.0, 4.0),
                    HasNull = false,
                    HasNotNull = true,
                    DistinctCount = 2,
                },
            },
            StatisticsCacheMaxAgeSeconds = 3600,
        };
    }
}
