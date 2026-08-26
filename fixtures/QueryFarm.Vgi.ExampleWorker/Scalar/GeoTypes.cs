using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// Shared point-array shapes/math for the six <c>geo_distance_*</c>/<c>geo_centroid_*</c>
/// fixtures — plain 2D Euclidean geometry (NOT haversine/geographic distance; <c>lat</c>/<c>lon</c>
/// are just axis labels here, matching vgi-java's <c>GeoTypes.euclidean</c>).
/// </summary>
internal static class GeoTypes
{
    public static readonly StructType PointStructType = new(new[]
    {
        new Field("lat", DoubleType.Default, nullable: true),
        new Field("lon", DoubleType.Default, nullable: true),
    });

    public static readonly ListType PointListType = new(new Field("item", DoubleType.Default, nullable: true));

    public static readonly FixedSizeListType PointFixedType = new(DoubleType.Default, 2);

    public static Field VarargsField(string name, IArrowType elementType) =>
        new(name, elementType, nullable: true, new Dictionary<string, string> { [VgiWireMetadata.VarargsKey] = VgiWireMetadata.VarargsTrueValue });

    public static double Euclidean(double lat1, double lon1, double lat2, double lon2)
    {
        var dlat = lat2 - lat1;
        var dlon = lon2 - lon1;
        return Math.Sqrt(dlat * dlat + dlon * dlon);
    }

    public static (double Lat, double Lon)? ReadStructPoint(IArrowArray array, int index)
    {
        if (array is not StructArray s || s.IsNull(index))
        {
            return null;
        }

        double? lat = null;
        double? lon = null;
        var fields = ((StructType)s.Data.DataType).Fields;
        for (var i = 0; i < fields.Count; i++)
        {
            if (s.Fields[i] is not DoubleArray column || column.IsNull(index))
            {
                continue;
            }

            switch (fields[i].Name)
            {
                case "lat": lat = column.GetValue(index); break;
                case "lon": lon = column.GetValue(index); break;
            }
        }

        return lat is null || lon is null ? null : (lat.Value, lon.Value);
    }

    public static (double Lat, double Lon)? ReadListPoint(IArrowArray array, int index)
    {
        if (array is not ListArray list || list.IsNull(index))
        {
            return null;
        }

        return ReadPointFromValues(list.GetSlicedValues(index));
    }

    public static (double Lat, double Lon)? ReadFixedPoint(IArrowArray array, int index)
    {
        if (array is not FixedSizeListArray list || list.IsNull(index))
        {
            return null;
        }

        return ReadPointFromValues(list.GetSlicedValues(index));
    }

    private static (double Lat, double Lon)? ReadPointFromValues(IArrowArray values)
    {
        if (values is not DoubleArray d || d.Length < 2 || d.IsNull(0) || d.IsNull(1))
        {
            return null;
        }

        return (d.GetValue(0)!.Value, d.GetValue(1)!.Value);
    }

    public static StructArray BuildPointStructArray(IReadOnlyList<(double Lat, double Lon)?> points)
    {
        var latBuilder = new DoubleArray.Builder();
        var lonBuilder = new DoubleArray.Builder();
        var validity = new ArrowBuffer.BitmapBuilder();
        var nullCount = 0;
        foreach (var point in points)
        {
            if (point is null)
            {
                latBuilder.AppendNull();
                lonBuilder.AppendNull();
                validity.Append(false);
                nullCount++;
            }
            else
            {
                latBuilder.Append(point.Value.Lat);
                lonBuilder.Append(point.Value.Lon);
                validity.Append(true);
            }
        }

        return new StructArray(
            PointStructType,
            points.Count,
            [latBuilder.Build(), lonBuilder.Build()],
            validity.Build(),
            nullCount);
    }
}
