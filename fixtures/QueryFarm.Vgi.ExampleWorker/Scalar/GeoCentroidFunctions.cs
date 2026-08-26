using Apache.Arrow;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>Arithmetic-mean centroid of N (varargs, N&gt;=1) points, one class per Arrow shape a
/// point can arrive as. See <see cref="GeoTypes"/>.</summary>
public sealed class GeoCentroidStructFunction : IScalarFunction
{
    public string Name => "geo_centroid_struct";

    public string Description => "Centroid of N struct points";

    public Schema ArgumentsSchema { get; } = new([GeoTypes.VarargsField("point", GeoTypes.PointStructType)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", GeoTypes.PointStructType, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams) => GeoCentroid.Compute(processParams, GeoTypes.ReadStructPoint);
}

public sealed class GeoCentroidListFunction : IScalarFunction
{
    public string Name => "geo_centroid_list";

    public string Description => "Centroid of N list points";

    public Schema ArgumentsSchema { get; } = new([GeoTypes.VarargsField("point", GeoTypes.PointListType)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", GeoTypes.PointStructType, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams) => GeoCentroid.Compute(processParams, GeoTypes.ReadListPoint);
}

public sealed class GeoCentroidFixedFunction : IScalarFunction
{
    public string Name => "geo_centroid_fixed";

    public string Description => "Centroid of N fixed-size list points";

    public Schema ArgumentsSchema { get; } = new([GeoTypes.VarargsField("point", GeoTypes.PointFixedType)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", GeoTypes.PointStructType, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams) => GeoCentroid.Compute(processParams, GeoTypes.ReadFixedPoint);
}

internal static class GeoCentroid
{
    public static RecordBatch Compute(ScalarProcessParams processParams, Func<IArrowArray, int, (double Lat, double Lon)?> readPoint)
    {
        var length = processParams.Input.Length;
        var columnCount = processParams.Input.ColumnCount;
        var columns = Enumerable.Range(0, columnCount).Select(processParams.Input.Column).ToList();
        var results = new List<(double Lat, double Lon)?>(length);

        for (var row = 0; row < length; row++)
        {
            double latSum = 0;
            double lonSum = 0;
            var anyNull = false;
            foreach (var column in columns)
            {
                var point = readPoint(column, row);
                if (point is null)
                {
                    anyNull = true;
                    break;
                }

                latSum += point.Value.Lat;
                lonSum += point.Value.Lon;
            }

            results.Add(anyNull || columnCount == 0 ? null : (latSum / columnCount, lonSum / columnCount));
        }

        var resultArray = GeoTypes.BuildPointStructArray(results);
        return new RecordBatch(processParams.OutputSchema, [resultArray], length);
    }
}
