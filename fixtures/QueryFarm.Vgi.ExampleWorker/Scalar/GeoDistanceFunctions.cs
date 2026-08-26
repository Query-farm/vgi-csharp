using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>Euclidean distance between two fixed (non-varargs) points, one class per Arrow shape
/// DuckDB can carry a "point" as. See <see cref="GeoTypes"/>.</summary>
public sealed class GeoDistanceStructFunction : IScalarFunction
{
    public string Name => "geo_distance_struct";

    public string Description => "Euclidean distance between two struct points";

    public Schema ArgumentsSchema { get; } = new(
        [new Field("p1", GeoTypes.PointStructType, nullable: true), new Field("p2", GeoTypes.PointStructType, nullable: true)],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", DoubleType.Default, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams) =>
        GeoDistance.Compute(processParams, GeoTypes.ReadStructPoint);
}

public sealed class GeoDistanceListFunction : IScalarFunction
{
    public string Name => "geo_distance_list";

    public string Description => "Euclidean distance between two list points";

    public Schema ArgumentsSchema { get; } = new(
        [new Field("p1", GeoTypes.PointListType, nullable: true), new Field("p2", GeoTypes.PointListType, nullable: true)],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", DoubleType.Default, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams) =>
        GeoDistance.Compute(processParams, GeoTypes.ReadListPoint);
}

public sealed class GeoDistanceFixedFunction : IScalarFunction
{
    public string Name => "geo_distance_fixed";

    public string Description => "Euclidean distance between two fixed-size list points";

    public Schema ArgumentsSchema { get; } = new(
        [new Field("p1", GeoTypes.PointFixedType, nullable: true), new Field("p2", GeoTypes.PointFixedType, nullable: true)],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", DoubleType.Default, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams) =>
        GeoDistance.Compute(processParams, GeoTypes.ReadFixedPoint);
}

internal static class GeoDistance
{
    public static RecordBatch Compute(ScalarProcessParams processParams, Func<IArrowArray, int, (double Lat, double Lon)?> readPoint)
    {
        var p1 = processParams.Input.Column(0);
        var p2 = processParams.Input.Column(1);
        var length = processParams.Input.Length;
        var builder = new DoubleArray.Builder();
        for (var i = 0; i < length; i++)
        {
            var a = readPoint(p1, i);
            var b = readPoint(p2, i);
            if (a is null || b is null)
            {
                builder.AppendNull();
                continue;
            }

            builder.Append(GeoTypes.Euclidean(a.Value.Lat, a.Value.Lon, b.Value.Lat, b.Value.Lon));
        }

        return new RecordBatch(processParams.OutputSchema, [builder.Build()], length);
    }
}
