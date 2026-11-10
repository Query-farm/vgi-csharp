using System.Buffers.Binary;

namespace QueryFarm.Vgi.Internal;

/// <summary>Native evaluator for <c>duckdb.spatial/intersects_extent@1</c>, matching DuckDB core's
/// <c>a &amp;&amp; b</c> exactly: the XY bounding boxes of two WKB geometries intersect (inclusive
/// boundaries, exact double comparison, Z and M ignored). A geometry with no vertices — empty, or
/// a POINT whose coordinates are all NaN — intersects nothing. A vertex with NaN on one axis makes
/// that axis unbounded for its geometry. Accepts ISO WKB (Z/M as +1000/+2000/+3000) and EWKB
/// (Z/M/SRID flag bits), either byte order per (sub)geometry.</summary>
internal static class WkbExtent
{
    private const uint EwkbZ = 0x80000000;
    private const uint EwkbM = 0x40000000;
    private const uint EwkbSrid = 0x20000000;

    /// <exception cref="FormatException">A value is not well-formed WKB.</exception>
    /// <exception cref="NotSupportedException">A value has an unsupported geometry type.</exception>
    public static bool Intersects(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        var a = Read(left);
        var b = Read(right);
        return a.HasVertex && b.HasVertex &&
            a.MinX <= b.MaxX && b.MinX <= a.MaxX && a.MinY <= b.MaxY && b.MinY <= a.MaxY;
    }

    private static Box Read(ReadOnlySpan<byte> wkb)
    {
        var box = new Box();
        var position = 0;
        ReadGeometry(wkb, ref position, ref box, depth: 0);
        if (position != wkb.Length)
        {
            throw new FormatException($"Unexpected trailing data in WKB at position {position}.");
        }

        return box;
    }

    private static void ReadGeometry(ReadOnlySpan<byte> wkb, ref int position, ref Box box, int depth)
    {
        if (depth > 256)
        {
            throw new FormatException("WKB geometry is nested too deeply.");
        }

        var order = Take(wkb, ref position, 1)[0];
        if (order > 1)
        {
            throw new FormatException($"Invalid WKB byte order {order}.");
        }

        var littleEndian = order == 1;
        var raw = UInt32(wkb, ref position, littleEndian);
        var hasZ = (raw & EwkbZ) != 0;
        var hasM = (raw & EwkbM) != 0;
        if ((raw & EwkbSrid) != 0)
        {
            UInt32(wkb, ref position, littleEndian);
        }

        var code = raw & 0x0FFFFFFF;
        var (baseType, iso) = (code % 1000, code / 1000);
        if (iso > 3 || baseType is < 1 or > 7)
        {
            throw new NotSupportedException($"Unsupported WKB geometry type {raw}.");
        }

        var dimensions = 2 + (hasZ || iso is 1 or 3 ? 1 : 0) + (hasM || iso is 2 or 3 ? 1 : 0);
        switch (baseType)
        {
            case 1:
                ReadVertex(wkb, ref position, ref box, dimensions, littleEndian, isPoint: true);
                break;
            case 2:
                ReadVertices(wkb, ref position, ref box, dimensions, littleEndian);
                break;
            case 3:
                for (var ring = Count(wkb, ref position, littleEndian, 4); ring > 0; ring--)
                {
                    ReadVertices(wkb, ref position, ref box, dimensions, littleEndian);
                }

                break;
            default:
                for (var part = Count(wkb, ref position, littleEndian, 5); part > 0; part--)
                {
                    ReadGeometry(wkb, ref position, ref box, depth + 1);
                }

                break;
        }
    }

    private static void ReadVertices(ReadOnlySpan<byte> wkb, ref int position, ref Box box, int dimensions,
        bool littleEndian)
    {
        for (var vertex = Count(wkb, ref position, littleEndian, dimensions * 8); vertex > 0; vertex--)
        {
            ReadVertex(wkb, ref position, ref box, dimensions, littleEndian, isPoint: false);
        }
    }

    private static void ReadVertex(ReadOnlySpan<byte> wkb, ref int position, ref Box box, int dimensions,
        bool littleEndian, bool isPoint)
    {
        var x = Double(wkb, ref position, littleEndian);
        var y = Double(wkb, ref position, littleEndian);
        var allNaN = double.IsNaN(x) && double.IsNaN(y);
        for (var extra = 2; extra < dimensions; extra++)
        {
            allNaN &= double.IsNaN(Double(wkb, ref position, littleEndian));
        }

        // A POINT with every coordinate NaN is WKB's encoding of POINT EMPTY.
        if (isPoint && allNaN)
        {
            return;
        }

        box.HasVertex = true;
        Extend(x, ref box.MinX, ref box.MaxX);
        Extend(y, ref box.MinY, ref box.MaxY);
    }

    private static void Extend(double value, ref double min, ref double max)
    {
        if (double.IsNaN(value))
        {
            // A NaN coordinate makes the whole axis unbounded for this geometry.
            min = double.NegativeInfinity;
            max = double.PositiveInfinity;
            return;
        }

        min = Math.Min(min, value);
        max = Math.Max(max, value);
    }

    private static int Count(ReadOnlySpan<byte> wkb, ref int position, bool littleEndian, int minimumElementSize)
    {
        var count = UInt32(wkb, ref position, littleEndian);
        if (count > (uint)(wkb.Length - position) / (uint)minimumElementSize)
        {
            throw new FormatException($"WKB element count {count} exceeds the remaining data.");
        }

        return (int)count;
    }

    private static uint UInt32(ReadOnlySpan<byte> wkb, ref int position, bool littleEndian)
    {
        var bytes = Take(wkb, ref position, 4);
        return littleEndian ? BinaryPrimitives.ReadUInt32LittleEndian(bytes) : BinaryPrimitives.ReadUInt32BigEndian(bytes);
    }

    private static double Double(ReadOnlySpan<byte> wkb, ref int position, bool littleEndian)
    {
        var bytes = Take(wkb, ref position, 8);
        return littleEndian ? BinaryPrimitives.ReadDoubleLittleEndian(bytes) : BinaryPrimitives.ReadDoubleBigEndian(bytes);
    }

    private static ReadOnlySpan<byte> Take(ReadOnlySpan<byte> wkb, ref int position, int length)
    {
        if (length > wkb.Length - position)
        {
            throw new FormatException($"WKB is truncated at position {position}.");
        }

        var slice = wkb.Slice(position, length);
        position += length;
        return slice;
    }

    private struct Box
    {
        public bool HasVertex;
        public double MinX = double.PositiveInfinity;
        public double MaxX = double.NegativeInfinity;
        public double MinY = double.PositiveInfinity;
        public double MaxY = double.NegativeInfinity;

        public Box()
        {
        }
    }
}
