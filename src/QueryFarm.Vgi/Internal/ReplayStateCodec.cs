using System.Text;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Serializes/deserializes a growing list of doubles or strings as opaque <c>byte[]</c> — the
/// accumulator shape an ORDER-INDEPENDENT-BUT-NEEDS-EVERY-RAW-VALUE aggregate (percentile/median,
/// listagg) uses for its per-group <see cref="Aggregate.IAggregateFunction"/> state: rather than
/// folding rows into a running scalar, these replay every value at <c>finalize</c> time (sort for a
/// percentile, join for a listagg). Cheap length-prefixed binary encoding — no Arrow IPC overhead
/// for what is, in every fixture using this, a handful to a few thousand values per group.
/// </summary>
public static class ReplayStateCodec
{
    public static byte[] WriteDoubles(IReadOnlyList<double> values)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(values.Count);
            foreach (var v in values)
            {
                writer.Write(v);
            }
        }

        return stream.ToArray();
    }

    public static List<double> ReadDoubles(byte[]? bytes)
    {
        var result = new List<double>();
        if (bytes is null || bytes.Length == 0)
        {
            return result;
        }

        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
        var count = reader.ReadInt32();
        result.Capacity = count;
        for (var i = 0; i < count; i++)
        {
            result.Add(reader.ReadDouble());
        }

        return result;
    }

    public static byte[] WriteStrings(IReadOnlyList<string> values)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(values.Count);
            foreach (var v in values)
            {
                writer.Write(v);
            }
        }

        return stream.ToArray();
    }

    public static List<string> ReadStrings(byte[]? bytes)
    {
        var result = new List<string>();
        if (bytes is null || bytes.Length == 0)
        {
            return result;
        }

        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
        var count = reader.ReadInt32();
        result.Capacity = count;
        for (var i = 0; i < count; i++)
        {
            result.Add(reader.ReadString());
        }

        return result;
    }
}
