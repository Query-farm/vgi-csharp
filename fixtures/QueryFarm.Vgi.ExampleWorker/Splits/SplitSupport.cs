using System.Buffers.Binary;
using Apache.Arrow;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Splits;

/// <summary>
/// Shared plumbing for the <c>test/sql/integration/splits/*</c> fixture surface. Every
/// range-shaped split fixture in this namespace names its work as a payload encoding
/// <c>(ordinal, start, end)</c> over the integer range <c>[start, end)</c> of the <c>n</c> output
/// column (<see cref="SplitPayloadCodec"/>) and differs only in how it DIVIDES <c>[0, n)</c> into
/// ranges (<see cref="SplitRanges"/>) or in what it attaches to each emitted batch.
/// </summary>
internal static class SplitPayloadCodec
{
    public static byte[] Encode(long ordinal, long start, long end)
    {
        var bytes = new byte[24];
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(0, 8), ordinal);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(8, 8), start);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(16, 8), end);
        return bytes;
    }

    public static (long Ordinal, long Start, long End) Decode(byte[] payload) => (
        BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(0, 8)),
        BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(8, 8)),
        BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(16, 8)));
}

/// <summary>Resume-cursor codec for a paginated plan enumeration: the number of splits already
/// emitted for THIS scan's enumeration (see <c>SplitPaginatedFunction</c>). A place in the
/// ENUMERATION, not in the data — lives only for the duration of one plan-page loop.</summary>
internal static class SplitCursorCodec
{
    public static byte[] Encode(long emitted)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, emitted);
        return bytes;
    }

    public static long Decode(byte[]? cursor) =>
        cursor is { Length: 8 } bytes ? BinaryPrimitives.ReadInt64LittleEndian(bytes) : 0;
}

internal static class SplitRanges
{
    /// <summary>Divides <c>[0, count)</c> into <c>splits</c> pieces as evenly as possible — the
    /// remainder (when <c>count</c> doesn't divide evenly) lands one-per-split on the FIRST
    /// <c>count % splits</c> splits. <c>splits &lt;= 0</c> (or a negative <c>count</c>) yields no
    /// ranges at all.</summary>
    public static List<(long Start, long End)> Even(long count, long splits)
    {
        var result = new List<(long, long)>();
        if (splits <= 0 || count < 0)
        {
            return result;
        }

        var baseSize = count / splits;
        var remainder = count % splits;
        var pos = 0L;
        for (var i = 0L; i < splits; i++)
        {
            var size = baseSize + (i < remainder ? 1 : 0);
            result.Add((pos, pos + size));
            pos += size;
        }

        return result;
    }

    /// <summary>Puts ~99% of <c>[0, count)</c> in ONE split, dividing the remaining ~1% evenly
    /// across the rest — <c>skew.test</c>'s proof that correctness never depends on splits being
    /// evenly sized. <c>splits == 1</c> degenerates to a single full-range split.</summary>
    public static List<(long Start, long End)> Skewed(long count, long splits)
    {
        if (splits <= 0 || count < 0)
        {
            return [];
        }

        if (splits == 1)
        {
            return [(0, count)];
        }

        var smallTotal = count / 100;
        var bigSize = count - smallTotal;
        var result = new List<(long, long)> { (0, bigSize) };
        result.AddRange(Even(smallTotal, splits - 1).Select(r => (bigSize + r.Start, bigSize + r.End)));
        return result;
    }

    /// <summary>Divides <c>[0, count)</c> evenly across roughly half of <c>splits</c> "real"
    /// slots, then interleaves zero-row splits among the rest — every row is still covered
    /// exactly once, just with empty splits deliberately scattered through the enumeration (see
    /// <c>zero_row_split.test</c>: a zero-ROW split must not end the reader).</summary>
    public static List<(long Start, long End)> EmptyInterleaved(long count, long splits)
    {
        if (splits <= 0)
        {
            return [];
        }

        var realRanges = Even(count, Math.Max(1, splits / 2));
        var result = new List<(long, long)>();
        var nextReal = 0;
        for (var i = 0L; i < splits; i++)
        {
            if (i % 2 == 0 && nextReal < realRanges.Count)
            {
                result.Add(realRanges[nextReal]);
                nextReal++;
            }
            else
            {
                result.Add((0, 0));
            }
        }

        // Any real ranges left over (splits too small to interleave them all) still get emitted —
        // every row must be covered exactly once regardless of how they're arranged.
        while (nextReal < realRanges.Count)
        {
            result.Add(realRanges[nextReal]);
            nextReal++;
        }

        return result;
    }
}

/// <summary>Guards a split-only function's ordinary (non-split) init path — see
/// <c>splits/rollback.test</c>'s <c>vgi_split_scans=false</c> scenario, which disables the
/// client's plan/claim path entirely and falls back to calling <c>CreateProducer</c> with no
/// redeemed split at all. The message MUST contain "split-only" (pinned by that test).</summary>
internal static class SplitOnlyGuard
{
    public static IReadOnlyList<byte[]> RequireSingle(TableInitParams initParams, string functionName)
    {
        if (initParams.SplitPayloads is not { Count: 1 } payloads)
        {
            throw new InvalidOperationException(
                $"'{functionName}' is split-only and has no ordinary (non-split) scan path — " +
                "it was initialized with no redeemed split.");
        }

        return payloads;
    }
}

/// <summary>Redeems a single <c>(start, end)</c> range payload, emitting the integer sequence
/// <c>[start, end)</c> as the <c>n</c> column of <paramref name="outputSchema"/> in
/// <paramref name="batchSize"/>-row chunks. Reused by every range-shaped split fixture in this
/// namespace. <paramref name="metadata"/>, when given, is attached to every emitted batch (e.g.
/// cache-control metadata — see <c>Cache.CacheMetadata</c>; harmless to resend on every batch).</summary>
internal sealed class RangeProducer(
    long start, long end, Schema outputSchema, int batchSize = 500,
    Func<IReadOnlyDictionary<string, string>>? metadata = null) : ITableFunctionProducer
{
    private long _next = start;

    public void Produce(OutputCollector output)
    {
        if (_next >= end)
        {
            output.Finish();
            return;
        }

        var rows = (int)Math.Min(batchSize, end - _next);
        var builder = new Apache.Arrow.Int64Array.Builder();
        builder.Reserve(rows);
        for (var i = 0; i < rows; i++)
        {
            builder.Append(_next + i);
        }

        _next += rows;
        var batch = new RecordBatch(outputSchema, [builder.Build()], rows);
        if (metadata is null)
        {
            output.Emit(batch);
        }
        else
        {
            output.Emit(batch, metadata());
        }

        if (_next >= end)
        {
            output.Finish();
        }
    }
}
