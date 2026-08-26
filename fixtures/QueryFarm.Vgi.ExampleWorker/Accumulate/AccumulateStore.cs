using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.ExampleWorker.Accumulate;

/// <summary>
/// Fixture-ONLY, ATTACH-session-scoped persistent row store backing the
/// <c>accumulate</c>/<c>accumulate_read</c>/<c>accumulate_clear</c> probes
/// (test/sql/integration/accumulate/*.test) — NOT part of the shared QueryFarm.Vgi framework.
/// "accumulate" isn't a VGI protocol feature; it's example/test-fixture logic layered on plain
/// file I/O, mirroring vgi-python/vgi-java's own BoundStorage-backed <c>AccumulateStore</c> fixture
/// (a segmented, time-ordered append log built entirely on generic storage primitives — no
/// framework-level TTL/eviction support exists anywhere, by design).
///
/// Keyed by the FULL raw <c>attach_opaque_data</c> bytes (identity name + a random per-ATTACH
/// suffix — see <c>VgiServiceImpl.EncodeIdentity</c>'s doc comment) so two independent
/// <c>ATTACH</c>es of the SAME catalog name never share a collection
/// (<c>accumulate/attach_scope.test</c>), and by the collection <c>name</c> so distinct
/// collections under one attach are independent (<c>accumulate/basic.test</c>'s 'a'/'b'/'ts'/...).
///
/// Each <c>accumulate()</c> call's rows land in ONE file (a "segment"), named by that call's
/// timestamp (UTC ticks, zero-padded so lexicographic == chronological order) plus a random
/// suffix — segment order is entirely derived from the filename, and each segment's row count is
/// read back from its own content (never trusted from the filename) so a segment can be trimmed
/// in place (<see cref="TrimToNewest"/>) without invalidating anything else's bookkeeping.
/// </summary>
public sealed class AccumulateStore
{
    private readonly string _dir;

    public AccumulateStore(byte[] attachOpaqueData, string name)
    {
        var attachHash = Convert.ToHexStringLower(SHA256.HashData(attachOpaqueData));
        var nameHash = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)));
        _dir = Path.Combine(Path.GetTempPath(), "vgi_csharp_accumulate", attachHash, nameHash);
    }

    private string SchemaPath => Path.Combine(_dir, "schema.bin");

    private string RowsDir => Path.Combine(_dir, "rows");

    public bool Exists => File.Exists(SchemaPath);

    public Schema? ReadPinnedSchema() =>
        File.Exists(SchemaPath) ? SchemaIpc.ReadSchemaOnly(File.ReadAllBytes(SchemaPath)) : null;

    /// <summary>Pins <paramref name="schema"/> as this collection's schema if none is pinned yet —
    /// a no-op (NOT a re-validation) when one already exists; the caller (<c>AccumulateFunction.Bind</c>)
    /// is responsible for comparing against <see cref="ReadPinnedSchema"/> and rejecting a mismatch
    /// BEFORE calling this.</summary>
    public void EnsurePinnedSchema(Schema schema)
    {
        Directory.CreateDirectory(_dir);
        if (!File.Exists(SchemaPath))
        {
            File.WriteAllBytes(SchemaPath, SchemaIpc.WriteSchemaOnly(schema));
        }
    }

    public void AppendSegment(RecordBatch batch, DateTime timestampUtc)
    {
        Directory.CreateDirectory(RowsDir);
        var fileName = $"{timestampUtc.Ticks:D19}_{Guid.NewGuid():N}.arrow";
        File.WriteAllBytes(Path.Combine(RowsDir, fileName), RecordBatchIpc.Write(batch));
    }

    private IReadOnlyList<string> SegmentFiles() =>
        Directory.Exists(RowsDir)
            ? Directory.EnumerateFiles(RowsDir).OrderBy(f => f, StringComparer.Ordinal).ToList()
            : [];

    private static DateTime SegmentTimestamp(string path)
    {
        var ticksText = Path.GetFileNameWithoutExtension(path).Split('_')[0];
        return new DateTime(long.Parse(ticksText), DateTimeKind.Utc);
    }

    public IReadOnlyList<RecordBatch> ReadAllSegments() =>
        SegmentFiles().Select(f => RecordBatchIpc.Read(File.ReadAllBytes(f))).ToList();

    /// <summary>Every segment whose timestamp equals (i.e. was appended during) the given call —
    /// the <c>result := 'new'</c> read path.</summary>
    public IReadOnlyList<RecordBatch> ReadSegmentsAt(DateTime timestampUtc) =>
        SegmentFiles()
            .Where(f => SegmentTimestamp(f) == timestampUtc)
            .Select(f => RecordBatchIpc.Read(File.ReadAllBytes(f)))
            .ToList();

    public long RowCount => SegmentFiles().Sum(f => RecordBatchIpc.Read(File.ReadAllBytes(f)).Length);

    /// <summary>Deletes every segment strictly older than <paramref name="cutoffUtc"/> (TTL
    /// eviction — <c>call_time - ttl</c>). Returns the row count removed.</summary>
    public long EvictOlderThan(DateTime cutoffUtc)
    {
        long removed = 0;
        foreach (var f in SegmentFiles())
        {
            if (SegmentTimestamp(f) >= cutoffUtc)
            {
                continue;
            }

            removed += RecordBatchIpc.Read(File.ReadAllBytes(f)).Length;
            File.Delete(f);
        }

        return removed;
    }

    /// <summary>Keeps only the newest <paramref name="maxRows"/> rows: whole older segments are
    /// dropped, and the ONE segment straddling the boundary is trimmed to its newest (tail) rows
    /// in place. A no-op when <paramref name="maxRows"/> is non-positive (0 == "unlimited") or the
    /// collection already fits.</summary>
    public void TrimToNewest(long maxRows)
    {
        if (maxRows <= 0)
        {
            return;
        }

        long kept = 0;
        foreach (var f in SegmentFiles().Reverse()) // newest first
        {
            if (kept >= maxRows)
            {
                File.Delete(f);
                continue;
            }

            var batch = RecordBatchIpc.Read(File.ReadAllBytes(f));
            if (kept + batch.Length <= maxRows)
            {
                kept += batch.Length;
                continue;
            }

            var keepFromThis = (int)(maxRows - kept);
            var trimmed = batch.Slice(batch.Length - keepFromThis, keepFromThis);
            File.WriteAllBytes(f, RecordBatchIpc.Write(trimmed));
            kept = maxRows;
        }
    }

    /// <summary>Deletes the whole collection (schema pin + every segment). Returns the row count
    /// removed (0 for an already-empty/never-created collection).</summary>
    public long Clear()
    {
        if (!Directory.Exists(_dir))
        {
            return 0;
        }

        var total = RowCount;
        Directory.Delete(_dir, recursive: true);
        return total;
    }
}
