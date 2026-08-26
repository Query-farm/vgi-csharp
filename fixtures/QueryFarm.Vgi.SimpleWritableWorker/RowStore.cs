using System.Buffers.Binary;
using System.Text;
using Apache.Arrow;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.SimpleWritableWorker;

/// <summary>
/// A tiny cross-process, file-backed row store for ONE writable fixture table — keyed by an
/// internally-generated row-id (a fresh <see cref="Guid"/> minted on INSERT; NOT any user-visible
/// column — see <see cref="WritableTableFixture"/>'s doc comment for why the row-id must be its own
/// hidden column rather than reusing a business column like <c>id</c>).
///
/// Backed by <see cref="FunctionStorage"/> (same file-backed cross-PROCESS primitive
/// table-buffering functions use) rather than plain in-memory state: this worker has no `launch:`
/// pooling configured (M7), but DuckDB can still spawn a genuinely NEW stdio subprocess between two
/// statements of the SAME session (observed empirically — an in-memory-only first attempt lost every
/// row between INSERT and the following SELECT). The storage KEY is scoped to <c>*Params.AttachOpaqueData</c>
/// (see that property's doc comment) rather than a fixed table-name string, so two independent
/// ATTACHes of this same catalog — e.g. two test files <c>scripts/run_tests.py</c> runs in PARALLEL —
/// get ISOLATED storage instead of corrupting each other's rows.
/// </summary>
public sealed class RowStore(string tableName)
{
    private const string Namespace = "rows";
    private const string MetaNamespace = "meta";
    private const string NextIdKey = "next_id";

    /// <summary>Mints the next row-id for this table (this attach session) — a monotonically
    /// increasing counter, NOT a random <see cref="Guid"/>: <see cref="ScanAll"/> orders rows by
    /// row-id, and a big-endian 8-byte counter's hex encoding sorts lexicographically the same as
    /// its numeric order, so scan results come back in INSERTION order — which
    /// <c>simple_writable/update.test</c>'s plain (non-<c>rowsort</c>) <c>RETURNING</c> assertions
    /// depend on. Not safe under concurrent inserts (read-then-write, no locking) — fine for this
    /// fixture's single-connection sequential test usage, not a general-purpose primitive.</summary>
    public byte[] NextRowId(byte[] attachOpaqueData)
    {
        var storage = StorageFor(attachOpaqueData);
        var existing = storage.ReadSingle(MetaNamespace, NextIdKey);
        var next = existing is { Length: 8 } ? BinaryPrimitives.ReadInt64BigEndian(existing) : 0L;

        var updated = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(updated, next + 1);
        storage.WriteSingle(MetaNamespace, NextIdKey, updated);

        var rowId = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(rowId, next);
        return rowId;
    }

    public void Put(byte[] attachOpaqueData, byte[] rowId, RecordBatch fullRow) =>
        StorageFor(attachOpaqueData).WriteSingle(Namespace, KeyFor(rowId), RecordBatchIpc.Write(fullRow));

    public RecordBatch? Get(byte[] attachOpaqueData, byte[] rowId)
    {
        var bytes = StorageFor(attachOpaqueData).ReadSingle(Namespace, KeyFor(rowId));
        return bytes is null ? null : RecordBatchIpc.Read(bytes);
    }

    public void Delete(byte[] attachOpaqueData, byte[] rowId) =>
        StorageFor(attachOpaqueData).DeleteSingle(Namespace, KeyFor(rowId));

    /// <summary>Every currently-stored row (for THIS attach session), each its own single-row
    /// full-schema batch, in no particular order.</summary>
    public IReadOnlyList<RecordBatch> ScanAll(byte[] attachOpaqueData)
    {
        var storage = StorageFor(attachOpaqueData);
        return storage.ListKeys(Namespace)
            .OrderBy(key => key, StringComparer.Ordinal)
            .Select(key => storage.ReadSingle(Namespace, key))
            .Where(bytes => bytes is not null)
            .Select(bytes => RecordBatchIpc.Read(bytes!))
            .ToList();
    }

    private FunctionStorage StorageFor(byte[] attachOpaqueData) =>
        new([.. attachOpaqueData, 0, .. Encoding.UTF8.GetBytes(tableName)]);

    private static string KeyFor(byte[] rowId) => Convert.ToHexString(rowId);
}
