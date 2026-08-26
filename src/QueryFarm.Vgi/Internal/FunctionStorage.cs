using QueryFarm.Vgi.Buffering;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// File-backed <see cref="IFunctionStorage"/> — see that interface's doc comment for why table-
/// buffering needs cross-PROCESS (not just cross-thread) durable state. One instance is scoped to
/// one <c>execution_id</c> (a whole query execution); every namespace/key pair underneath gets its
/// own directory of small append-only files (log entries) or a single overwritten file (the
/// <see cref="WriteSingle"/>/<see cref="ReadSingle"/> pair <see cref="VgiServiceImpl"/> uses to
/// stash the query's bind context — arguments/settings — where the standalone
/// <c>table_buffering_process</c>/<c>table_buffering_combine</c> unary RPCs, which carry neither on
/// the wire, can still recover them regardless of which pooled worker process serves the call).
///
/// Root directory: <c>%TEMP%/vgi_csharp_buffering/&lt;execution_id-hex&gt;/...</c> — same
/// <see cref="Path.GetTempPath"/> convention as <see cref="CrossProcessWorkQueue"/>, so it works
/// identically across processes without configuration. <see cref="DeleteAll"/> (best-effort, called
/// from <c>table_buffering_destructor</c>) wipes one execution's whole subtree.
/// </summary>
public sealed class FunctionStorage(byte[] executionId) : IFunctionStorage
{
    private readonly string _root = RootFor(executionId);

    public void Append(string ns, string key, byte[] value)
    {
        var dir = LogDir(ns, key);
        Directory.CreateDirectory(dir);
        // Sortable-by-filename entry order: a zero-padded tick prefix (append order, best-effort —
        // ties broken by the trailing guid) plus a guid for cross-process uniqueness (two processes
        // racing the same tick must never collide on one filename). Write-to-temp-then-rename keeps
        // a concurrent ScanLog from ever observing a partially-written file.
        var name = $"{DateTime.UtcNow.Ticks:D20}_{Guid.NewGuid():N}.bin";
        var finalPath = Path.Combine(dir, name);
        var tmpPath = finalPath + ".tmp";
        File.WriteAllBytes(tmpPath, value);
        File.Move(tmpPath, finalPath);
    }

    public IReadOnlyList<byte[]> ScanLog(string ns, string key)
    {
        var dir = LogDir(ns, key);
        if (!Directory.Exists(dir))
        {
            return [];
        }

        return Directory.GetFiles(dir, "*.bin")
            .OrderBy(path => path, StringComparer.Ordinal)
            .Select(File.ReadAllBytes)
            .ToList();
    }

    /// <summary>Overwrites the single value stored for <paramref name="ns"/>/<paramref name="key"/>
    /// (unlike <see cref="Append"/>'s log semantics) — used for one-shot bind-context storage.</summary>
    public void WriteSingle(string ns, string key, byte[] value)
    {
        var dir = SingleDir(ns);
        Directory.CreateDirectory(dir);
        var finalPath = Path.Combine(dir, SanitizeKey(key) + ".bin");
        // Unique per call (unlike a bare "finalPath + .tmp") — a multi-worker table-buffering
        // execution can have TWO pooled workers race to WriteSingle the same (ns, key) bind-context
        // entry concurrently. A shared temp filename means whichever call's File.Move(overwrite:
        // true) runs second finds its own temp file already renamed away by the first (or
        // overwritten mid-write), throwing FileNotFoundException instead of just losing the race
        // harmlessly — the way Append's already-unique (tick + guid) temp name does.
        var tmpPath = $"{finalPath}.{Guid.NewGuid():N}.tmp";
        File.WriteAllBytes(tmpPath, value);
        File.Move(tmpPath, finalPath, overwrite: true);
    }

    public byte[]? ReadSingle(string ns, string key)
    {
        var path = Path.Combine(SingleDir(ns), SanitizeKey(key) + ".bin");
        return File.Exists(path) ? File.ReadAllBytes(path) : null;
    }

    /// <summary>Removes the single value stored for <paramref name="ns"/>/<paramref name="key"/>, if
    /// any — a no-op if it was never written (or already deleted). Used by a writable-table DELETE
    /// implementation backed by this class as a simple per-row-id key/value store (see
    /// <c>Worker.RegisterCatalogTable</c>'s writable-table fixtures).</summary>
    public void DeleteSingle(string ns, string key)
    {
        var path = Path.Combine(SingleDir(ns), SanitizeKey(key) + ".bin");
        File.Delete(path);
    }

    /// <summary>Every key currently written under <paramref name="ns"/> (via <see cref="WriteSingle"/>,
    /// not yet <see cref="DeleteSingle"/>-d) — NOT necessarily in insertion order (directory listing
    /// order). NOTE: <see cref="SanitizeKey"/> is lossy (non-alphanumeric characters all collapse to
    /// <c>'_'</c>), so this returns the SANITIZED form, not necessarily the original key — fine for a
    /// row-id key space that's itself alphanumeric (e.g. a formatted integer).</summary>
    public IReadOnlyList<string> ListKeys(string ns)
    {
        var dir = SingleDir(ns);
        return Directory.Exists(dir)
            ? Directory.GetFiles(dir, "*.bin").Select(path => Path.GetFileNameWithoutExtension(path)).ToList()
            : [];
    }

    /// <summary>Best-effort recursive delete of everything stored for one execution — called from
    /// <c>table_buffering_destructor</c>. Swallows I/O errors (another process may still be
    /// mid-read); this is cleanup, not a correctness requirement.</summary>
    public static void DeleteAll(byte[] executionId)
    {
        var root = RootFor(executionId);
        if (!Directory.Exists(root))
        {
            return;
        }

        try
        {
            Directory.Delete(root, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private static string RootFor(byte[] executionId) =>
        Path.Combine(Path.GetTempPath(), "vgi_csharp_buffering", Convert.ToHexString(executionId));

    private string LogDir(string ns, string key) => Path.Combine(_root, "log", SanitizeKey(ns), SanitizeKey(key));

    private string SingleDir(string ns) => Path.Combine(_root, "kv", SanitizeKey(ns));

    private static string SanitizeKey(string key)
    {
        Span<char> buffer = stackalloc char[key.Length];
        for (var i = 0; i < key.Length; i++)
        {
            var c = key[i];
            buffer[i] = char.IsAsciiLetterOrDigit(c) ? c : '_';
        }

        return new string(buffer);
    }
}
