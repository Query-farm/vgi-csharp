namespace QueryFarm.Vgi.Buffering;

/// <summary>
/// Durable, cross-PROCESS storage scoped to one query execution — the coordination primitive
/// <see cref="ITableBufferingFunction.Process"/>/<see cref="ITableBufferingFunction.Combine"/>/the
/// FINALIZE producer use to hand state to each other. Necessary because, unlike a table-in-out
/// function's substream (which stays on ONE connection end to end), table-buffering's
/// <c>table_buffering_process</c>/<c>table_buffering_combine</c> calls are each independently
/// worker-pool-acquired unary RPCs — under the stdio/subprocess transport that can mean a SEPARATE
/// OS PROCESS per call (see <c>Internal.CrossProcessWorkQueue</c>'s doc comment for the same
/// discovery in the plain-table-function context), so in-memory state on one call is invisible to
/// the next. The concrete implementation (<c>Internal.FunctionStorage</c>) backs this with files
/// under the OS temp directory, keyed by the query's <c>execution_id</c> — durable and visible
/// across process boundaries without any extra runtime dependency (matches vgi-python's
/// <c>BoundStorage</c> role, minus the SQLite backend).
/// </summary>
public interface IFunctionStorage
{
    /// <summary>Appends one opaque blob to the durable log for <paramref name="ns"/>/<paramref name="key"/>
    /// — never overwrites; <see cref="ScanLog"/> returns every appended entry, in append order.</summary>
    void Append(string ns, string key, byte[] value);

    /// <summary>Every blob appended so far for <paramref name="ns"/>/<paramref name="key"/>, oldest
    /// first. Empty (never null) if nothing was ever appended.</summary>
    IReadOnlyList<byte[]> ScanLog(string ns, string key);
}
