namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The first batch written on the stream <c>init</c> opens — a stream HEADER (its own complete
/// IPC stream: schema + one row + EOS, written before the main output stream begins). See
/// <see cref="QueryFarm.VgiRpc.Streaming.IRpcStream.Header"/>. C++ reads every field by name and
/// tolerates any of them being absent, so property order doesn't matter here.
/// </summary>
public sealed class GlobalInitResponse
{
    public byte[] ExecutionId { get; set; } = [];

    public byte[]? OpaqueData { get; set; }

    public long MaxWorkers { get; set; } = 1;
}
