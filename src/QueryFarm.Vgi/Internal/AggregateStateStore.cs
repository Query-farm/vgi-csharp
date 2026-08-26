using System.Globalization;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Thin typed view over <see cref="FunctionStorage"/> for one aggregate execution: per-group
/// accumulator state (keyed by the C++ side's opaque <c>group_id</c>) plus the bind-time const
/// arguments every standalone <c>aggregate_update</c>/<c>_combine</c>/<c>_finalize</c> unary call
/// needs to recover (it doesn't ride their own wire request — see
/// <see cref="Aggregate.AggregateCallParams"/>'s doc comment). Layout mirrors vgi-java's
/// <c>AggregateStateStore</c>: one namespace for state, one for the saved arguments blob.
/// </summary>
public sealed class AggregateStateStore(FunctionStorage storage)
{
    private const string StateNamespace = "aggregate_state";
    private const string ArgsNamespace = "aggregate_bindargs";
    private const string ArgsKey = "args";

    public byte[]? ReadState(long groupId) => storage.ReadSingle(StateNamespace, Key(groupId));

    public void WriteState(long groupId, byte[] value) => storage.WriteSingle(StateNamespace, Key(groupId), value);

    public void SaveArgs(byte[] argumentsBytes) => storage.WriteSingle(ArgsNamespace, ArgsKey, argumentsBytes);

    public byte[]? LoadArgs() => storage.ReadSingle(ArgsNamespace, ArgsKey);

    private static string Key(long groupId) => groupId.ToString(CultureInfo.InvariantCulture);
}
