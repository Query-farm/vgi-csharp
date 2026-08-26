using QueryFarm.Vgi.Buffering;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// Backs <c>table_buffering_combine_crash.test</c>: inherits <see cref="SumAllColumnsFunction"/>'s
/// bind/process behavior unchanged (every Sink call succeeds normally, returning the shared
/// execution-id state_id) but always raises during the Combine phase, before any finalize producer
/// is ever built — exercises the C++ Sink::Finalize unwind path (as opposed to
/// <see cref="ExceptionFinalizeFunction"/>'s Source-phase failure).
/// </summary>
public sealed class CrashOnCombineFunction() : SumAllColumnsFunction(
    "crash_on_combine", description: "Test function that raises exception during combine")
{
    public override IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams) =>
        throw new InvalidOperationException("Intentional exception during combine()");
}
