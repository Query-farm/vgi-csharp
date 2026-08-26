using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// Backs <c>exceptions.test</c>'s <c>exception_finalize</c> case: inherits
/// <see cref="SumAllColumnsFunction"/>'s bind/process/combine behavior unchanged but always raises
/// during the FINALIZE (Source) phase, on the very first tick. Also registered a second time under
/// the name <c>crash_on_finalize</c> (<paramref name="name"/>) for
/// <c>table_buffering_finalize_crash.test</c> — same behavior, different registration name so that
/// test's error-then-recovery narrative reads independently of the general exception-handling
/// coverage in <c>exceptions.test</c>.
/// </summary>
public sealed class ExceptionFinalizeFunction(string name = "exception_finalize") : SumAllColumnsFunction(
    name, description: "Test function that raises exception during finalize")
{
    public override ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
        new ThrowingProducer();

    private sealed class ThrowingProducer : ITableFunctionProducer
    {
        public void Produce(OutputCollector output) =>
            throw new InvalidOperationException("Intentional exception during finalize()");
    }
}
