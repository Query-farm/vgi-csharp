using Apache.Arrow;
using QueryFarm.Vgi.Buffering;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>Hard-kills the worker during the process phase to exercise client pool recovery.</summary>
public sealed class CrashOnProcessFunction() : SumAllColumnsFunction(
    "crash_on_process",
    includeLoggingArg: false,
    description: "Worker hard-kills itself during process (test)")
{
    public override byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams)
    {
        System.Diagnostics.Process.GetCurrentProcess().Kill();
        Thread.Sleep(Timeout.Infinite);
        throw new InvalidOperationException("crash_on_process failed to terminate the worker");
    }
}
