using Apache.Arrow;
using QueryFarm.Vgi.Buffering;

namespace QueryFarm.Vgi.ExampleWorker.Buffering;

/// <summary>
/// Backs <c>exceptions.test</c>'s <c>exception_process</c> case: inherits
/// <see cref="SumAllColumnsFunction"/>'s bind/combine/finalize behavior but overrides
/// <see cref="Process"/> to raise on every SECOND batch it sees for a given execution (a
/// cross-process-safe ordinal, via <see cref="TableBufferingProcessParams.Storage"/>'s append-log)
/// and, on every OTHER batch, deliberately never stores real data — mirroring the regression this
/// fixture guards: a subclass overriding <c>process()</c> without calling the parent's storage
/// append must still let <c>finalize()</c> succeed cleanly (a zero-sum row) when nothing was ever
/// stored, rather than crashing on an empty accumulator.
/// </summary>
public sealed class ExceptionProcessFunction() : SumAllColumnsFunction(
    "exception_process", description: "Test function that raises exception during process")
{
    private const string BatchCounterNamespace = "exception_process";
    private const string BatchCounterKey = "batch_count";

    public override byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams)
    {
        // Cross-process-safe batch ordinal: append a tiny marker, then count how many markers
        // exist so far for this execution. Never appends the actual batch data (see class doc).
        processParams.Storage.Append(BatchCounterNamespace, BatchCounterKey, []);
        var ordinal = processParams.Storage.ScanLog(BatchCounterNamespace, BatchCounterKey).Count;
        if (ordinal % 2 == 0)
        {
            throw new InvalidOperationException($"Intentional exception on batch {ordinal}");
        }

        return processParams.ExecutionId;
    }
}
