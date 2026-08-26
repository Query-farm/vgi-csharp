using Apache.Arrow;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.TableInOut;

/// <summary>
/// The per-substream cursor an <see cref="ITableInOutFunction.CreateProcessor"/> returns — driven
/// once per input batch during the INPUT phase (<see cref="Process"/>, exchange-shaped: one output
/// batch per input batch turn) and, if the function has a finalize step
/// (<see cref="ITableInOutFunction.HasFinalize"/>), once per client "tick" during the FINALIZE phase
/// that follows on the SAME connection (<see cref="Finalize"/>, producer-shaped — mirrors
/// <see cref="Table.ITableFunctionProducer.Produce"/>).
/// </summary>
public interface ITableInOutProcessor
{
    /// <summary>Processes one input batch, emitting zero or one output batch via
    /// <paramref name="output"/>.Emit(...) (an EMPTY batch — 0 rows — is a legal "consumed but
    /// nothing to emit yet" turn, not end-of-stream; the client keeps sending more input). Never
    /// call <paramref name="output"/>.Finish() here — the INPUT phase ends when the client closes
    /// its input stream, not when this returns.</summary>
    void Process(RecordBatch input, OutputCollector output);

    /// <summary>Produces this FINALIZE tick's contribution — same contract as
    /// <see cref="Table.ITableFunctionProducer.Produce"/>: call <paramref name="output"/>.Emit(...)
    /// at most once and/or <paramref name="output"/>.Finish() once there is nothing left. Only
    /// invoked when <see cref="ITableInOutFunction.HasFinalize"/> is true; the default here ends
    /// the stream immediately (no finalize output) for functions that never override it.</summary>
    void Finalize(OutputCollector output) => output.Finish();
}
