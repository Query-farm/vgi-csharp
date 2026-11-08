using Apache.Arrow;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.TableInOut;

/// <summary>
/// An <see cref="ITableInOutProcessor"/> whose turns are asynchronous — for a processor that awaits
/// I/O per input batch (a write function calling a remote API, say) instead of blocking a thread on
/// it. Return one from <see cref="ITableInOutFunction.CreateProcessor"/> exactly as a synchronous
/// processor: the SDK awaits <see cref="ProcessAsync"/>, <see cref="ResumeAsync"/> and
/// <see cref="FinalizeAsync"/> with the same contracts as <see cref="ITableInOutProcessor.Process"/>,
/// <see cref="ITableInOutProcessor.Resume"/> and <see cref="ITableInOutProcessor.Finalize"/>.
/// </summary>
/// <remarks>
/// <see cref="ITableInOutProcessor.Process"/> is implemented for you (it blocks on
/// <see cref="ProcessAsync"/>), so code that drives a processor synchronously keeps working; the
/// SDK's own dispatch never calls it for an asynchronous processor. <see cref="ResumeAsync"/> and
/// <see cref="FinalizeAsync"/> default to the synchronous <see cref="ITableInOutProcessor.Resume"/>
/// and <see cref="ITableInOutProcessor.Finalize"/>; override whichever your function uses.
/// </remarks>
public interface IAsyncTableInOutProcessor : ITableInOutProcessor
{
    /// <summary>Processes one input batch, emitting exactly one reply — see
    /// <see cref="ITableInOutProcessor.Process"/>.</summary>
    Task ProcessAsync(RecordBatch input, OutputCollector output, CancellationToken cancellationToken);

    /// <summary>Emits the next reply for the last input batch — see
    /// <see cref="ITableInOutProcessor.Resume"/>.</summary>
    Task ResumeAsync(OutputCollector output, CancellationToken cancellationToken)
    {
        Resume(output);
        return Task.CompletedTask;
    }

    /// <summary>Produces this FINALIZE tick's contribution — see
    /// <see cref="ITableInOutProcessor.Finalize"/>.</summary>
    Task FinalizeAsync(OutputCollector output, CancellationToken cancellationToken)
    {
        Finalize(output);
        return Task.CompletedTask;
    }

    void ITableInOutProcessor.Process(RecordBatch input, OutputCollector output) =>
        ProcessAsync(input, output, CancellationToken.None).GetAwaiter().GetResult();
}
