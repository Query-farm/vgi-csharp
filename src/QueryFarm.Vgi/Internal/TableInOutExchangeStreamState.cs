using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;
using QueryFarm.VgiRpc.Wire;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The per-substream streaming state for a table-in-out function's <c>init(phase=INPUT)</c>-opened
/// exchange: one input batch in, exactly one output batch out, per turn — or, for continued output
/// (<see cref="ContinuedOutput"/>), a continuation tick in and the input's next reply out, through
/// <see cref="ITableInOutProcessor.Resume"/>. See <see cref="ScalarStreamState"/> for the analogous
/// scalar-function version.
/// </summary>
/// <param name="function">The function the stream runs.</param>
/// <param name="processor">This substream's processor.</param>
/// <param name="onAbandoned">Called when the stream ends without reaching end of input — a turn
/// failed (the stream ends in that error) or the client cancelled — so a processor kept for a
/// FINALIZE phase that will now never come is released. Not called on a normal end of input.</param>
public sealed class TableInOutExchangeStreamState(
    ITableInOutFunction function,
    ITableInOutProcessor processor,
    Action? onAbandoned = null) : ExchangeState
{
    /// <summary>Whether the last reply said <c>HAVE_MORE_OUTPUT</c>, so the next input must be a
    /// continuation tick. Lives on this state, which every transport keeps for the stream's life.</summary>
    private bool _morePending;

    public override Task ExchangeAsync(AnnotatedBatch input, OutputCollector output, ICallContext? ctx, CancellationToken cancellationToken)
    {
        try
        {
            Turn(input, output);
        }
        catch
        {
            // Any exception out of a turn ends the stream with that error (vgi-rpc writes it and
            // stops reading this stream's input), so no FINALIZE follows.
            onAbandoned?.Invoke();
            throw;
        }

        return Task.CompletedTask;
    }

    public override void OnCancel(ICallContext? ctx) => onAbandoned?.Invoke();

    private void Turn(AnnotatedBatch input, OutputCollector output)
    {
        // A tick asks for the next reply of the last input's output, and is the only valid input
        // while that output is pending.
        var tick = ContinuedOutput.IsTick(input.Metadata);
        if (tick && !_morePending)
        {
            throw new InvalidOperationException($"{function.Name}: continuation tick received, but no output is pending");
        }

        if (tick && input.Batch.Length != 0)
        {
            throw new InvalidOperationException($"{function.Name}: a continuation tick must have no rows, got {input.Batch.Length}");
        }

        if (!tick && _morePending)
        {
            throw new InvalidOperationException($"{function.Name}: input received while output is pending; expected a continuation tick");
        }

        // ITableInOutProcessor.Process only receives the raw RecordBatch (not the AnnotatedBatch),
        // so a processor that needs this turn's incoming custom_metadata (e.g. a revalidation-aware
        // function reading vgi.cache.if_none_match off the input batch — see
        // OutputCollector.InputMetadata's doc comment) has no other way to see it. The validators
        // belong to one input: set per turn, they reset on every real input, and a tick (which
        // never carries them) offers none to Resume.
        output.InputMetadata = input.Metadata;
        using (ContinuedOutput.BeginExchangeTurn(output))
        {
            if (tick)
            {
                processor.Resume(output);
            }
            else
            {
                processor.Process(input.Batch, output);
            }
        }

        // Every turn is answered with exactly one reply (a zero-row batch for "no output yet").
        // A turn that emits nothing would write no reply at all on a pipe transport, and the
        // client, waiting for one, would hang.
        if (output.EmittedBatch is null)
        {
            throw new InvalidOperationException(tick
                ? $"{function.Name}: Resume emitted no reply to a continuation tick (No data batch was emitted)"
                : $"{function.Name}: Process emitted no reply to an input batch (No data batch was emitted); " +
                  "emit a zero-row batch when there is no output");
        }

        var more = ContinuedOutput.SaysMore(output.EmittedMetadata);
        if (more && !function.ContinuesOutput)
        {
            throw new InvalidOperationException(
                $"{function.Name}: emitted more output but does not declare ContinuesOutput");
        }

        // A tick's reply may not carry cache control, which stands for an input's whole output
        // and rides its first reply.
        if (tick && output.EmittedMetadata?.Keys.Any(k => k.StartsWith("vgi.cache.", StringComparison.Ordinal)) == true)
        {
            throw new InvalidOperationException(
                $"{function.Name}: cache control is only valid on an input's first reply, not on one answering a continuation tick");
        }

        _morePending = more;
    }
}
