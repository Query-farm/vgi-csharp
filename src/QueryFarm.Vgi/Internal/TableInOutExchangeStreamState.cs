using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;
using QueryFarm.VgiRpc.Wire;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The per-substream streaming state for a table-in-out function's <c>init(phase=INPUT)</c>-opened
/// exchange: one input batch in, at most one output batch out, per turn — or, for continued output
/// (<see cref="ContinuedOutput"/>), a continuation tick in and the input's next reply out, through
/// <see cref="ITableInOutProcessor.Resume"/>. See <see cref="ScalarStreamState"/> for the analogous
/// scalar-function version.
/// </summary>
public sealed class TableInOutExchangeStreamState(ITableInOutFunction function, ITableInOutProcessor processor) : ExchangeState
{
    /// <summary>Whether the last reply said <c>HAVE_MORE_OUTPUT</c>, so the next input must be a
    /// continuation tick. Lives on this state, which every transport keeps for the stream's life.</summary>
    private bool _morePending;

    public override Task ExchangeAsync(AnnotatedBatch input, OutputCollector output, ICallContext? ctx, CancellationToken cancellationToken)
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

        var more = ContinuedOutput.SaysMore(output.EmittedMetadata);
        if (more && !function.ContinuesOutput)
        {
            throw new InvalidOperationException(
                $"{function.Name}: emitted more output but does not declare ContinuesOutput");
        }

        if (tick)
        {
            // Every tick is answered with a reply (an empty one may still say more); none may carry
            // cache control, which stands for an input's whole output and rides its first reply.
            if (output.EmittedBatch is null)
            {
                throw new InvalidOperationException($"{function.Name}: Resume emitted no reply to a continuation tick");
            }

            if (output.EmittedMetadata?.Keys.Any(k => k.StartsWith("vgi.cache.", StringComparison.Ordinal)) == true)
            {
                throw new InvalidOperationException(
                    $"{function.Name}: cache control is only valid on an input's first reply, not on one answering a continuation tick");
            }
        }

        _morePending = more;
        return Task.CompletedTask;
    }
}
