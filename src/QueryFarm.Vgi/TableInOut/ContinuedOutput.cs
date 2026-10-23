using System.Runtime.CompilerServices;
using Apache.Arrow;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.TableInOut;

/// <summary>
/// Continued table-in-out output: several replies for one input batch. Mirrors vgi-python's
/// <c>emit(..., more=True)</c> / <c>resume()</c>.
///
/// <para>A table-in-out exchange answers each input batch with one reply. A function that needs
/// several (one upstream API page per reply, or more output than it wants to build at once)
/// declares <see cref="ITableInOutFunction.ContinuesOutput"/>, emits every reply but the last with
/// <see cref="Emit"/>'s <c>more: true</c> (<c>vgi.status: HAVE_MORE_OUTPUT</c> on the reply), and
/// implements <see cref="ITableInOutProcessor.Resume"/>. The client asks for each next reply with a
/// continuation tick — a zero-row batch of the input schema carrying <c>vgi.status: CONTINUE</c> —
/// which the framework answers by calling <see cref="ITableInOutProcessor.Resume"/>. Every reply's
/// <c>vgi_rpc.parent_row#b64</c> indexes the last real (non-tick) input batch.</para>
///
/// <para>The protocol is strict, with no capability negotiation: while output is pending a tick is
/// the only valid input, and a tick is valid only then; cache control (<c>vgi.cache.*</c>) may ride
/// an input's first reply only; every turn emits exactly one reply; and "more" — whether from
/// <c>more: true</c> or spelled directly as <c>vgi.status: HAVE_MORE_OUTPUT</c> in the emit
/// metadata — is refused outside a table-in-out exchange turn. When the client closes its input with output still pending (a LIMIT stopped asking), the
/// pending output is simply dropped.</para>
/// </summary>
public static class ContinuedOutput
{
    /// <summary>The batch custom_metadata key carrying the continuation status.</summary>
    public const string StatusKey = "vgi.status";

    /// <summary><see cref="StatusKey"/>'s value on a reply that has more output for the same input.</summary>
    public const string HaveMoreOutput = "HAVE_MORE_OUTPUT";

    /// <summary><see cref="StatusKey"/>'s value on a continuation tick (input side).</summary>
    public const string Continue = "CONTINUE";

    /// <summary>The collectors of the table-in-out exchange turns in flight — the only place
    /// <c>more: true</c> is valid. vgi-rpc's <see cref="OutputCollector"/> is sealed and has no slot
    /// for it, so the exchange registers each turn's collector here for that turn's duration.</summary>
    private static readonly ConditionalWeakTable<OutputCollector, object> s_exchangeTurns = new();

    /// <summary>Emits this turn's table-in-out reply, saying whether more output follows for the
    /// same input batch. <c>more: false</c> is a plain emit, so the last reply can pass its own
    /// condition:
    /// <code>output.Emit(batch, metadata, more: state.Row &lt; state.Counts.Length);</code>
    /// Valid only from the <see cref="ITableInOutProcessor.Process"/> or
    /// <see cref="ITableInOutProcessor.Resume"/> of a function that declares
    /// <see cref="ITableInOutFunction.ContinuesOutput"/>.</summary>
    public static void Emit(this OutputCollector output, RecordBatch batch, IReadOnlyDictionary<string, string>? metadata, bool more)
    {
        ArgumentNullException.ThrowIfNull(output);
        if (!more)
        {
            output.Emit(batch, metadata);
            return;
        }

        if (!s_exchangeTurns.TryGetValue(output, out _))
        {
            throw new InvalidOperationException(
                "Emit(more: true) is only valid in a table-in-out function's Process or Resume");
        }

        var merged = metadata is null ? new Dictionary<string, string>() : new Dictionary<string, string>(metadata);
        merged[StatusKey] = HaveMoreOutput;
        output.Emit(batch, merged);
    }

    /// <summary>Whether <paramref name="metadata"/> (a reply's) says more output follows.</summary>
    internal static bool SaysMore(IReadOnlyDictionary<string, string>? metadata) =>
        metadata is not null && metadata.TryGetValue(StatusKey, out var status) && status == HaveMoreOutput;

    /// <summary>Refuses a reply outside a table-in-out exchange (a table function's producer, a
    /// table-in-out FINALIZE, a scalar) that says more output follows, however it was spelled —
    /// <c>Emit(more: true)</c> already refuses there, so this catches <c>vgi.status:
    /// HAVE_MORE_OUTPUT</c> put in the emit metadata directly. No client sends a continuation tick
    /// on such a stream, so it would wait for output that never comes.</summary>
    internal static void RefuseOutsideExchange(string emitter, IReadOnlyDictionary<string, string>? metadata)
    {
        if (SaysMore(metadata))
        {
            throw new InvalidOperationException(
                $"{emitter}: emitted {StatusKey}: {HaveMoreOutput}, which is only valid on a table-in-out " +
                "exchange's reply (Process or Resume)");
        }
    }

    /// <summary>Whether <paramref name="metadata"/> (an exchange input batch's) marks a continuation
    /// tick rather than real input.</summary>
    internal static bool IsTick(IReadOnlyDictionary<string, string>? metadata) =>
        metadata is not null && metadata.TryGetValue(StatusKey, out var status) && status == Continue;

    /// <summary>Marks <paramref name="output"/> as a table-in-out exchange turn's collector until the
    /// returned scope is disposed.</summary>
    internal static IDisposable BeginExchangeTurn(OutputCollector output)
    {
        s_exchangeTurns.AddOrUpdate(output, output);
        return new TurnScope(output);
    }

    private sealed class TurnScope(OutputCollector output) : IDisposable
    {
        public void Dispose() => s_exchangeTurns.Remove(output);
    }
}
