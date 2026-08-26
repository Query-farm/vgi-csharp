using Apache.Arrow;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.Buffering;

/// <summary>
/// Base class for a <c>COPY ... TO (FORMAT '&lt;name&gt;', ...)</c> writer — mechanically a
/// table-buffering (Sink+Combine) function with NO Source phase: <see cref="Process"/> writes one
/// input batch, <see cref="Combine"/> runs exactly once (on the coordinator, after every batch has
/// been written) to close the destination, and the (never actually drained — COPY TO produces no
/// rows) finalize producer just finishes immediately. Register via
/// <see cref="Worker.RegisterCopyToFormat"/>, which also advertises it through
/// <c>catalog_copy_from_formats</c> (the RPC that covers both directions) — including this class's
/// <see cref="SinkOrderDependent"/> as the format's <c>ordered</c> flag. Mirrors vgi-python/
/// vgi-java's <c>CopyToFunction</c>.
///
/// Cross-process invariant: <see cref="Write"/> (Sink) and <see cref="Close"/> (Combine, exactly
/// once) may run on DIFFERENT worker processes (pool rotation, or <c>pool false</c>) — any state
/// one needs to hand the other MUST go through <see cref="TableBufferingProcessParams.Storage"/>/
/// <see cref="TableBufferingCombineParams.Storage"/>, never an in-memory field on this instance.
/// </summary>
public abstract class CopyToFunction : ITableBufferingFunction
{
    public abstract string Name { get; }

    public virtual string SchemaName => "main";

    public abstract string Description { get; }

    /// <summary>The format's OPTIONS — every field a named argument; never a positional or
    /// TABLE-typed argument (the input TABLE argument this Sink+Source shape implies is the COPY
    /// source itself, wired automatically — <see cref="Worker.RegisterCopyToFormat"/> never adds
    /// an explicit <see cref="TableArgFields.Table"/> field for it).</summary>
    public abstract Schema ArgumentsSchema { get; }

    /// <summary>COPY TO produces no output rows.</summary>
    public Schema OutputSchema { get; } = new([], metadata: null);

    /// <summary><see langword="true"/> forces a single-threaded, source-ordered sink — override
    /// when row order in the destination must match source order (the C++ operator otherwise
    /// shards the sink across threads/processes, arbitrary interleaving).</summary>
    public virtual bool SinkOrderDependent => false;

    public void Bind(TableInOutBindParams bindParams) => OnBind(bindParams, RequireCopyTo(bindParams.CopyTo));

    /// <summary>Override to validate options and/or request secrets via
    /// <see cref="TableInOutBindParams.Secrets"/> — see <see cref="Internal.SecretsAccessor.Get"/>'s
    /// two-phase-retry doc comment. A no-op by default.</summary>
    protected virtual void OnBind(TableInOutBindParams bindParams, CopyToContext copyTo)
    {
    }

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => OutputSchema;

    public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams)
    {
        Write(batch, processParams, RequireCopyTo(processParams.CopyTo).FilePath);
        return processParams.ExecutionId;
    }

    public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams)
    {
        Close(combineParams, RequireCopyTo(combineParams.CopyTo).FilePath);
        return [];
    }

    public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
        new NoOutputProducer();

    /// <summary>Sink phase: write one input batch to <paramref name="filePath"/> (or, more likely,
    /// stash it via <paramref name="processParams"/>.Storage for <see cref="Close"/> to assemble —
    /// see this class's cross-process invariant doc comment).</summary>
    protected abstract void Write(RecordBatch batch, TableBufferingProcessParams processParams, string filePath);

    /// <summary>Combine phase (exactly once): finalize/close the destination — the last chance to
    /// assemble every <see cref="Write"/> call's contribution (via <paramref name="combineParams"/>.Storage)
    /// into the real file at <paramref name="filePath"/>.</summary>
    protected abstract void Close(TableBufferingCombineParams combineParams, string filePath);

    private static CopyToContext RequireCopyTo(CopyToContext? copyTo) =>
        copyTo ?? throw new InvalidOperationException(
            "This function is a COPY ... TO format handler and must be invoked via " +
            "COPY ... TO (FORMAT '<name>', ...), not called directly.");

    private sealed class NoOutputProducer : ITableFunctionProducer
    {
        public void Produce(OutputCollector output) => output.Finish();
    }
}
