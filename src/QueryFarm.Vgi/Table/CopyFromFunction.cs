using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.Table;

/// <summary>
/// Base class for a <c>COPY ... FROM (FORMAT '&lt;name&gt;', ...)</c> reader — an ordinary
/// producer function whose output schema is dictated ENTIRELY by the COPY target (DuckDB inserts
/// no cast for COPY FROM, so <see cref="Read"/> must emit rows matching
/// <see cref="Protocol.CopyFromContext.ExpectedSchema"/> exactly: column-for-column, name AND
/// type, no more no fewer). Register via <see cref="Worker.RegisterCopyFromFormat"/>, which also
/// advertises it through <c>catalog_copy_from_formats</c>. Mirrors vgi-python/vgi-java's
/// <c>CopyFromFunction</c>.
/// </summary>
public abstract class CopyFromFunction : ITableFunction
{
    public abstract string Name { get; }

    public virtual string SchemaName => "main";

    public abstract string Description { get; }

    /// <summary>The format's OPTIONS — every field a named (optional or required) argument; never
    /// a positional or TABLE-typed argument (the row data is the COPY target itself, not a call
    /// argument).</summary>
    public abstract Schema ArgumentsSchema { get; }

    /// <summary>Never actually used — <see cref="ResolveOutputSchema"/> always overrides it with
    /// the COPY target's own required schema.</summary>
    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableBindParams bindParams) =>
        SchemaIpc.ReadSchemaOnly(RequireCopyFrom(bindParams.CopyFrom).ExpectedSchema);

    public void Bind(TableBindParams bindParams) => OnBind(bindParams, RequireCopyFrom(bindParams.CopyFrom));

    /// <summary>Override to validate options (throw for unsupported/missing-required) and/or
    /// request secrets via <see cref="TableBindParams.Secrets"/> — see
    /// <see cref="Internal.SecretsAccessor.Get"/>'s two-phase-retry doc comment. A no-op by
    /// default.</summary>
    protected virtual void OnBind(TableBindParams bindParams, CopyFromContext copyFrom)
    {
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(this, initParams, RequireCopyFrom(initParams.CopyFrom));

    /// <summary>Reads <paramref name="path"/> and hands every row to <paramref name="emit"/> (any
    /// number of calls, in any batch sizing) matching <paramref name="expectedSchema"/> exactly —
    /// runs synchronously to completion; the framework handles ticking emitted batches back one
    /// per turn.</summary>
    protected abstract void Read(string path, TableInitParams initParams, Schema expectedSchema, Action<RecordBatch> emit);

    private static CopyFromContext RequireCopyFrom(CopyFromContext? copyFrom) =>
        copyFrom ?? throw new InvalidOperationException(
            "This function is a COPY ... FROM format handler and must be invoked via " +
            "COPY ... FROM (FORMAT '<name>', ...), not called directly.");

    private sealed class Producer(CopyFromFunction owner, TableInitParams initParams, CopyFromContext copyFrom) : ITableFunctionProducer
    {
        private Queue<RecordBatch>? _pending;

        public void Produce(OutputCollector output)
        {
            if (_pending is null)
            {
                _pending = new Queue<RecordBatch>();
                var expectedSchema = SchemaIpc.ReadSchemaOnly(copyFrom.ExpectedSchema);
                owner.Read(copyFrom.FilePath, initParams, expectedSchema, batch => _pending.Enqueue(batch));
            }

            if (_pending.Count == 0)
            {
                output.Finish();
                return;
            }

            output.Emit(_pending.Dequeue());
            if (_pending.Count == 0)
            {
                output.Finish();
            }
        }
    }
}
