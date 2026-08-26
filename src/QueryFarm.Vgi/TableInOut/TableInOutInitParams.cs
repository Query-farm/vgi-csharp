using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;

namespace QueryFarm.Vgi.TableInOut;

/// <summary>Parameters an <see cref="ITableInOutFunction"/> sees when its per-substream processor is
/// created (mirrors the <c>init(phase=INPUT)</c> RPC) — fires once per substream, before any input
/// batches arrive.</summary>
public sealed class TableInOutInitParams
{
    public required string FunctionName { get; init; }

    public required TableArguments Arguments { get; init; }

    public byte[]? Settings { get; init; }

    /// <summary>Fully-RESOLVED secrets from the bind call that opened this processor — see
    /// <see cref="Table.TableInitParams.Secrets"/>'s doc comment.</summary>
    public byte[]? Secrets { get; init; }

    public required Schema InputSchema { get; init; }

    /// <summary>The resolved per-call output schema (from <see cref="ITableInOutFunction.ResolveOutputSchema"/>)
    /// — the function's FULL declared output shape, regardless of what this call actually
    /// requested. A processor that advertises <see cref="ITableInOutFunction.ProjectionPushdown"/>
    /// must emit <see cref="ProjectedSchema"/> instead (the wire itself is declared with the
    /// narrowed schema — see <see cref="ProjectionIds"/>'s doc comment); every other processor
    /// emits this FULL schema unconditionally.</summary>
    public required Schema OutputSchema { get; init; }

    /// <summary>Zero-based indices (into <see cref="OutputSchema"/>) of the columns DuckDB actually
    /// needs for THIS call, or <see langword="null"/> for "all columns" — the table-in-out analog
    /// of <see cref="Table.TableInitParams.ProjectionIds"/>. Only meaningful when this function
    /// advertised <see cref="ITableInOutFunction.ProjectionPushdown"/>; unlike a plain table
    /// function's producer (where an unadvertised function can still emit full-width batches and
    /// let DuckDB trim them client-side), the batched correlated-LATERAL operator VALIDATES the
    /// wire schema strictly against whatever it negotiated — so a
    /// <see cref="ITableInOutFunction.ProjectionPushdown"/>-advertising processor MUST emit exactly
    /// <see cref="ProjectedSchema"/>, not <see cref="OutputSchema"/>, whenever this is non-null.</summary>
    public IReadOnlyList<long>? ProjectionIds { get; init; }

    /// <summary>Convenience: <see cref="OutputSchema"/> narrowed to <see cref="ProjectionIds"/> (or
    /// the full schema when <see cref="ProjectionIds"/> is <see langword="null"/>) — the schema a
    /// projection-pushdown-aware processor should actually emit.</summary>
    public Schema ProjectedSchema =>
        ProjectionIds is null
            ? OutputSchema
            : new Schema(ProjectionIds.Select(i => OutputSchema.GetFieldByIndex((int)i)), metadata: null);

    /// <summary>Stable across the INPUT and (if this function has one) FINALIZE phase of ONE
    /// substream — mint/reuse a per-substream accumulator keyed by this if needed, though for the
    /// common case (finalize runs on the very same connection right after INPUT's EOS) ordinary
    /// mutable state captured by the <see cref="ITableInOutProcessor"/> instance itself is simpler
    /// and sufficient; this is provided for parity with the wire protocol, not because most
    /// functions need it.</summary>
    public byte[]? ExecutionId { get; init; }

    public byte[]? SubstreamId { get; init; }

    /// <summary>Raw <c>BindRequest.AttachOpaqueData</c> — see
    /// <see cref="Table.TableBindParams.AttachOpaqueData"/>'s doc comment.</summary>
    public byte[] AttachOpaqueData { get; init; } = [];
}
