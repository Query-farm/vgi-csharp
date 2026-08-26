using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Table;

/// <summary>Parameters an <see cref="ITableFunction"/> sees when its producer stream is opened
/// (mirrors the <c>init</c> RPC) — the table-function analog of <see cref="Scalar.ScalarProcessParams"/>,
/// except this fires once per call (not once per batch) since a table function's producer then
/// drives its own output pace.</summary>
public sealed class TableInitParams
{
    public required string FunctionName { get; init; }

    /// <summary>Same decoded arguments as the bind call that opened this producer (re-decoded
    /// rather than reused from <see cref="TableBindParams"/> — nothing about one call may be cached
    /// on the shared <see cref="ITableFunction"/> singleton).</summary>
    public required TableArguments Arguments { get; init; }

    public byte[]? Settings { get; init; }

    /// <summary>Fully-RESOLVED secrets from the bind call that opened this producer — by the time
    /// <c>init</c> runs, any two-phase secret-scope retry (see <see cref="Internal.SecretsAccessor"/>)
    /// has already completed, so this is plain already-resolved data: decode with
    /// <see cref="Internal.SecretArgCodec.Decode"/> then <see cref="Internal.SecretArgCodec.FindByType"/>/
    /// <see cref="Internal.SecretArgCodec.ForScopeOfType"/>. <c>null</c> when no secrets were resolved.</summary>
    public byte[]? Secrets { get; init; }

    /// <summary>The resolved per-call output schema (from <see cref="ITableFunction.ResolveOutputSchema"/>).
    /// Every batch the returned <see cref="ITableFunctionProducer"/> emits must use this schema —
    /// or the <see cref="ProjectedSchema"/> subset when <see cref="ProjectionIds"/> is non-null and
    /// this function advertises <see cref="ITableFunction.ProjectionPushdown"/>.</summary>
    public required Schema OutputSchema { get; init; }

    /// <summary>Zero-based indices (into <see cref="OutputSchema"/>) of the columns DuckDB actually
    /// needs — <see langword="null"/> means "all columns". Only meaningful when this function
    /// advertised <see cref="ITableFunction.ProjectionPushdown"/>; otherwise DuckDB still expects
    /// (and will itself trim) the FULL <see cref="OutputSchema"/> from every emitted batch.</summary>
    public IReadOnlyList<long>? ProjectionIds { get; init; }

    /// <summary>Convenience: <see cref="OutputSchema"/> narrowed to <see cref="ProjectionIds"/> (or
    /// the full schema when <see cref="ProjectionIds"/> is <see langword="null"/>) — the schema a
    /// projection-pushdown-aware producer should actually emit.</summary>
    public Schema ProjectedSchema =>
        ProjectionIds is null
            ? OutputSchema
            : new Schema(ProjectionIds.Select(i => OutputSchema.GetFieldByIndex((int)i)), metadata: null);

    /// <summary>Raw embedded-IPC pushdown-filter bytes (<c>InitRequest.PushdownFilters</c>) —
    /// <see langword="null"/> when DuckDB pushed no filters down. Only meaningful when this
    /// function advertised <see cref="ITableFunction.FilterPushdown"/>. Decode with
    /// <see cref="PushdownFilter.Decode"/>.</summary>
    public byte[]? PushdownFilters { get; init; }

    /// <summary>One embedded-IPC single-column batch per IN-filter/join-key column
    /// (<c>InitRequest.JoinKeys</c>) — a <c>pushdown_filters</c> node of type <c>"join_keys"</c>
    /// names which one of these (by its <c>keys_column</c> field) holds its candidate value set.
    /// Decode with <see cref="Internal.PushdownFilterCodec"/>'s join-key helpers.</summary>
    public IReadOnlyList<byte[]>? JoinKeys { get; init; }

    public long? RowLimit { get; init; }

    public string? OrderByColumnName { get; init; }

    public VgiOrderByDirection? OrderByDirection { get; init; }

    public VgiNullOrder? OrderByNullOrder { get; init; }

    public long? OrderByLimit { get; init; }

    public double? TablesamplePercentage { get; init; }

    public long? TablesampleSeed { get; init; }

    public byte[]? ExecutionId { get; init; }

    /// <summary>Raw <c>BindRequest.AttachOpaqueData</c> — see <see cref="TableBindParams.AttachOpaqueData"/>'s
    /// doc comment.</summary>
    public byte[] AttachOpaqueData { get; init; } = [];

    /// <summary>See <see cref="TableBindParams.TransactionOpaqueData"/> — the same value, re-decoded
    /// from the <see cref="Protocol.BindRequest"/> embedded in this init call.</summary>
    public byte[] TransactionOpaqueData { get; init; } = [];

    /// <summary>The VERIFIED, envelope-stripped <see cref="ScanSplit.Payload"/> bytes this init is
    /// redeeming — <see langword="null"/> for an ordinary (non-split) init, and a single-element
    /// list for a split init (the client redeems exactly one split per init call; see
    /// <c>AdvanceToNextSplit</c>'s greedy per-split claim loop). A function that declared
    /// <see cref="ITableFunction.SupportsSplits"/> but is only ever meant to be read through the
    /// split path can check this for <see langword="null"/> and refuse the ordinary-init fallback
    /// (see <c>splits/rollback.test</c>'s <c>vgi_split_scans=false</c> scenario).</summary>
    public IReadOnlyList<byte[]>? SplitPayloads { get; init; }

    /// <summary>Non-<see langword="null"/> only when this init opened a <c>COPY ... FROM</c> — see
    /// <see cref="TableBindParams.CopyFrom"/>'s doc comment.</summary>
    public Protocol.CopyFromContext? CopyFrom { get; init; }

    /// <summary>See <see cref="TableBindParams.AtUnit"/> — the same value, re-decoded from the
    /// <see cref="Protocol.BindRequest"/> embedded in this init call. This is where a function-backed
    /// table with a version-independent schema (so it has no need to override
    /// <see cref="ITableFunction.Bind"/>/<see cref="ITableFunction.ResolveOutputSchema"/>) should
    /// resolve its version — e.g. inside <see cref="ITableFunction.CreateProducer"/>.</summary>
    public string? AtUnit { get; init; }

    /// <summary>See <see cref="AtUnit"/>.</summary>
    public string? AtValue { get; init; }
}
