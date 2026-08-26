using Apache.Arrow;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Table;

/// <summary>Parameters an <see cref="ITableFunction"/> sees at bind time.</summary>
public sealed class TableBindParams
{
    public required string FunctionName { get; init; }

    /// <summary>Opaque, not-yet-decoded serialized argument bytes — decode with
    /// <see cref="TableArgCodec.Decode"/> (or use <see cref="Arguments"/>, already decoded).</summary>
    public byte[] ArgumentsBytes { get; init; } = [];

    /// <summary>The decoded view of <see cref="ArgumentsBytes"/> — every positional/named SQL
    /// argument this call was made with (all bind-time constants; a table function has no per-row
    /// input).</summary>
    public required TableArguments Arguments { get; init; }

    /// <summary>Opaque, not-yet-decoded serialized settings bytes (one row, columns named by
    /// DuckDB setting key) — <see langword="null"/> when this function declared no
    /// <see cref="ITableFunction.RequiredSettings"/>.</summary>
    public byte[]? Settings { get; init; }

    /// <summary>Secrets access for this bind attempt — statically pre-resolved secrets are readable
    /// immediately via <see cref="Internal.SecretsAccessor.Resolved"/>; a DYNAMIC (call-argument-
    /// derived) scope lookup goes through <see cref="Internal.SecretsAccessor.Get"/> from
    /// <see cref="ITableFunction.Bind"/>, which triggers the C++ extension's two-phase bind retry when
    /// unresolved. See <see cref="Internal.SecretsAccessor"/>'s doc comment.</summary>
    public required Internal.SecretsAccessor Secrets { get; init; }

    /// <summary>The concrete per-call argument schema DuckDB resolved (decoded from
    /// <see cref="Protocol.BindRequest.InputSchema"/>), field-for-field matching
    /// <see cref="ITableFunction.ArgumentsSchema"/>'s declared positional/named order — the REAL
    /// resolved type behind any <c>ANY</c>-typed/varargs argument (e.g. <c>constant_columns</c>).
    /// <see langword="null"/> when the call site declared no such dynamic arguments.</summary>
    public Schema? InputSchema { get; init; }

    /// <summary>Raw <see cref="Protocol.BindRequest.AttachOpaqueData"/> — echoed back verbatim by
    /// the C++ extension for every RPC belonging to ONE <c>ATTACH</c>'s lifetime, so (unlike
    /// <see cref="CatalogRegistry.DefaultIdentity"/>'s name-derived, deterministic-per-name routing
    /// key) it is safe to use as a genuinely per-attach-SESSION unique key — e.g. scoping a writable
    /// fixture's durable row store so two independent <c>ATTACH</c>es of the same catalog (two
    /// parallel test files, or two attaches in one session) never see each other's data. Empty when
    /// the call carries none.</summary>
    public byte[] AttachOpaqueData { get; init; } = [];

    /// <summary>Raw <see cref="Protocol.BindRequest.TransactionOpaqueData"/> — non-empty only when
    /// this call runs inside an explicit SQL transaction (<c>BEGIN</c>/<c>COMMIT</c>/<c>ROLLBACK</c>)
    /// on a catalog that advertised <see cref="Protocol.CatalogAttachResult.SupportsTransactions"/>;
    /// empty for an autocommit statement (each gets its own fresh, effectively-unused transaction)
    /// or a catalog that doesn't support transactions at all. Use it as a per-transaction storage
    /// key (see <c>Internal.FunctionStorage</c> — already cross-process/durable, so it doubles as
    /// transaction-scoped storage keyed by this value instead of an execution id) for state that
    /// must survive across multiple binds within ONE transaction and be cleared on COMMIT/ROLLBACK —
    /// see <c>table/transaction_storage.test</c>'s <c>tx_cached_value</c> fixture.</summary>
    public byte[] TransactionOpaqueData { get; init; } = [];

    /// <summary>Non-<see langword="null"/> only when this bind opened a <c>COPY ... FROM (FORMAT
    /// '&lt;this function's name&gt;', ...)</c> — the destination path and the exact output schema
    /// DuckDB requires (no cast is inserted for COPY FROM). See <see cref="Buffering.CopyToFunction"/>'s
    /// sibling doc comment for the COPY TO side.</summary>
    public Protocol.CopyFromContext? CopyFrom { get; init; }

    /// <summary>The time-travel <c>AT (VERSION =&gt; ...)</c>/<c>AT (TIMESTAMP =&gt; ...)</c> clause
    /// unit/value this bind carries (<see cref="Protocol.BindRequest.AtUnit"/>/<c>AtValue</c>) —
    /// both <see langword="null"/> when the query has no AT clause. A function that resolves its own
    /// version (rather than relying on a catalog-level <see cref="Catalog.CatalogTable.ResolveAtClause"/>/
    /// <see cref="Catalog.CatalogTable.ResolveScanArguments"/> swap) reads these directly — see
    /// <c>table/time_travel_pushdown.test</c>'s <c>tt_pushdown_fn</c>.</summary>
    public string? AtUnit { get; init; }

    /// <summary>See <see cref="AtUnit"/>.</summary>
    public string? AtValue { get; init; }
}
