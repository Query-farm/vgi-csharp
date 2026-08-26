namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>aggregate_bind</c> RPC's packed request — mirrors <see cref="BindRequest"/> but scoped to
/// what an aggregate actually needs (no <c>FunctionType</c>/<c>copy_from</c>/<c>copy_to</c>/etc.,
/// which only the shared <c>bind</c> RPC's scalar/table/table-in-out paths use).
///
/// PROPERTY DECLARATION ORDER IS LOAD-BEARING — matches the C++ extension's generated
/// <c>AggregateBindRequestSchema</c> exactly: function_name, arguments, input_schema, settings,
/// secrets, attach_opaque_data, schema_name.
/// </summary>
public sealed class AggregateBindRequest
{
    public string FunctionName { get; set; } = "";

    /// <summary>Bind-time constant ("ConstParam") values only — embedded IPC struct
    /// <c>positional_&lt;i&gt;</c>, re-indexed sequentially over JUST the const positions (see
    /// <see cref="Internal.TableArgCodec"/>'s doc comment and the C++ extension's
    /// <c>BuildAggregateBindRequest</c>: const values are collected in declaration order, not by
    /// their original argument index).</summary>
    public byte[] Arguments { get; set; } = [];

    /// <summary>Schema-only IPC bytes describing the NON-const ("Param") input columns — the shape
    /// every <c>aggregate_update</c> call's <c>input_batch</c> carries (plus the synthetic
    /// <c>__vgi_group_id</c> column prepended by the C++ side).</summary>
    public byte[]? InputSchema { get; set; }

    public byte[]? Settings { get; set; }

    public byte[]? Secrets { get; set; }

    public byte[]? AttachOpaqueData { get; set; }

    public string? SchemaName { get; set; }
}

/// <summary>The <c>aggregate_bind</c> RPC's packed result (the dataclass embedded under the
/// method's own auto-wrapped <c>result</c> field) — property order matches the C++ extension's
/// generated <c>AggregateBindResultSchema</c>: output_schema, execution_id.</summary>
public sealed class AggregateBindResult
{
    /// <summary>Schema-only IPC bytes for the (possibly dynamically-resolved, for an ANY-typed
    /// return) single-field output schema.</summary>
    public byte[] OutputSchema { get; set; } = [];

    /// <summary>Scopes every subsequent <c>aggregate_update</c>/<c>_combine</c>/<c>_finalize</c>/
    /// <c>_destructor</c> call for this bound aggregate — minted fresh per bind, shared by every
    /// parallel worker connection DuckDB spawns for the one query.</summary>
    public byte[] ExecutionId { get; set; } = [];
}
