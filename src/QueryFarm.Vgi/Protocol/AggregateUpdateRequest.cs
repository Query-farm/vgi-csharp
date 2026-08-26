using Apache.Arrow;

namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>aggregate_update</c> RPC's packed request — one call per DuckDB-side batch of rows being
/// folded into per-group accumulator state. <see cref="InputBatch"/>'s schema is always
/// <c>[__vgi_group_id: int64, ...the aggregate's non-const Param columns, in declaration order]</c>
/// — the C++ extension assigns each DuckDB aggregate state a fresh <c>group_id</c> (monotonic,
/// scoped to the whole bind) the first time it's touched by update/combine/finalize.
///
/// PROPERTY DECLARATION ORDER IS LOAD-BEARING — matches <c>AggregateUpdateRequestSchema</c> exactly:
/// function_name, execution_id, input_batch, attach_opaque_data, schema_name.
/// </summary>
public sealed class AggregateUpdateRequest
{
    public string FunctionName { get; set; } = "";

    public byte[] ExecutionId { get; set; } = [];

    public RecordBatch InputBatch { get; set; } = null!;

    public byte[]? AttachOpaqueData { get; set; }

    public string? SchemaName { get; set; }
}

/// <summary>The <c>aggregate_update</c> RPC's packed result — no fields (matches the C++
/// extension's generated <c>AggregateUpdateResultSchema</c>, an empty schema); still wrapped in the
/// standard <c>{result: binary}</c> outer envelope like every other unary RPC result.</summary>
public sealed class AggregateUpdateResult
{
}
