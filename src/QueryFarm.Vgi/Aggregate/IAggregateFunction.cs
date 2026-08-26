using Apache.Arrow;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Aggregate;

/// <summary>
/// The raw contract a VGI aggregate function implements — <c>SELECT f(...) FROM t GROUP BY g</c>.
/// Ported from vgi-python's/vgi-java's <c>AggregateFunction</c>, reshaped into the hand-rolled,
/// Arrow-array-native style this port's other four function kinds
/// (<see cref="Scalar.IScalarFunction"/>, <see cref="Table.ITableFunction"/>,
/// <see cref="TableInOut.ITableInOutFunction"/>, <see cref="Buffering.ITableBufferingFunction"/>)
/// already use, rather than attribute-driven reflection binding.
///
/// Five phases, matching the C++ extension's <c>AggregateFunction</c> registration
/// (<c>vgi_aggregate_function_impl.cpp</c>):
/// <list type="number">
/// <item><b>Bind</b> — <see cref="Bind"/>/<see cref="ResolveOutputSchema"/>, once per bound call
/// site — validate arguments, resolve a dynamic (ANY) output type.</item>
/// <item><b>Update</b> — <see cref="Update"/>, once per input batch (parallel across
/// threads/processes) — fold rows into per-group accumulator state, keyed by an opaque
/// <c>group_id</c> the C++ side assigns (NOT the SQL-level GROUP BY key — purely an internal
/// per-DuckDB-aggregate-state handle).</item>
/// <item><b>Combine</b> — <see cref="Combine"/>, whenever DuckDB merges two states (parallel
/// aggregation across threads, OR a window segment tree) — fold one group's state into another's.</item>
/// <item><b>Finalize</b> — <see cref="Finalize"/>, once per requested batch of group ids — produce
/// the single output value each group id resolves to.</item>
/// <item><b>Destructor</b> — best-effort cleanup, handled generically by
/// <c>VgiServiceImpl</c> (wipes ALL of this execution's stored state) — no per-function hook needed.</item>
/// </list>
///
/// State lifecycle: every group's accumulator state is opaque <c>byte[]</c> this function chooses
/// its own encoding for (a few packed primitives for <c>vgi_sum</c>/<c>vgi_avg</c>, an embedded
/// Arrow IPC batch for something that needs to replay raw rows at finalize time like
/// <c>nest_tensor</c>/<c>vgi_percentile</c>). It is NEVER cached in-memory across calls — DuckDB
/// spawns a SEPARATE OS PROCESS per parallel worker under the stdio/subprocess transport, so
/// anything one call needs a LATER call (possibly in a different process) to see must round-trip
/// through the state bytes <see cref="Update"/>/<see cref="Combine"/> return.
///
/// "Must reassign to persist": <see cref="Update"/>'s <c>states</c> dictionary is pre-populated
/// ONLY with existing on-disk bytes for group ids that already had saved state; a brand-new group
/// id is simply ABSENT until this implementation adds an entry for it. After the call returns,
/// EVERY entry present in the dictionary (new or updated) is persisted — an entry never touched
/// this call (present neither before nor added during it) stays untouched. This is what makes "the
/// post-update state happens to be byte-identical to some notion of empty" harmless: presence in
/// the dictionary, not a value comparison, is what triggers a write.
/// </summary>
public interface IAggregateFunction
{
    string Name { get; }

    string SchemaName => "main";

    string Description => "";

    /// <summary>Optional free-text comment surfaced via <c>duckdb_functions().comment</c>
    /// (<see cref="Protocol.FunctionInfo.Comment"/>). <see langword="null"/> (the default) reports
    /// no comment.</summary>
    string? Comment => null;

    /// <summary>Optional key/value tags surfaced via <c>duckdb_functions().tags</c>
    /// (<see cref="Protocol.FunctionInfo.Tags"/>). Empty (the default) reports no tags.</summary>
    IReadOnlyDictionary<string, string> Tags => new Dictionary<string, string>();

    IReadOnlyList<string> Categories => [];

    /// <summary>The aggregate's full SQL-visible positional argument list (const AND non-const, in
    /// declaration order) — a const ("ConstParam") field carries
    /// <c>{VgiWireMetadata.ConstKey: VgiWireMetadata.ConstTrueValue}</c> metadata so the C++ side
    /// erases it before <see cref="Update"/> ever sees a column for it (mirrors
    /// <see cref="Scalar.IScalarFunction.ArgumentsSchema"/>'s const-field convention). A varargs
    /// field carries <c>VgiWireMetadata.VarargsKey</c> metadata instead, same as a scalar's.</summary>
    Schema ArgumentsSchema { get; }

    /// <summary>The STATIC single-field output schema used for catalog/DuckDB registration — use
    /// <see cref="Internal.AnyScalarSchema.SingleResult"/> for a dynamic (ANY) return type and
    /// override <see cref="ResolveOutputSchema"/> to resolve the concrete per-call type.</summary>
    Schema OutputSchema { get; }

    FunctionStability? Stability => null;

    IReadOnlyList<string> RequiredSettings => [];

    /// <summary>Secrets this function needs statically resolved and shipped on <c>aggregate_bind</c>
    /// — see <see cref="Table.ITableFunction.RequiredSecrets"/>'s doc comment. An aggregate supports
    /// ONLY static resolution — no dynamic two-phase retry (secret *values* are bind-time-only; the
    /// C++ extension ships an EMPTY <see cref="AggregateBindParams.Secrets"/> to
    /// <see cref="Update"/>/<see cref="Combine"/>/<see cref="Finalize"/>, so a bind-time decision
    /// (e.g. the finalize output type) must be threaded through the output schema, not re-read).</summary>
    IReadOnlyList<RequiredSecret> RequiredSecrets => [];

    AggregateOrderDependent OrderDependent => AggregateOrderDependent.NotOrderDependent;

    AggregateDistinctDependent DistinctDependent => AggregateDistinctDependent.NotDistinctDependent;

    /// <summary>Called once per bound call site — a no-op by default; override to validate
    /// arguments eagerly (a bad ConstParam should fail at bind time, not deep inside finalize).</summary>
    void Bind(AggregateBindParams bindParams)
    {
    }

    /// <summary>Resolves the schema to use for THIS specific call. Defaults to the static
    /// <see cref="OutputSchema"/>; override for a dynamic (ANY) return type resolved from
    /// <see cref="AggregateBindParams.InputSchema"/>.</summary>
    Schema ResolveOutputSchema(AggregateBindParams bindParams) => OutputSchema;

    /// <summary>Folds one batch of input rows into per-group state. <paramref name="inputColumns"/>
    /// (already stripped of the synthetic group-id column) and <paramref name="groupIds"/> are
    /// row-parallel — row <c>i</c>'s columns belong to group <c>groupIds[i]</c>. See this
    /// interface's doc comment for the "must reassign to persist" contract on
    /// <paramref name="states"/>.</summary>
    void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams);

    /// <summary>Merges <paramref name="source"/>'s state into <paramref name="target"/>'s, returning
    /// the merged state to persist under the target group id. <paramref name="source"/> is always
    /// non-null (the caller never invokes this for an unseen source — nothing to merge);
    /// <paramref name="target"/> is <see langword="null"/> when the target group id has no prior
    /// state (typical response: treat <paramref name="source"/> as the identity-merged result).
    /// <paramref name="source"/>'s own stored state is left untouched — one source may feed several
    /// targets (a window segment tree); only <c>aggregate_destructor</c> frees state.</summary>
    byte[] Combine(byte[] source, byte[]? target, AggregateCallParams callParams);

    /// <summary>Produces one output value per requested group id. <paramref name="states"/> is
    /// parallel to <paramref name="groupIds"/> — <see langword="null"/> at index <c>i</c> means group
    /// <paramref name="groupIds"/>[i] never appeared in any <see cref="Update"/>/<see cref="Combine"/>
    /// call for this execution (an empty input table, or every row skipped under DEFAULT null
    /// handling) — this implementation decides what that means for its own result (SQL NULL for
    /// SUM, 0 for COUNT, etc.). Returns an array of length <c>groupIds.Length</c> matching
    /// <paramref name="outputSchema"/>'s single field's type.</summary>
    IArrowArray Finalize(long[] groupIds, byte[]?[] states, Schema outputSchema, AggregateCallParams callParams);
}
