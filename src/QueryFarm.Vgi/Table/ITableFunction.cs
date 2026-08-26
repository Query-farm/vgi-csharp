using Apache.Arrow;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Table;

/// <summary>
/// The raw contract a table ("producer") function implements — the VGI analog of
/// <see cref="Scalar.IScalarFunction"/> for the <c>ProducerState</c> stream kind (client sends
/// empty "tick" batches; the server emits data batches until it calls
/// <c>OutputCollector.Finish()</c>). Ported from vgi-java's <c>TableFunction</c>/vgi-python's
/// <c>TableFunction</c>, adapted to C#'s immutable-array model.
///
/// A table function's SQL arguments are ALWAYS bind-time constants — there is no per-row input the
/// way a scalar function has one (that's what makes table-in-out/<c>ITableInOutFunction</c>, M4, a
/// different interface) — so <see cref="TableBindParams.Arguments"/>/<see cref="TableInitParams.Arguments"/>
/// decode with <see cref="Internal.TableArgCodec"/>, not <see cref="Internal.ScalarArgCodec"/>.
/// </summary>
public interface ITableFunction
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

    /// <summary>Describes the function's positional AND named (keyword) arguments. Named
    /// arguments carry <c>vgi_arg=named</c> metadata (see <see cref="Internal.VgiWireMetadata"/>) —
    /// field ORDER still matters (it's the order <see cref="Internal.TableArgCodec"/>'s
    /// <c>positional_&lt;i&gt;</c> indices are assigned), but named fields are looked up by name on
    /// both the wire and in <see cref="Internal.TableArgCodec.Decode"/>.</summary>
    Schema ArgumentsSchema { get; }

    /// <summary>The function's STATIC/declared output schema — one field per output column (unlike
    /// a scalar function's single-field output). For a dynamic-output function (e.g. a varargs
    /// function whose output columns mirror its call-site arguments) this is a best-effort/empty
    /// placeholder; the REAL per-call schema comes from <see cref="ResolveOutputSchema"/>.</summary>
    Schema OutputSchema { get; }

    FunctionStability? Stability => null;

    IReadOnlyList<string> RequiredSettings => [];

    /// <summary>Secrets this function needs statically resolved and shipped on every bind — see
    /// <see cref="Protocol.RequiredSecret"/>. A function whose secret scope depends on its OWN call
    /// arguments (e.g. <c>scoped_secret_demo</c>'s per-path lookup) leaves this empty and instead
    /// calls <see cref="TableBindParams.Secrets"/>'s <see cref="Internal.SecretsAccessor.Get"/> from
    /// <see cref="Bind"/> — that triggers the C++ extension's two-phase bind retry automatically.</summary>
    IReadOnlyList<RequiredSecret> RequiredSecrets => [];

    /// <summary>Advertises whether this function honors <see cref="TableInitParams.ProjectionIds"/>
    /// (only emitting the requested output columns). <see langword="null"/> (the default) tells
    /// DuckDB not to bother pushing projection down — it will still receive/keep only the columns
    /// it asked for from whatever full-width batches this function emits, just less efficiently.</summary>
    bool? ProjectionPushdown => null;

    /// <summary>Advertises whether this function inspects <see cref="TableInitParams.PushdownFilters"/>
    /// and pre-filters its own output. <see langword="null"/>/<see langword="false"/> (the default)
    /// means DuckDB applies every filter itself after receiving the function's full output.</summary>
    bool? FilterPushdown => null;

    bool? SamplingPushdown => null;

    bool? LateMaterialization => null;

    IReadOnlyList<string> SupportedExpressionFilters => [];

    VgiOrderPreservation? OrderPreservation => null;

    int? MaxWorkers => null;

    bool SupportsBatchIndex => false;

    bool SupportsSplits => false;

    /// <summary>When <see cref="FilterPushdown"/> is true, whether the pushed-down filters this
    /// function applies are EXACT (DuckDB may skip re-checking them) or merely a superset/best-effort
    /// narrowing (DuckDB always re-checks). Defaults to false (safe/conservative).</summary>
    bool FiltersExactlyApplied => false;

    bool SupportsPositions => false;

    long? SplitTokenTtlSeconds => null;

    VgiPartitionKind PartitionKind => VgiPartitionKind.NotPartitioned;

    /// <summary>Required WHERE-filter column paths (dotted for struct subfields, e.g. <c>"s.a"</c>)
    /// — when non-empty, a bind call missing a qualifying filter on every listed path must throw
    /// (propagates as a bind failure DuckDB surfaces as a Binder/Catalog error naming the missing
    /// paths). See <c>required_filters_*.test</c>. Checked by <see cref="Bind"/> implementations
    /// that opt in; the base interface does no enforcement itself.</summary>
    IReadOnlyList<string> RequiredFilterColumns => [];

    /// <summary>Called once per bound call (mirrors the <c>bind</c> RPC) — a no-op by default;
    /// override to validate arguments (including throwing for missing required filters) or capture
    /// per-call state. Exceptions propagate as a bind failure back to the client.</summary>
    void Bind(TableBindParams bindParams)
    {
    }

    /// <summary>Resolves the schema to use for THIS specific call — a pure function of the bind
    /// call's arguments, called independently (and potentially more than once) from both
    /// <c>bind</c> and <c>init</c>. Defaults to the static <see cref="OutputSchema"/>; override for
    /// dynamic-output functions (e.g. <c>constant_columns</c>, whose columns mirror its varargs).</summary>
    Schema ResolveOutputSchema(TableBindParams bindParams) => OutputSchema;

    /// <summary>Creates the per-call producer that emits this call's output batches — mirrors the
    /// <c>init</c> RPC opening a producer stream. Exceptions propagate as an init failure.</summary>
    ITableFunctionProducer CreateProducer(TableInitParams initParams);

    /// <summary>Cardinality estimate for this call's result, or <see langword="null"/> when
    /// unknown (the default). Forwarded to DuckDB's optimizer via the
    /// <c>table_function_cardinality</c> RPC — a best-effort, non-critical call the client makes
    /// at most once per bound call site. Reporting an accurate estimate can matter for join
    /// ordering: a scan that reports "unknown" may be planned as the hash-join BUILD side instead
    /// of the probe side, which means no dynamic filter is ever pushed into it (see
    /// <c>splits/dynamic_filters.test</c>'s join-key-pushdown coverage).</summary>
    long? Cardinality(TableBindParams bindParams) => null;

    /// <summary>Per-output-column statistics for this call's result, or <see langword="null"/> when
    /// unknown (the default) — forwarded to DuckDB's optimizer via the <c>table_function_statistics</c>
    /// RPC (a best-effort, non-critical call, same caching/failure semantics as
    /// <see cref="Cardinality"/>). Only consulted for a column the scanned catalog table (if any)
    /// didn't already answer via its own <see cref="Catalog.CatalogTable.Statistics"/>. Keyed by
    /// OUTPUT column name.</summary>
    IReadOnlyDictionary<string, Catalog.ColumnStatisticsInput>? Statistics(TableBindParams bindParams) => null;

    /// <summary>Per-parallel-scan-thread diagnostics surfaced as <c>Extra Info</c> under
    /// <c>EXPLAIN ANALYZE</c> (DuckDB calls this once per thread inside
    /// <c>OperatorProfiler::FinishSource</c>) — empty (the default) means no user-defined keys;
    /// intrinsic keys (worker/function/batch-shape) are added by the C++ extension itself and need
    /// no worker cooperation. <paramref name="executionId"/> is the scan's
    /// <c>InitRequest.ExecutionId</c> — the correlation key for whatever per-batch diagnostics a
    /// producer chose to persist (e.g. via <see cref="Internal.FunctionStorage"/>) while running.
    /// Exceptions here are swallowed by the C++ client (never aborts <c>EXPLAIN ANALYZE</c>), so
    /// this need not be defensive about its own failures.</summary>
    IReadOnlyDictionary<string, string> DynamicToString(TableBindParams bindParams, byte[] executionId) =>
        new Dictionary<string, string>();

    /// <summary>
    /// Divides this scan into named, independently redeemable splits — see <see cref="PlanRequest"/>/
    /// <see cref="PlanResult"/>. Only ever invoked when <see cref="SupportsSplits"/> is
    /// <see langword="true"/> (that flag alone is what routes the C++ client onto the split
    /// plan/claim path at all — see <c>VgiTableFunctionInitGlobal</c>'s <c>if (bind_data.supports_splits)</c>
    /// gate); the default here returns an empty plan and is never reached by a function that
    /// leaves <see cref="SupportsSplits"/> at its own default <see langword="false"/>.
    ///
    /// A split NAMES work rather than describing it — "these three files at version 47" survives
    /// a retry; "rows 0-999 of whatever this returns now" does not, and a distributed engine WILL
    /// retry. The same split may be redeemed more than once (recursive CTEs, task retry) and may
    /// be abandoned mid-stream (LIMIT, an empty join build side); neither is an error.
    ///
    /// Any state carried from planning to reading must live in cross-process storage keyed by
    /// <see cref="TableInitParams.ExecutionId"/> (see <c>Internal.CrossProcessWorkQueue</c>/
    /// <c>Internal.FunctionStorage</c>) — the process that plans is, in general, not the process
    /// that later redeems a given split.
    /// </summary>
    PlanResult Plan(TableBindParams bindParams, PlanRequest request) => PlanResult.Empty;
}
