using Apache.Arrow;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Scalar;

/// <summary>
/// The raw contract a scalar function implements. A hardcoded implementation (see
/// <c>examples/01-minimal-scalar-worker</c>'s <c>UpperCaseFunction</c>, or the ANY-typed dynamic-
/// output fixtures under <c>fixtures/QueryFarm.Vgi.ExampleWorker/Scalar</c>) is a fully supported
/// way to implement this interface directly; the attribute-driven convenience base class
/// <see cref="ScalarFn"/> (<c>[Param]</c>/<c>[ConstParam]</c>/<c>[Setting]</c>/<c>[OutputLength]</c>
/// reflection dispatch, ported from vgi-java's <c>ScalarFn.ComputePlan</c>/vgi-python's
/// <c>ScalarFunction</c>) builds on top of this same contract without changing it.
/// </summary>
public interface IScalarFunction
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

    /// <summary>Describes the function's positional arguments. Field NAMES are cosmetic — DuckDB
    /// only inspects field TYPES/nullability/count/metadata (<c>vgi_const</c>/<c>vgi_varargs</c>/
    /// <c>vgi_type=any</c>) when registering the function's signature.</summary>
    Schema ArgumentsSchema { get; }

    /// <summary>The function's STATIC/declared return schema (exactly one field) — used for
    /// catalog registration (<c>FunctionInfo.OutputSchema</c>, <c>duckdb_functions()</c>). For a
    /// dynamic-output ("ANY"-typed) function this is a single nullable field whose type carries
    /// <c>vgi_type=any</c> metadata; the REAL per-call type comes from
    /// <see cref="ResolveOutputSchema"/>.</summary>
    Schema OutputSchema { get; }

    /// <summary>Optional stability hint (defaults to CONSISTENT server-side when omitted) —
    /// override to VOLATILE for a non-deterministic function (disables input dedup/caching on the
    /// C++ side) or CONSISTENT_WITHIN_QUERY.</summary>
    FunctionStability? Stability => null;

    /// <summary>Optional null-handling hint (defaults to DEFAULT, meaning DuckDB may short-circuit
    /// a scalar NULL argument to a NULL result without ever calling the worker). Override to
    /// SPECIAL for a function that wants to see/handle NULL rows itself.</summary>
    FunctionNullHandling? NullHandling => null;

    /// <summary>Setting names (<c>SET &lt;key&gt; = ...</c>) this function needs resolved and
    /// shipped on every bind — advertised so the C++ extension bothers looking them up at all.</summary>
    IReadOnlyList<string> RequiredSettings => [];

    /// <summary>Secrets this function needs statically resolved and shipped on every bind — see
    /// <see cref="Attributes.SecretAttribute"/>. Advertised so the C++ extension bothers resolving
    /// them at all; scalar functions only support static (by-type/name/scope) resolution, not the
    /// table/table-in-out-only dynamic two-phase retry (<see cref="Internal.SecretsAccessor"/>).</summary>
    IReadOnlyList<RequiredSecret> RequiredSecrets => [];

    /// <summary>Cache-control <c>vgi.cache.*</c> custom_metadata (see
    /// <c>~/Development/vgi/src/include/vgi_cache_control.hpp</c> for the full key catalogue —
    /// e.g. <c>vgi.cache.per_value = "1"</c> opts a deterministic function into the C++ extension's
    /// per-distinct-value result memoization tier) to attach to every exchange-turn result batch
    /// this function emits. Static per function identity, not call-specific — the C++ side latches
    /// the advertisement once seen and applies it for the life of the attachment, so re-sending it
    /// on every call is harmless but not required. <see langword="null"/> (the default) attaches no
    /// cache-control metadata.</summary>
    IReadOnlyDictionary<string, string>? CacheControlMetadata => null;

    /// <summary>Called once per bound call (mirrors the <c>bind</c> RPC) — a no-op by default;
    /// override to validate arguments or capture per-call state. Exceptions propagate as a bind
    /// failure back to the client.</summary>
    void Bind(ScalarBindParams bindParams)
    {
    }

    /// <summary>Resolves the schema to use for THIS specific call, given the concrete input schema
    /// DuckDB sent (<c>null</c> when no input schema was supplied — e.g. a zero-arg function, or a
    /// pre-bind catalog probe). A pure function of <paramref name="inputSchema"/> — called
    /// independently (and potentially more than once) from both <c>bind</c> and <c>init</c>, so it
    /// must not depend on any state mutated by <see cref="Bind"/>. Defaults to the static
    /// <see cref="OutputSchema"/>; override for ANY-typed dynamic-output functions
    /// (<see cref="Types.TypeRules"/>-driven promotion).</summary>
    Schema ResolveOutputSchema(Schema? inputSchema) => OutputSchema;

    /// <summary>Computes one exchange turn: <paramref name="processParams"/>.Input has one row per
    /// call and one column per positional argument; the returned batch must have exactly one
    /// column (matching <paramref name="processParams"/>.OutputSchema's type) and the same row
    /// count.</summary>
    RecordBatch Process(ScalarProcessParams processParams);
}
