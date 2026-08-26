namespace QueryFarm.Vgi.Attributes;

/// <summary>
/// Marks a <c>Compute</c> parameter as bound to a resolved DuckDB secret — named after vgi-python's
/// <c>Secret</c> marker. Invisible in the function's SQL signature (not counted in
/// <see cref="IScalarFunction.ArgumentsSchema"/>/<c>duckdb_functions()</c>); the C++ extension
/// pre-resolves the secret (by <see cref="SecretType"/>, optionally narrowed by <see cref="Name"/>/
/// <see cref="Scope"/>) BEFORE the very first bind call and ships it in <c>BindRequest.Secrets</c> —
/// the function must additionally declare this requirement in <c>FunctionInfo.RequiredSecrets</c> for
/// the extension to bother resolving it at all (see <see cref="Scalar.ScalarFn"/>'s <c>ComputePlan</c>,
/// which derives that list automatically from every <see cref="SecretAttribute"/> it finds).
///
/// The bound parameter type is <c>IReadOnlyDictionary&lt;string, Apache.Arrow.IArrowArray&gt;?</c> —
/// the resolved secret's field name → single-element value column map (<see langword="null"/> when no
/// matching secret was resolved). Since scalar functions only support STATIC secret declarations (no
/// dynamic-scope two-phase retry — see <see cref="Internal.SecretsAccessor"/>'s doc comment for why
/// that's a table/table-in-out-only mechanism), the resolved value is available on the very first
/// bind/compute call whenever a matching secret exists at all.
/// </summary>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class SecretAttribute : Attribute
{
    /// <summary>The DuckDB secret TYPE this parameter needs resolved (e.g. <c>"vgi_example"</c>) —
    /// required, C++ enforces type matching.</summary>
    public required string SecretType { get; init; }

    /// <summary>Optional exact secret name for name-based resolution — <see langword="null"/> (the
    /// default) resolves by type (optionally narrowed by <see cref="Scope"/>) instead.</summary>
    public string? Name { get; init; }

    /// <summary>Optional static scope (resolved once, the same way for every call — NOT a per-call
    /// dynamic value) for scope-based pre-resolution.</summary>
    public string? Scope { get; init; }
}
