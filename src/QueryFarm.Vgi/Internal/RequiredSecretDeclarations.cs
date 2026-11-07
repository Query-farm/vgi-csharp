using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Checks a function's static <c>RequiredSecrets</c> before it goes into <see cref="FunctionInfo"/>.
/// The client resolves exactly the requirements it is sent, so a declaration that is malformed must
/// fail loudly at discovery — never be skipped, which would leave the function running without the
/// credentials it asked for (the fleet bug vgi <c>docs/secret_lookup_fixtures.md</c> pins down).
/// </summary>
internal static class RequiredSecretDeclarations
{
    /// <summary>Returns the requirements as a list, in order, or throws
    /// <see cref="InvalidOperationException"/> naming the function and entry when the list is
    /// <see langword="null"/>, an entry is <see langword="null"/>, or an entry's
    /// <see cref="RequiredSecret.SecretType"/> is empty. An empty <c>Scope</c>/<c>SecretName</c> is
    /// normalised to <see langword="null"/> (no scope / no name), which is what the client treats it
    /// as.</summary>
    public static List<RequiredSecret> Validate(string functionName, IReadOnlyList<RequiredSecret>? declared)
    {
        if (declared is null)
        {
            throw new InvalidOperationException(
                $"Function '{functionName}': RequiredSecrets is null; return an empty list when the function needs no secrets.");
        }

        var result = new List<RequiredSecret>(declared.Count);
        for (var i = 0; i < declared.Count; i++)
        {
            var entry = declared[i] ?? throw new InvalidOperationException(
                $"Function '{functionName}': RequiredSecrets[{i}] is null.");
            if (string.IsNullOrWhiteSpace(entry.SecretType))
            {
                throw new InvalidOperationException(
                    $"Function '{functionName}': RequiredSecrets[{i}] has no SecretType; every required secret must name its DuckDB secret type (e.g. new RequiredSecret {{ SecretType = \"s3\" }}).");
            }

            result.Add(new RequiredSecret
            {
                SecretType = entry.SecretType,
                Scope = string.IsNullOrEmpty(entry.Scope) ? null : entry.Scope,
                SecretName = string.IsNullOrEmpty(entry.SecretName) ? null : entry.SecretName,
            });
        }

        return result;
    }
}
