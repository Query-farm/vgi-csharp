using Apache.Arrow;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Unified secrets access for a table/table-in-out function's <c>Bind</c> — pre-resolved secrets
/// (from a static <c>RequiredSecrets</c> declaration) are readable immediately via
/// <see cref="Resolved"/>; a DYNAMIC lookup (a scope computed from the call's own arguments — e.g.
/// <c>scoped_secret_demo</c>'s per-path scope) goes through <see cref="Get"/>, which on the FIRST bind
/// attempt registers a pending lookup and returns <see langword="null"/> — the caller (<see
/// cref="VgiServiceImpl"/>) notices <see cref="NeedsResolution"/> after <c>Bind</c> returns and, instead
/// of a normal <c>BindResponse</c>, sends back a secret-scope request; the C++ extension resolves it and
/// resends the bind with <c>resolved_secrets_provided=true</c>, at which point THIS SAME accessor (a
/// fresh instance, <paramref name="isRetry"/>=true) has the answer in <see cref="Resolved"/>.
///
/// Mirrors vgi-python's <c>SecretsAccessor</c>/<c>bind()</c>'s "auto-retry on pending lookups" framework
/// hook, simplified: unlike Python, a "simple" (no scope/no name) <see cref="Get"/> call is NOT
/// special-cased to check <see cref="Resolved"/> first on the FIRST attempt — a truly dynamic
/// (non-statically-declared) lookup can never already be resolved on attempt #1 (the outer wire column
/// is keyed by the secret's DB NAME, not its TYPE, so a same-attempt match would require the caller to
/// already know the secret's name — at which point it should pass <c>name:</c> instead), so this always
/// registers pending + returns <see langword="null"/> on a non-retry call, matching what every VGI
/// worker's fixtures actually observe in practice.
/// </summary>
public sealed class SecretsAccessor
{
    private readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> _resolved;
    private readonly bool _isRetry;
    private readonly List<RequiredSecret> _pending = [];

    public SecretsAccessor(byte[]? secretsBytes, bool isRetry)
    {
        _resolved = SecretArgCodec.Decode(secretsBytes);
        _isRetry = isRetry;
    }

    /// <summary>Every secret already resolved on this bind attempt — populated from the start for a
    /// statically-declared (<c>RequiredSecrets</c>) secret, and from the SECOND attempt onward for one
    /// requested dynamically via <see cref="Get"/>.</summary>
    public IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> Resolved => _resolved;

    /// <summary><see langword="true"/> once at least one <see cref="Get"/> call has registered a
    /// pending lookup on this (non-retry) bind attempt — the caller must abandon whatever
    /// <c>BindResponse</c> it was building and send <see cref="PendingLookups"/> as a secret-scope
    /// request instead.</summary>
    public bool NeedsResolution => _pending.Count > 0;

    public IReadOnlyList<RequiredSecret> PendingLookups => _pending;

    /// <summary>Requests a secret by type, optionally narrowed by a dynamic <paramref name="scope"/>
    /// (a path DuckDB's SecretManager longest-prefix-matches against every registered secret of this
    /// type) and/or an exact <paramref name="name"/>. On the FIRST bind attempt this always registers
    /// a pending lookup and returns <see langword="null"/> (the framework retries); on a RETRY it
    /// looks the resolved answer up directly — by <paramref name="name"/> if given, else by
    /// <paramref name="scope"/> (longest-prefix match), else the first resolved secret of this type.</summary>
    public IReadOnlyDictionary<string, IArrowArray>? Get(string secretType, string? scope = null, string? name = null)
    {
        if (!_isRetry)
        {
            _pending.Add(new RequiredSecret { SecretType = secretType, Scope = scope, SecretName = name });
            return null;
        }

        if (name is not null)
        {
            return _resolved.GetValueOrDefault(name);
        }

        return scope is not null
            ? SecretArgCodec.ForScopeOfType(_resolved, scope, secretType)
            : SecretArgCodec.FindByType(_resolved, secretType);
    }
}

/// <summary>Thrown by a table/table-in-out function's <c>Bind</c> dispatch when its
/// <see cref="SecretsAccessor"/> ends the call with pending lookups — caught by
/// <see cref="VgiServiceImpl.BindAsync"/>, which sends a secret-scope-request <see cref="BindResponse"/>
/// (<see cref="BindResponse.LookupSecretTypes"/>/<see cref="BindResponse.LookupScopes"/>/
/// <see cref="BindResponse.LookupNames"/>) instead of a normal one, triggering the C++ extension's
/// two-phase bind retry.</summary>
internal sealed class SecretScopeRequestException(IReadOnlyList<RequiredSecret> lookups) : Exception
{
    public IReadOnlyList<RequiredSecret> Lookups { get; } = lookups;
}
