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
/// Mirrors vgi-python's <c>SecretsAccessor.get</c>: a lookup by <c>name</c> or by type alone that is
/// already satisfied by what was delivered (a statically-declared requirement) is answered at once,
/// matching the secret's own <c>type</c> field — never the outer column name, which is the secret's
/// DuckDB NAME (<c>__default_&lt;type&gt;</c> for an unnamed secret). A <c>scope</c> lookup always
/// goes to the client on the first attempt, because only the client knows whether a secret with a
/// longer matching scope exists.
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
    /// type) and/or an exact <paramref name="name"/> (which takes precedence over the scope, as on the
    /// client). Selection follows <see cref="SecretArgCodec.Lookup"/>. On the FIRST bind attempt a
    /// lookup the delivered secrets cannot answer yet (always the case for a scoped one) registers a
    /// pending lookup and returns <see langword="null"/> (the framework retries); on a RETRY the
    /// answer, or <see langword="null"/> when the secret genuinely does not exist.</summary>
    public IReadOnlyDictionary<string, IArrowArray>? Get(string secretType, string? scope = null, string? name = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(secretType);
        if (_isRetry || string.IsNullOrEmpty(scope) || !string.IsNullOrEmpty(name))
        {
            var found = SecretArgCodec.Lookup(_resolved, secretType, name, scope);
            if (found is not null || _isRetry)
            {
                return found;
            }
        }

        _pending.Add(new RequiredSecret { SecretType = secretType, Scope = scope, SecretName = name });
        return null;
    }

    /// <summary>Like <see cref="Get"/>, but a secret that is genuinely absent (on a retry, or in a
    /// read-only view) throws <see cref="VgiInvalidArgumentException"/> naming the type (and name or
    /// scope, if given) and no secret value. On the first bind attempt of a lookup that is not yet
    /// answerable this registers the lookup and returns <see langword="null"/> exactly like
    /// <see cref="Get"/>, so the framework can ask the client before deciding the secret is missing.</summary>
    public IReadOnlyDictionary<string, IArrowArray>? Require(string secretType, string? scope = null, string? name = null)
    {
        var found = Get(secretType, scope, name);
        if (found is not null || !_isRetry)
        {
            return found;
        }

        var detail = !string.IsNullOrEmpty(name) ? $" named '{name}'" : !string.IsNullOrEmpty(scope) ? $" for scope '{scope}'" : "";
        throw new VgiInvalidArgumentException(
            $"Required secret of type '{secretType}'{detail} was not found. Create one with CREATE SECRET (TYPE {secretType}, ...).");
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
