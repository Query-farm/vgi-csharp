namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The result of a <see cref="Worker.OnAttach"/> handler — lets a worker author validate a
/// <c>catalog_attach</c> request (throw to reject the ATTACH; the exception's <c>Message</c>
/// propagates verbatim as the client-visible error, same generic path every other RPC error
/// already uses) and influence the result beyond what <c>VgiServiceImpl.CatalogAttachAsync</c>
/// builds by default. Returning <see langword="null"/> (or not registering a handler at all)
/// keeps today's behavior unchanged.
/// </summary>
public sealed class AttachContext
{
    /// <summary>Overrides the <c>CatalogRegistry</c> routing identity used by every subsequent RPC
    /// for this attach (defaults to <see cref="CatalogAttachRequest.Name"/> when
    /// <see langword="null"/>) — lets one catalog NAME fan out into several registry buckets keyed
    /// by something resolved at attach time (e.g. a resolved data version), the same identity
    /// mechanism <c>same_name_catalogs.test</c>'s twin_a/twin_b already prove safe, just chosen
    /// dynamically per-attach instead of statically per <c>Register*</c> call.</summary>
    public string? Identity { get; init; }

    /// <summary>Extra bytes appended to the attach envelope after the identity/GUID prefix,
    /// opaque to the framework — read back by any function via the <c>AttachOpaqueData</c> it
    /// already receives on every call (split off everything after the first NUL byte and the
    /// following 16-byte GUID). Fixture-defined format; the framework never inspects it.</summary>
    public byte[]? ExtraOpaqueData { get; init; }

    /// <summary>Fed onto <see cref="CatalogAttachResult.ResolvedDataVersion"/> — the concrete
    /// version this attach resolved to, distinct from the caller's requested
    /// <see cref="CatalogAttachRequest.DataVersionSpec"/> range/spec string.</summary>
    public string? ResolvedDataVersion { get; init; }

    /// <summary>Fed onto <see cref="CatalogAttachResult.ResolvedImplementationVersion"/> — see
    /// <see cref="ResolvedDataVersion"/>.</summary>
    public string? ResolvedImplementationVersion { get; init; }
}
