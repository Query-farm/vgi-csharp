using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Catalog;

/// <summary>What a catalog answers <c>catalog_contents</c> with: a snapshot of every schema and its
/// contents, or "not modified" — the C# shape of vgi-python's <c>CatalogContentsResult</c>.</summary>
/// <remarks>
/// <para>The worker turns it into the wire <see cref="CatalogContentsResponse"/> and enforces the
/// revalidation rules (see <see cref="Internal.CatalogContentsResponder"/>): <see cref="NotModified"/>
/// is only accepted with an <see cref="Etag"/> equal to the request's <c>if_none_match</c> and no
/// <see cref="Schemas"/>; a full answer whose etag equals <c>if_none_match</c> is sent as
/// not-modified; a result with no etag never is. Schema paths must be unique, every parent must be
/// present, and the worker sends them parents first.</para>
/// </remarks>
public sealed record CatalogContentsResult
{
    /// <summary>One <see cref="SchemaContents"/> per schema, each item byte-identical to the
    /// matching per-schema RPC's. Must be empty when <see cref="NotModified"/> is set.</summary>
    public IReadOnlyList<SchemaContents> Schemas { get; init; } = [];

    /// <summary>Opaque validator for this snapshot (a generation counter, schema version, git sha,
    /// ...), sent back by the client as <c>if_none_match</c>. <see langword="null"/> means the catalog
    /// does not revalidate — unless it opted in to the framework's content hash
    /// (<see cref="CatalogContentsEtagMode.ContentHash"/>).</summary>
    public string? Etag { get; init; }

    /// <summary><see langword="true"/> when the request's <c>if_none_match</c> equals the current
    /// <see cref="Etag"/>, so the catalog skipped building the snapshot.</summary>
    public bool NotModified { get; init; }

    /// <summary>The "not modified" answer for a client already holding <paramref name="etag"/>.</summary>
    public static CatalogContentsResult Unchanged(string etag) => new() { Etag = etag, NotModified = true };
}

/// <summary>One <c>catalog_contents</c> call, as a catalog's handler sees it
/// (<see cref="Worker.OnCatalogContents(string, Func{CatalogContentsRequest, Task{CatalogContentsResult}})"/>).</summary>
public sealed class CatalogContentsRequest
{
    private readonly Func<Task<CatalogContentsResult>> _build;

    internal CatalogContentsRequest(
        string identity, byte[] attachOpaqueData, string? ifNoneMatch, long catalogVersion, Func<Task<CatalogContentsResult>> build)
    {
        Identity = identity;
        AttachOpaqueData = attachOpaqueData;
        IfNoneMatch = ifNoneMatch;
        CatalogVersion = catalogVersion;
        _build = build;
    }

    /// <summary>The attach's catalog identity (the attached catalog name, unless
    /// <see cref="Protocol.AttachContext.Identity"/> rerouted it).</summary>
    public string Identity { get; }

    /// <summary>The attach's opaque data, as every catalog RPC receives it.</summary>
    public byte[] AttachOpaqueData { get; }

    /// <summary>The etag of the snapshot the client already holds, or <see langword="null"/>. When
    /// it equals the catalog's current etag, answer <see cref="CatalogContentsResult.Unchanged"/>
    /// before building anything.</summary>
    public string? IfNoneMatch { get; }

    /// <summary>The catalog version the answer is for (what <c>catalog_version</c> reports).</summary>
    public long CatalogVersion { get; }

    /// <summary>Builds the default snapshot (every registered schema and its contents, no etag) —
    /// what the worker serves when no handler is registered. Cached per catalog contents, so calling
    /// it on every request is cheap.</summary>
    public Task<CatalogContentsResult> BuildAsync() => _build();
}

/// <summary>How the framework supplies a <c>catalog_contents</c> etag for a catalog that returns
/// none of its own (<see cref="Worker.CatalogContentsEtag"/>).</summary>
public enum CatalogContentsEtagMode
{
    /// <summary>No framework etag (the default): the client polls <c>catalog_version</c>.</summary>
    None,

    /// <summary>The etag is the hex SHA-256 of the snapshot
    /// (<see cref="Internal.CatalogContentsDigest.Compute"/>, the same digest vgi-python's
    /// <c>catalog_contents_digest()</c> computes), and a matching <c>if_none_match</c> is answered
    /// not-modified. The snapshot is still built, so this saves the transfer and the client's decode,
    /// not the build — mirrors vgi-python's <c>catalog_contents_etag = "content-hash"</c>.</summary>
    ContentHash,
}
