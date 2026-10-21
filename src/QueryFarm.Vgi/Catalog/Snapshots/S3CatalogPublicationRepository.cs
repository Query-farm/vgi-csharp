using System.Collections.Concurrent;
using System.Text.Json;
using Apache.Arrow;
using QueryFarm.VgiRpc.External;

namespace QueryFarm.Vgi.Catalog.Snapshots;

/// <summary>An object body and its opaque S3 conditional-write ETag.</summary>
public sealed record CatalogS3Object(byte[] Bytes, string Etag);

/// <summary>Translate IfAbsent / IfMatch directly to conditional S3 PutObject headers.</summary>
public sealed record CatalogS3PutOptions(string ContentType, string? ContentEncoding = null,
    bool IfAbsent = false, string? IfMatch = null);

/// <summary>
/// Minimal bridge to an application's S3/R2 SDK. Credentials and signing remain
/// with that SDK. Reads must enforce maxBytes before buffering the entire body.
/// Only a missing key returns null; only a conditional conflict returns false.
/// Authentication, transport and service failures must propagate as exceptions.
/// </summary>
public interface ICatalogS3Client
{
    Task<CatalogS3Object?> GetObjectAsync(string bucket, string key, long maxBytes,
        CancellationToken cancellationToken = default);
    Task<bool> PutObjectAsync(string bucket, string key, byte[] bytes, CatalogS3PutOptions options,
        CancellationToken cancellationToken = default);
    Task<string> PresignGetAsync(string bucket, string key, int expiresInSeconds,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Durable manifests and immutable RPC objects in S3-compatible storage. Configure
/// object lifecycle to retain objects for at least retentionSeconds; manifests
/// occupy a separate prefix. Conditional invalidation tombstones discovery and
/// never deletes an object already referenced by a caller.
/// </summary>
public sealed class S3CatalogPublicationRepository : ICatalogPublicationRepository, IExternalStorage
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower, IgnoreReadOnlyProperties = true };
    private readonly ICatalogS3Client _client;
    private readonly string _bucket, _prefix;
    private readonly int _urlTtlSeconds, _retentionSeconds;
    private readonly ConcurrentDictionary<string, PublicationLocation> _locations = new(StringComparer.Ordinal);
    public string Profile { get; }
    public IExternalStorage Storage => this;

    public S3CatalogPublicationRepository(ICatalogS3Client client, string bucket, string prefix, string profile,
        int urlTtlSeconds = 3600, int retentionSeconds = 86400)
    {
        ArgumentNullException.ThrowIfNull(client);
        if (string.IsNullOrEmpty(bucket) || string.IsNullOrEmpty(profile) || urlTtlSeconds <= 0 || retentionSeconds <= urlTtlSeconds)
            throw new ArgumentException("S3 publication needs bucket/profile and retention exceeding URL lifetime.");
        _client = client;
        _bucket = bucket;
        _prefix = prefix.TrimEnd('/') + "/";
        Profile = profile;
        _urlTtlSeconds = urlTtlSeconds;
        _retentionSeconds = retentionSeconds;
    }

    private string ManifestKey(string key) => _prefix + "manifests/" + key + ".json";
    private static double Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;

    private async Task<(CatalogPublicationManifest? Manifest, string? Etag)> LoadAsync(string key)
    {
        const int limit = 1024 * 1024;
        var value = await _client.GetObjectAsync(_bucket, ManifestKey(key), limit).ConfigureAwait(false);
        if (value is null) return (null, null);
        if (value.Bytes.Length > limit) throw new InvalidOperationException("Catalog manifest exceeds size limit.");
        using var document = JsonDocument.Parse(value.Bytes);
        if (document.RootElement.TryGetProperty("tombstone", out var deleted) && deleted.GetBoolean())
            return (null, value.Etag);
        var manifest = JsonSerializer.Deserialize<CatalogPublicationManifest>(value.Bytes, Json)
            ?? throw new InvalidOperationException("Catalog manifest cannot be null.");
        return (manifest, value.Etag);
    }

    private Task<bool> PutManifestAsync(CatalogPublicationManifest manifest, string? etag) =>
        _client.PutObjectAsync(_bucket, ManifestKey(manifest.CacheKey), JsonSerializer.SerializeToUtf8Bytes(manifest, Json),
            new("application/json", IfAbsent: etag is null, IfMatch: etag));

    public async Task<CatalogPublicationManifest?> LookupAsync(string key) =>
        (await LoadAsync(key).ConfigureAwait(false)).Manifest;

    public async Task<CatalogPublicationManifest> CommitIfAbsentAsync(CatalogPublicationManifest manifest)
    {
        for (var attempt = 0; attempt < 8; attempt++)
        {
            var (current, etag) = await LoadAsync(manifest.CacheKey).ConfigureAwait(false);
            if (current is not null) return current;
            if (await PutManifestAsync(manifest, etag).ConfigureAwait(false)) return manifest;
        }
        throw new InvalidOperationException("Catalog manifest conditional-write contention.");
    }

    public async Task<bool> InvalidateAsync(string key, string manifestId)
    {
        var (current, etag) = await LoadAsync(key).ConfigureAwait(false);
        if (current is null || current.ManifestId != manifestId) return false;
        return await _client.PutObjectAsync(_bucket, ManifestKey(key), "{\"tombstone\":true}"u8.ToArray(),
            new("application/json", IfMatch: etag)).ConfigureAwait(false);
    }

    public async Task<string> UploadAsync(byte[] data, Schema schema, string? contentEncoding, CancellationToken cancellationToken)
    {
        var objectId = _prefix + "objects/" + Guid.NewGuid().ToString("N") + ".arrow";
        if (!await _client.PutObjectAsync(_bucket, objectId, data,
            new("application/vnd.apache.arrow.stream", contentEncoding, IfAbsent: true), cancellationToken).ConfigureAwait(false))
            throw new InvalidOperationException("Immutable catalog object name collision.");
        var url = await _client.PresignGetAsync(_bucket, objectId, _urlTtlSeconds, cancellationToken).ConfigureAwait(false);
        var now = Now;
        _locations[url] = new(objectId, now + _urlTtlSeconds, now + _retentionSeconds);
        return url;
    }

    public Task<PublicationLocation> LocationAsync(ExternalRef reference) =>
        Task.FromResult(_locations.TryRemove(reference.Url, out var location)
            ? location : throw new InvalidOperationException("Reference was not uploaded by this repository."));

    public async Task<CatalogPublicationManifest?> RenewAsync(CatalogPublicationManifest manifest)
    {
        for (var attempt = 0; attempt < 8; attempt++)
        {
            var (current, etag) = await LoadAsync(manifest.CacheKey).ConfigureAwait(false);
            if (current is null) return null;
            if (current.ManifestId != manifest.ManifestId) return current;
            if (current.ObjectId is null || !current.RetainUntil.HasValue) return null;
            var remaining = current.RetainUntil.Value - Now;
            if (!double.IsFinite(remaining) || remaining <= 60) return null;
            var lifetime = (int)Math.Min(_urlTtlSeconds, remaining);
            var url = await _client.PresignGetAsync(_bucket, current.ObjectId, lifetime).ConfigureAwait(false);
            var renewed = current with { Url = url, ExpiresAt = Now + lifetime };
            if (await PutManifestAsync(renewed, etag).ConfigureAwait(false)) return renewed;
        }
        throw new InvalidOperationException("Catalog reference conditional-renewal contention.");
    }
}
