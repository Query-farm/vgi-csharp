using System.Text.Json;
using Apache.Arrow;
using QueryFarm.Vgi.Catalog.Snapshots;
using QueryFarm.VgiRpc.External;
using Xunit;

namespace QueryFarm.Vgi.Tests;

public sealed class CatalogSnapshotS3Tests
{
    private sealed class Store : ICatalogS3Client
    {
        public readonly Dictionary<string, (byte[] Body, int Version)> Objects = new(StringComparer.Ordinal);
        public Task<CatalogS3Object?> GetObjectAsync(string bucket, string key, long maxBytes, CancellationToken cancellationToken = default)
        {
            if (!Objects.TryGetValue(key, out var value)) return Task.FromResult<CatalogS3Object?>(null);
            if (value.Body.LongLength > maxBytes) throw new InvalidOperationException("Read limit exceeded.");
            return Task.FromResult<CatalogS3Object?>(new(value.Body.ToArray(), value.Version.ToString()));
        }
        public Task<bool> PutObjectAsync(string bucket, string key, byte[] bytes, CatalogS3PutOptions options, CancellationToken cancellationToken = default)
        {
            var present = Objects.TryGetValue(key, out var old);
            if (options.IfAbsent && present || options.IfMatch is not null && (!present || options.IfMatch != old.Version.ToString()))
                return Task.FromResult(false);
            Objects[key] = (bytes.ToArray(), old.Version + 1);
            return Task.FromResult(true);
        }
        public Task<string> PresignGetAsync(string bucket, string key, int expiresInSeconds, CancellationToken cancellationToken = default) =>
            Task.FromResult("https://objects.example/" + key);
    }

    [Fact]
    public async Task RepositoryRecoversConditionallyRenewsAndTombstones()
    {
        var store = new Store();
        var first = new S3CatalogPublicationRepository(store, "bucket", "catalog", "test");
        var bytes = "payload"u8.ToArray();
        var url = await first.UploadAsync(bytes, new Schema([], []), null, CancellationToken.None);
        var reference = new ExternalRef(url, new string('a', 64));
        var location = await first.LocationAsync(reference);
        var manifest = new CatalogPublicationManifest("key", "first", "vgi.v3", "3.0", "contract", "catalog-collections-1",
            1, null, reference.Sha256!, bytes.Length, bytes.Length, url, location.ObjectId, location.ExpiresAt, location.RetainUntil, false);
        Assert.Equal(manifest, await first.CommitIfAbsentAsync(manifest));
        var second = new S3CatalogPublicationRepository(store, "bucket", "catalog", "test");
        Assert.Equal(manifest, await second.LookupAsync("key"));
        Assert.Equal("first", (await second.CommitIfAbsentAsync(manifest with { ManifestId = "loser" })).ManifestId);
        var renewed = await second.RenewAsync(manifest);
        Assert.NotNull(renewed);
        Assert.Equal(manifest with { ExpiresAt = renewed.ExpiresAt }, renewed);
        Assert.False(await second.InvalidateAsync("key", "loser"));
        Assert.True(await second.InvalidateAsync("key", "first"));
        Assert.Null(await second.LookupAsync("key"));
        Assert.Equal(manifest, await second.CommitIfAbsentAsync(manifest));
        using var json = JsonDocument.Parse(store.Objects["catalog/manifests/key.json"].Body);
        Assert.Equal("first", json.RootElement.GetProperty("manifest_id").GetString());
        Assert.False(json.RootElement.TryGetProperty("reference", out _));
        Assert.True(store.Objects.ContainsKey(location.ObjectId!));
    }

    [Fact]
    public void RepositoryRejectsUnknownOrShortRetention()
    {
        Assert.Throws<ArgumentException>(() => new S3CatalogPublicationRepository(new Store(), "bucket", "catalog", "test", 60, 30));
    }
}
