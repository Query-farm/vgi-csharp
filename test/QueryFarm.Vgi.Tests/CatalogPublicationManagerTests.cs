using System.Collections.Concurrent;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using QueryFarm.Vgi.Catalog.Snapshots;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Attributes;
using QueryFarm.VgiRpc.Client.Http;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.External;
using Xunit;

namespace QueryFarm.Vgi.Tests;

public sealed class CatalogPublicationManagerTests
{
    [ProtocolName("vgi.v2")]
    public interface ICatalogClient
    {
        Task<CatalogAttachResult> CatalogAttachAsync(CatalogAttachRequest request);
        Task<CatalogContentsResponse> CatalogContentsAsync(byte[] attachOpaqueData, string? ifNoneMatch = null);
    }
    private sealed class ObjectStore : IExternalStorage, IAsyncDisposable
    {
        private readonly ConcurrentDictionary<string, (byte[] Bytes, string? Encoding)> _objects = new();
        private WebApplication? _app;
        private string _origin = "";
        public int Uploads;
        public int Gets;
        public bool Corrupt;

        public static async Task<ObjectStore> StartAsync()
        {
            var store = new ObjectStore();
            var builder = WebApplication.CreateSlimBuilder(Worker.WorkerHostOptions());
            builder.Logging.ClearProviders();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            store._app = builder.Build();
            store._app.MapGet("/{key}", async (HttpContext context, string key) =>
            {
                Interlocked.Increment(ref store.Gets);
                if (!store._objects.TryGetValue(key, out var item)) { context.Response.StatusCode = 404; return; }
                if (item.Encoding is not null) context.Response.Headers.ContentEncoding = item.Encoding;
                var bytes = store.Corrupt ? "invalid Arrow object"u8.ToArray() : item.Bytes;
                context.Response.ContentLength = bytes.Length;
                await context.Response.Body.WriteAsync(bytes, context.RequestAborted);
            });
            await store._app.StartAsync();
            store._origin = store._app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single();
            return store;
        }
        public Task<string> UploadAsync(byte[] data, Schema schema, string? contentEncoding, CancellationToken cancellationToken)
        {
            var key = Interlocked.Increment(ref Uploads).ToString();
            _objects[key] = (data, contentEncoding);
            return Task.FromResult(_origin + "/" + key);
        }
        public void Append(string url, byte[] extra)
        {
            var key = new Uri(url).AbsolutePath.TrimStart('/');
            var old = _objects[key];
            _objects[key] = ([..old.Bytes, ..extra], old.Encoding);
        }
        public byte[] Bytes(string url) => _objects[new Uri(url).AbsolutePath.TrimStart('/')].Bytes;
        public void Replace(string url, byte[] bytes) => _objects[new Uri(url).AbsolutePath.TrimStart('/')] = (bytes, null);
        public async ValueTask DisposeAsync() { if (_app is not null) await _app.DisposeAsync(); }
    }

    private sealed class Repository(ObjectStore storage, PublicationLocation? location = null) : ICatalogPublicationRepository
    {
        private readonly MemoryCatalogPublicationRepository _inner = new(storage, "test-http", location ?? new(Stable: true));
        public readonly ConcurrentDictionary<string, CatalogPublicationManifest> Committed = new();
        public Func<CatalogPublicationManifest, CatalogPublicationManifest?>? Renewal;
        public Action? AfterCommit;
        public string Profile => _inner.Profile;
        public IExternalStorage Storage => storage;
        public Task<CatalogPublicationManifest?> LookupAsync(string key) => _inner.LookupAsync(key);
        public async Task<CatalogPublicationManifest> CommitIfAbsentAsync(CatalogPublicationManifest manifest)
        {
            var result = await _inner.CommitIfAbsentAsync(manifest);
            Committed[result.CacheKey] = result;
            AfterCommit?.Invoke();
            return result;
        }
        public Task<bool> InvalidateAsync(string key, string manifestId) => _inner.InvalidateAsync(key, manifestId);
        public Task<PublicationLocation> LocationAsync(ExternalRef reference) => _inner.LocationAsync(reference);
        public Task<CatalogPublicationManifest?> RenewAsync(CatalogPublicationManifest manifest) => Task.FromResult(Renewal?.Invoke(manifest));
    }

    private static SnapshotRequest Request(string scope = "attachment-a") => new([], scope, 7, null);
    private static CatalogContentsResponse Contents() => new()
    {
        CatalogVersion = 7, Etag = "",
        Schemas = MetadataCollections.Encode<SchemaInfo>([new() { Path = ["main"] }, new() { Path = ["extra"] }], batchSize: 1),
        Functions = MetadataCollections.Encode(MetadataCollectionTests.BinaryFunctions(), 3),
    };
    private static CatalogSnapshotPolicy Policy(Repository repository, string? encoding = null) => new(repository,
        request => new SnapshotIdentity("test", "revision-1", request.CatalogVersion))
    {
        External = new() { UrlValidator = null },
        Compression = encoding is null ? null : new Compression(encoding),
    };

    [Fact]
    public async Task HttpWorkerPublishesResolvableResultsAndAuthenticatesBeforeCacheHits()
    {
        await using var storage = await ObjectStore.StartAsync();
        var repository = new Repository(storage);
        var identityCalls = 0;
        var policy = new CatalogSnapshotPolicy(repository, r =>
        {
            Interlocked.Increment(ref identityCalls);
            return new("http-worker", "revision-1", r.CatalogVersion);
        }) { External = new() { UrlValidator = null }, Compression = new("gzip") };
        var worker = new Worker().CatalogName("example").DefaultSchema("main").RegisterSchema("main").CatalogContents(true).CatalogSnapshot(policy);
        var builder = WebApplication.CreateSlimBuilder(Worker.WorkerHostOptions());
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        await using var app = builder.Build();
        worker.MapHttp(app);
        await app.StartAsync();
        var address = new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single());
        await using var client = new HttpRpcClient(address, new HttpRpcClientOptions
        {
            Protocol = "vgi.v2", ProtocolVersion = Worker.DefaultProtocolVersion,
            ExternalLocation = new() { UrlValidator = null },
        });
        var rpc = client.CreateProxy<ICatalogClient>();
        var attached = await rpc.CatalogAttachAsync(new() { Name = "example" });
        var response = await rpc.CatalogContentsAsync(attached.AttachOpaqueData);
        Assert.False(response.NotModified);
        Assert.Single(MetadataCollections.DecodeCatalog(response));
        Assert.Equal((1, 2), (storage.Uploads, storage.Gets)); // publication readback + client download
        var unchanged = await rpc.CatalogContentsAsync(attached.AttachOpaqueData, response.Etag);
        Assert.True(unchanged.NotModified);
        Assert.Equal((1, 2), (storage.Uploads, storage.Gets));
        var callsBefore = identityCalls;
        var damaged = attached.AttachOpaqueData.ToArray(); damaged[^1] ^= 1;
        await Assert.ThrowsAsync<RpcException>(() => rpc.CatalogContentsAsync(damaged));
        Assert.Equal(callsBefore, identityCalls);
        Assert.Equal((1, 2), (storage.Uploads, storage.Gets));
        var second = await rpc.CatalogAttachAsync(new() { Name = "example" });
        await rpc.CatalogContentsAsync(second.AttachOpaqueData);
        Assert.Equal(2, storage.Uploads); // default scope uses each sealed attachment
    }

    [Theory]
    [InlineData(null)]
    [InlineData("gzip")]
    [InlineData("zstd")]
    public async Task HttpPublicationIsSingleflightAndRecoversWithoutBuilding(string? encoding)
    {
        await using var storage = await ObjectStore.StartAsync();
        var repository = new Repository(storage);
        var policy = Policy(repository, encoding);
        var builds = 0;
        Task<CatalogContentsResponse> Build() { Interlocked.Increment(ref builds); return Task.FromResult(Contents()); }
        var replies = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => policy.Manager.WarmAsync(policy, Request(), Build)));
        var reference = Assert.IsType<ExternalRef>(replies[0].Reference);
        Assert.All(replies, reply => Assert.Equal(reference.Url, reply.Reference!.Url));
        Assert.Equal((1, 1, 1), (builds, storage.Uploads, storage.Gets));
        var conditional = await policy.Manager.ServeAsync(policy, Request(), "", Build);
        Assert.True(conditional.Response!.NotModified);
        Assert.Null(conditional.Reference);
        Assert.Equal((1, 1, 1), (builds, storage.Uploads, storage.Gets));
        var restarted = new CatalogSnapshotManager();
        Assert.Equal(reference.Url, (await restarted.WarmAsync(policy, Request(), Build)).Reference!.Url);
        Assert.Equal((1, 1, 2), (builds, storage.Uploads, storage.Gets));
        var manifest = repository.Committed.Values.Single();
        Assert.False(await restarted.InvalidateAsync(policy, manifest.CacheKey, "another-generation"));
        await restarted.WarmAsync(policy, Request(), Build);
        Assert.Equal((1, 1, 2), (builds, storage.Uploads, storage.Gets));
        Assert.NotEqual(reference.Url, (await restarted.WarmAsync(policy, Request("attachment-b"), Build)).Reference!.Url);
        Assert.Equal((2, 2, 3), (builds, storage.Uploads, storage.Gets));
    }

    [Fact]
    public async Task UnrelatedAttachmentsPublishWhileAnotherBuildIsBlocked()
    {
        await using var storage = await ObjectStore.StartAsync();
        var policy = Policy(new Repository(storage));
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blocked = policy.Manager.WarmAsync(policy, Request("blocked"), async () =>
        {
            started.SetResult();
            await release.Task;
            return Contents();
        });
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        try
        {
            var other = await policy.Manager.WarmAsync(policy, Request("independent"), () => Task.FromResult(Contents()))
                .WaitAsync(TimeSpan.FromSeconds(10));
            Assert.NotNull(other.Reference);
            Assert.False(blocked.IsCompleted);
        }
        finally { release.SetResult(); await blocked; }
        Assert.Equal((2, 2), (storage.Uploads, storage.Gets));
    }

    [Fact]
    public async Task ValidatorAndCapsRunBeforeObjectUpload()
    {
        await using var storage = await ObjectStore.StartAsync();
        var repository = new Repository(storage);
        var validator = new CatalogSnapshotPolicy(repository, _ => throw new InvalidOperationException("identity should not run")) { Validator = _ => "current" };
        var unchanged = await validator.Manager.ServeAsync(validator, Request(), "current", () => throw new InvalidOperationException("build should not run"));
        Assert.True(unchanged.Response!.NotModified);
        foreach (var rawLimit in new[] { true, false })
        {
            var bounded = new CatalogSnapshotPolicy(repository, r => new("test", "revision-1", r.CatalogVersion))
            {
                External = new() { UrlValidator = null }, Compression = new("gzip"),
                MaxRawBytes = rawLimit ? 1 : 1_000_000, MaxStoredBytes = rawLimit ? 1_000_000 : 1,
            };
            await Assert.ThrowsAsync<InvalidOperationException>(() => bounded.Manager.WarmAsync(bounded, Request(), () => Task.FromResult(Contents())));
        }
        Assert.Equal((0, 0), (storage.Uploads, storage.Gets));
    }

    [Fact]
    public async Task CorruptReadbackNeverCommitsAndFallbackRequiresOptIn()
    {
        await using var storage = await ObjectStore.StartAsync();
        storage.Corrupt = true;
        var repository = new Repository(storage);
        var policy = Policy(repository);
        await Assert.ThrowsAsync<InvalidOperationException>(() => policy.Manager.WarmAsync(policy, Request(), () => Task.FromResult(Contents())));
        Assert.Empty(repository.Committed);
        var fallback = new CatalogSnapshotPolicy(repository, r => new("test", "revision-1", r.CatalogVersion))
        {
            External = new() { UrlValidator = null }, OrdinaryOnFailure = true,
        };
        var reply = await fallback.Manager.WarmAsync(fallback, Request(), () => Task.FromResult(Contents()));
        Assert.Null(reply.Reference);
        Assert.Equal(7, reply.Response!.CatalogVersion);
        Assert.Empty(repository.Committed);
    }

    [Fact]
    public async Task RevisionChangeDuringCommitCannotServeTheOldPublication()
    {
        await using var storage = await ObjectStore.StartAsync();
        var repository = new Repository(storage);
        var revision = "before";
        var policy = new CatalogSnapshotPolicy(repository, r => new("test", revision, r.CatalogVersion))
        { External = new() { UrlValidator = null } };
        repository.AfterCommit = () => { revision = "after"; repository.AfterCommit = null; };
        var builds = 0;
        var result = await policy.Manager.WarmAsync(policy, Request(), () => { builds++; return Task.FromResult(Contents()); });
        Assert.EndsWith("/2", result.Reference!.Url);
        Assert.Equal((2, 2, 2), (builds, storage.Uploads, storage.Gets));
    }

    [Theory]
    [InlineData("vgi_rpc.location")]
    [InlineData("vgi_rpc.log_level")]
    [InlineData("vgi_rpc.error_type")]
    public async Task ImportRejectsControlMetadataOnAnOtherwiseValidEnvelope(string key)
    {
        await using var storage = await ObjectStore.StartAsync();
        var repository = new Repository(storage);
        var policy = Policy(repository);
        var published = await policy.Manager.WarmAsync(policy, Request(), () => Task.FromResult(Contents()));
        var manifest = repository.Committed.Values.Single();
        using var input = new MemoryStream(storage.Bytes(published.Reference!.Url));
        using var reader = new ArrowStreamReader(input);
        using var batch = reader.ReadNextRecordBatch()!;
        using var output = new MemoryStream();
        using (var writer = new ArrowStreamWriter(output, batch.Schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(batch, new Dictionary<string, string> { [key] = "invalid control metadata" });
            writer.WriteEnd();
        }
        var bytes = output.ToArray();
        storage.Replace(published.Reference.Url, bytes);
        var damaged = manifest with { Sha256 = CatalogSnapshotManager.Hash(bytes), RawSize = bytes.Length, StoredSize = bytes.Length };
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => new CatalogSnapshotManager().ImportAsync(policy, Request(), damaged));
        Assert.Contains("pointer or log", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ImportRejectsTrailingBytesEvenWithMatchingDigest()
    {
        await using var storage = await ObjectStore.StartAsync();
        var repository = new Repository(storage);
        var policy = Policy(repository);
        var published = await policy.Manager.WarmAsync(policy, Request(), () => Task.FromResult(Contents()));
        var manifest = repository.Committed.Values.Single();
        storage.Append(published.Reference!.Url, [1, 2, 3, 4]);
        var bytes = storage.Bytes(published.Reference.Url);
        var damaged = manifest with { Sha256 = CatalogSnapshotManager.Hash(bytes), RawSize = bytes.Length, StoredSize = bytes.Length };
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => new CatalogSnapshotManager().ImportAsync(policy, Request(), damaged));
        Assert.Contains("trailing", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RenewalPreservesMetadataAndFetchesTheNewUrlOnce(bool concurrentWinner)
    {
        await using var storage = await ObjectStore.StartAsync();
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var repository = new Repository(storage, new("object-id", now + 120, now + 10000));
        var policy = Policy(repository);
        var builds = 0;
        Task<CatalogContentsResponse> Build() { builds++; return Task.FromResult(Contents()); }
        await policy.Manager.WarmAsync(policy, Request(), Build);
        var strict = new CatalogSnapshotPolicy(repository, r => new("test", "revision-1", r.CatalogVersion))
        { External = new() { UrlValidator = null }, ExpiryMarginSeconds = 180 };
        repository.Renewal = old => old with { ManifestId = concurrentWinner ? "concurrent-winner" : old.ManifestId, Url = old.Url + "?renewed=1", ExpiresAt = now + 600 };
        await policy.Manager.WarmAsync(strict, Request(), Build);
        await policy.Manager.WarmAsync(strict, Request(), Build);
        Assert.Equal((1, 1, 2), (builds, storage.Uploads, storage.Gets));
        var stricter = new CatalogSnapshotPolicy(repository, r => new("test", "revision-1", r.CatalogVersion))
        { External = new() { UrlValidator = null }, ExpiryMarginSeconds = 900 };
        repository.Renewal = old => old with { ExpiresAt = now + 2000, Etag = "changed-content" };
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => policy.Manager.WarmAsync(stricter, Request(), Build));
        Assert.Contains("immutable", error.Message, StringComparison.OrdinalIgnoreCase);
    }
}
