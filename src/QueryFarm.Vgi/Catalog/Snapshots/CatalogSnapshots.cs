using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.External;
using QueryFarm.VgiRpc.Reflection;
using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi.Catalog.Snapshots;

/// <summary>Stable metadata generation and declared visibility. Null scope means attachment-local.</summary>
public sealed record SnapshotIdentity(string CatalogNamespace, string SourceRevision, long CatalogVersion, string? VisibilityScope = null);
public sealed record SnapshotRequest(byte[] AttachOpaqueData, string AttachmentScope, long CatalogVersion, ICallContext? Context);
public sealed record PublicationLocation(string? ObjectId = null, double? ExpiresAt = null, double? RetainUntil = null, bool Stable = false);
public sealed record CatalogPublicationManifest(string CacheKey, string ManifestId, string ProtocolName, string WireRevision,
    string ContractHash, string CodecRevision, long CatalogVersion, string? Etag, string Sha256, long RawSize, long StoredSize,
    string Url, string? ObjectId, double? ExpiresAt, double? RetainUntil, bool Stable, int FormatVersion = 1, int ValidatorRevision = 1)
{
    public ExternalRef Reference => new(Url, Sha256);
    public override string ToString() => $"CatalogPublicationManifest[{CacheKey}]";
}
/// <summary>Durable implementations atomically discover, commit and conditionally invalidate manifests.</summary>
public interface ICatalogPublicationRepository
{
    string Profile { get; }
    IExternalStorage Storage { get; }
    Task<CatalogPublicationManifest?> LookupAsync(string key);
    Task<CatalogPublicationManifest> CommitIfAbsentAsync(CatalogPublicationManifest manifest);
    Task<bool> InvalidateAsync(string key, string manifestId);
    Task<PublicationLocation> LocationAsync(ExternalRef reference);
    Task<CatalogPublicationManifest?> RenewAsync(CatalogPublicationManifest manifest) => Task.FromResult<CatalogPublicationManifest?>(null);
}
public sealed class MemoryCatalogPublicationRepository(IExternalStorage storage, string profile, PublicationLocation location) : ICatalogPublicationRepository
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, CatalogPublicationManifest> _entries = new(StringComparer.Ordinal);
    public string Profile => profile;
    public IExternalStorage Storage => storage;
    public Task<CatalogPublicationManifest?> LookupAsync(string key) => Task.FromResult(_entries.GetValueOrDefault(key));
    public Task<CatalogPublicationManifest> CommitIfAbsentAsync(CatalogPublicationManifest manifest) => Task.FromResult(_entries.GetOrAdd(manifest.CacheKey, manifest));
    public Task<bool> InvalidateAsync(string key, string manifestId) => Task.FromResult(_entries.TryGetValue(key, out var old) && old.ManifestId == manifestId && _entries.TryRemove(new KeyValuePair<string, CatalogPublicationManifest>(key, old)));
    public Task<PublicationLocation> LocationAsync(ExternalRef reference) => Task.FromResult(location);
}
/// <summary>Omit this policy for ordinary delivery. Authentication always runs before its identity hook.</summary>
public sealed class CatalogSnapshotPolicy(ICatalogPublicationRepository repository, Func<SnapshotRequest, SnapshotIdentity> identity)
{
    public ICatalogPublicationRepository Repository { get; } = repository;
    public Func<SnapshotRequest, SnapshotIdentity> Identity { get; } = identity;
    public Func<SnapshotRequest, string?>? Validator { get; init; }
    public ClientExternalConfig External { get; init; } = new();
    public Compression? Compression { get; init; }
    public bool OrdinaryOnFailure { get; init; }
    public long MaxRawBytes { get; init; } = 128 * 1024 * 1024;
    public long MaxStoredBytes { get; init; } = 128 * 1024 * 1024;
    public double ExpiryMarginSeconds { get; init; } = 60;
    public int MaxCachedManifests { get; init; } = 256;
    public CatalogSnapshotManager Manager { get; } = new();
}
internal static class CatalogSnapshotScope { internal static readonly AsyncLocal<string?> Current = new(); }
public sealed record SnapshotDelivery(CatalogContentsResponse? Response, ExternalRef? Reference);

/// <summary>Validated immutable catalog publications. Warm calls perform no inventory or object work.</summary>
public sealed class CatalogSnapshotManager
{
    private const string Wire = "2.2", Codec = "catalog-collections-1";
    private static readonly Schema Outer = new([new Field("result", BinaryType.Default, false)], null);
    private readonly object _stateLock = new();
    private readonly Dictionary<string, KeyGate> _gates = new(StringComparer.Ordinal);
    private readonly Dictionary<string, CatalogPublicationManifest> _validated = new(StringComparer.Ordinal);
    public static string Hash(byte[] bytes) => Convert.ToHexStringLower(SHA256.HashData(bytes));

    public async Task<SnapshotDelivery> ServeAsync(CatalogSnapshotPolicy p, SnapshotRequest request, string? ifNoneMatch, Func<Task<CatalogContentsResponse>> build)
    {
        ValidatePolicy(p);
        var validator = p.Validator?.Invoke(request);
        if (validator is not null && validator == ifNoneMatch) return new(Unchanged(request.CatalogVersion, validator), null);
        try
        {
            return await ServeCheckedAsync(p, request, ifNoneMatch, build).ConfigureAwait(false);
        }
        catch when (p.OrdinaryOnFailure)
        {
            var before = p.Identity(request);
            var response = await build().ConfigureAwait(false);
            MetadataCollections.DecodeCatalog(response);
            if (before != p.Identity(request) || response.CatalogVersion != before.CatalogVersion) throw new InvalidOperationException("Catalog changed during publication fallback.");
            response.Etag ??= Digest(response);
            return new(response.Etag == ifNoneMatch ? Unchanged(response.CatalogVersion, response.Etag) : response, null);
        }
    }
    private async Task<SnapshotDelivery> ServeCheckedAsync(CatalogSnapshotPolicy p, SnapshotRequest request, string? inm, Func<Task<CatalogContentsResponse>> build)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var identity = p.Identity(request);
            var key = Key(p, request, identity);
            using var lease = await AcquireAsync(key).ConfigureAwait(false);
            if (identity != p.Identity(request)) continue;
            CatalogPublicationManifest? manifest;
            lock (_stateLock) _validated.TryGetValue(key, out manifest);
            if (manifest is null)
            {
                var recovered = await p.Repository.LookupAsync(key).ConfigureAwait(false);
                if (recovered is not null)
                {
                    try
                    {
                        if (!Usable(recovered, p.ExpiryMarginSeconds))
                        {
                            var renewed = await p.Repository.RenewAsync(recovered).ConfigureAwait(false);
                            if (renewed is not null) { ValidateRenewal(recovered, renewed); recovered = renewed; }
                        }
                        await ReadAsync(p, key, identity, recovered).ConfigureAwait(false);
                        manifest = recovered;
                    }
                    catch { await p.Repository.InvalidateAsync(key, recovered.ManifestId).ConfigureAwait(false); }
                }
            }
            if (manifest is not null)
            {
                if (Usable(manifest, p.ExpiryMarginSeconds))
                {
                    if (identity != p.Identity(request)) continue;
                    Remember(p, key, manifest);
                    return manifest.Etag is not null && manifest.Etag == inm
                        ? new(Unchanged(manifest.CatalogVersion, manifest.Etag), null) : new(null, manifest.Reference);
                }
                var renewed = await p.Repository.RenewAsync(manifest).ConfigureAwait(false);
                if (renewed is not null) { ValidateRenewal(manifest, renewed); await ReadAsync(p, key, identity, renewed).ConfigureAwait(false); Remember(p, key, renewed); continue; }
                await p.Repository.InvalidateAsync(key, manifest.ManifestId).ConfigureAwait(false);
                lock (_stateLock) _validated.Remove(key);
            }
            var response = await build().ConfigureAwait(false);
            if (response.NotModified) throw new InvalidOperationException("Publication build must return a complete catalog.");
            MetadataCollections.DecodeCatalog(response);
            if (response.CatalogVersion != identity.CatalogVersion || identity != p.Identity(request)) continue;
            response.Etag ??= Digest(response);
            if (response.Etag == inm) return new(Unchanged(response.CatalogVersion, response.Etag), null);
            using var envelope = new RecordBatch(Outer, [new BinaryArray.Builder().Append(EmbeddedIpc.Encode(response)).Build()], 1);
            var bounded = new BoundedStorage(p.Repository.Storage, p.MaxRawBytes, p.MaxStoredBytes);
            var reference = await ExternalLocation.PublishExternalAsync(envelope, bounded, p.Compression).ConfigureAwait(false);
            var location = await p.Repository.LocationAsync(reference).ConfigureAwait(false);
            manifest = new(key, Guid.NewGuid().ToString("N"), "vgi.v2", Wire, ContractHash(), Codec, identity.CatalogVersion, response.Etag,
                reference.Sha256!, bounded.RawSize, bounded.StoredSize, reference.Url, location.ObjectId, location.ExpiresAt, location.RetainUntil, location.Stable);
            var readback = await ReadAsync(p, key, identity, manifest).ConfigureAwait(false);
            if (response.CatalogVersion != readback.CatalogVersion || response.Etag != readback.Etag || !Streams(response).Zip(Streams(readback)).All(pair => pair.First is null ? pair.Second is null : pair.Second is not null && pair.First.AsSpan().SequenceEqual(pair.Second))) throw new InvalidOperationException("Published catalog differs from collected snapshot.");
            if (identity != p.Identity(request)) continue;
            var winner = await p.Repository.CommitIfAbsentAsync(manifest).ConfigureAwait(false);
            if (winner.ManifestId != manifest.ManifestId) await ReadAsync(p, key, identity, winner).ConfigureAwait(false);
            if (identity != p.Identity(request)) continue;
            Remember(p, key, winner);
            return new(null, winner.Reference);
        }
        throw new InvalidOperationException("Catalog revision or publication changed repeatedly.");
    }
    public Task<SnapshotDelivery> WarmAsync(CatalogSnapshotPolicy p, SnapshotRequest request, Func<Task<CatalogContentsResponse>> build) => ServeAsync(p, request, null, build);
    public async Task ImportAsync(CatalogSnapshotPolicy p, SnapshotRequest request, CatalogPublicationManifest manifest)
    {
        ValidatePolicy(p);
        var identity = p.Identity(request); var key = Key(p, request, identity);
        using var lease = await AcquireAsync(key).ConfigureAwait(false);
        await ReadAsync(p, key, identity, manifest).ConfigureAwait(false);
        if (identity != p.Identity(request)) throw new InvalidOperationException("Catalog changed during publication import.");
        var winner = await p.Repository.CommitIfAbsentAsync(manifest).ConfigureAwait(false);
        if (winner.ManifestId != manifest.ManifestId) await ReadAsync(p, key, identity, winner).ConfigureAwait(false);
        if (identity != p.Identity(request)) throw new InvalidOperationException("Catalog changed during publication import.");
        Remember(p, key, winner);
    }
    public async Task<bool> InvalidateAsync(CatalogSnapshotPolicy p, string key, string manifestId)
    {
        using var lease = await AcquireAsync(key).ConfigureAwait(false);
        lock (_stateLock) { if (_validated.TryGetValue(key, out var old) && old.ManifestId == manifestId) _validated.Remove(key); }
        return await p.Repository.InvalidateAsync(key, manifestId).ConfigureAwait(false);
    }
    private void Remember(CatalogSnapshotPolicy p, string key, CatalogPublicationManifest value)
    {
        lock (_stateLock) { if (_validated.Count >= p.MaxCachedManifests && !_validated.ContainsKey(key)) _validated.Clear(); _validated[key] = value; }
    }
    private sealed class KeyGate
    {
        internal readonly SemaphoreSlim Semaphore = new(1, 1);
        internal int Users;
    }
    private sealed class KeyLease(CatalogSnapshotManager owner, string key, KeyGate gate) : IDisposable
    {
        public void Dispose()
        {
            gate.Semaphore.Release();
            lock (owner._stateLock)
            {
                if (--gate.Users == 0) { owner._gates.Remove(key); gate.Semaphore.Dispose(); }
            }
        }
    }
    private async Task<KeyLease> AcquireAsync(string key)
    {
        KeyGate gate;
        lock (_stateLock)
        {
            if (!_gates.TryGetValue(key, out gate!)) _gates[key] = gate = new();
            gate.Users++;
        }
        await gate.Semaphore.WaitAsync().ConfigureAwait(false);
        return new(this, key, gate);
    }
    private async Task<CatalogContentsResponse> ReadAsync(CatalogSnapshotPolicy p, string key, SnapshotIdentity identity, CatalogPublicationManifest m)
    {
        if (m.CacheKey != key || m.CatalogVersion != identity.CatalogVersion || m.ProtocolName != "vgi.v2" || m.WireRevision != Wire || m.ContractHash != ContractHash() || m.CodecRevision != Codec || m.FormatVersion != 1 || m.ValidatorRevision != 1 || m.Sha256 is not { Length: 64 } || m.RawSize <= 0 || m.RawSize > p.MaxRawBytes || m.StoredSize <= 0 || m.StoredSize > p.MaxStoredBytes || !Usable(m, p.ExpiryMarginSeconds)) throw new InvalidOperationException("Invalid or expired catalog publication manifest.");
        var config = new FetchConfig { MaxFetchBytes = Math.Min(p.External.FetchConfig.MaxFetchBytes, p.MaxStoredBytes), MaxDecompressedBytes = Math.Min(p.External.FetchConfig.MaxDecompressedBytes ?? p.MaxRawBytes, p.MaxRawBytes), MaxRedirects = p.External.FetchConfig.MaxRedirects, Timeout = p.External.FetchConfig.Timeout };
        var raw = await ExternalFetch.FetchUrlAsync(m.Url, config, p.External.UrlValidator, CancellationToken.None).ConfigureAwait(false);
        if (raw.LongLength != m.RawSize || Hash(raw) != m.Sha256) throw new InvalidOperationException("Catalog publication digest or size mismatch.");
        using var input = new MemoryStream(raw);
        using var reader = new ArrowStreamReader(input);
        if (!ValueCodec.SchemasEqual(reader.Schema, Outer)) throw new InvalidOperationException("Invalid catalog RPC envelope schema.");
        using var batch = reader.ReadNextRecordBatch() ?? throw new InvalidOperationException("Missing catalog RPC result.");
        if (reader.LastBatchCustomMetadata?.Keys.Any(key => key.StartsWith("vgi_rpc.location", StringComparison.Ordinal) || key.StartsWith("vgi_rpc.log_", StringComparison.Ordinal) || key.StartsWith("vgi_rpc.error_", StringComparison.Ordinal)) == true)
            throw new InvalidOperationException("Catalog RPC envelope cannot contain pointer or log metadata.");
        if (batch.Length != 1 || batch.Column(0).IsNull(0)) throw new InvalidOperationException("Invalid catalog RPC result row count.");
        using var extra = reader.ReadNextRecordBatch();
        if (extra is not null) throw new InvalidOperationException("Catalog RPC envelope has multiple batches.");
        if (input.Position != input.Length) throw new InvalidOperationException("Catalog RPC envelope has trailing bytes.");
        var inner = ((BinaryArray)batch.Column(0)).GetBytes(0).ToArray();
        using var innerInput = new MemoryStream(inner);
        using var innerReader = new ArrowStreamReader(innerInput);
        if (!ValueCodec.SchemasEqual(innerReader.Schema, SchemaDerivation.InnerSchemaFor(typeof(CatalogContentsResponse)))) throw new InvalidOperationException("Invalid catalog response schema.");
        using var row = innerReader.ReadNextRecordBatch() ?? throw new InvalidOperationException("Missing catalog response row.");
        using var second = innerReader.ReadNextRecordBatch();
        if (row.Length != 1 || second is not null) throw new InvalidOperationException("Catalog response must have one row.");
        if (innerInput.Position != innerInput.Length) throw new InvalidOperationException("Catalog response has trailing bytes.");
        var response = EmbeddedIpc.Decode<CatalogContentsResponse>(inner);
        if (response.NotModified || response.CatalogVersion != m.CatalogVersion || response.Etag != m.Etag) throw new InvalidOperationException("Catalog publication metadata mismatch.");
        MetadataCollections.DecodeCatalog(response);
        return response;
    }
    private static void ValidatePolicy(CatalogSnapshotPolicy p)
    {
        if (p.MaxRawBytes <= 0 || p.MaxStoredBytes <= 0 || p.MaxCachedManifests <= 0 || !double.IsFinite(p.ExpiryMarginSeconds) || p.ExpiryMarginSeconds < 0)
            throw new ArgumentOutOfRangeException(nameof(p), "Catalog publication limits must be positive and the expiry margin finite and nonnegative.");
    }
    private static void ValidateRenewal(CatalogPublicationManifest before, CatalogPublicationManifest after)
    {
        // A conditional renewal may lose to a new publication. Its descriptor
        // is a repository winner, not an edit of the old object; ReadAsync
        // validates its identity, envelope and contents before it can be used.
        if (before.ManifestId != after.ManifestId) return;
        if (before with { Url = after.Url, ExpiresAt = after.ExpiresAt } != after)
            throw new InvalidOperationException("Publication renewal changed immutable catalog metadata.");
    }
    private static bool Usable(CatalogPublicationManifest m, double margin)
    {
        var deadline = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0 + margin;
        return (m.Stable || m.ExpiresAt.HasValue) && (!m.ExpiresAt.HasValue || double.IsFinite(m.ExpiresAt.Value) && m.ExpiresAt > deadline) && (!m.RetainUntil.HasValue || double.IsFinite(m.RetainUntil.Value) && m.RetainUntil > deadline);
    }
    private static string Key(CatalogSnapshotPolicy p, SnapshotRequest r, SnapshotIdentity id)
    {
        if (string.IsNullOrEmpty(id.CatalogNamespace) || string.IsNullOrEmpty(id.SourceRevision) || id.CatalogVersion != r.CatalogVersion) throw new InvalidOperationException("Publication needs namespace, stable revision and matching catalog version.");
        return Hash(JsonSerializer.SerializeToUtf8Bytes(new object[] { "vgi.v2", Wire, ContractHash(), Codec, id.CatalogNamespace, id.SourceRevision, id.VisibilityScope ?? r.AttachmentScope, id.CatalogVersion, p.Repository.Profile, p.Compression?.ToString() ?? "none" }));
    }
    private static string ContractHash() => CatalogWireContract.Hash;
    private static byte[]?[] Streams(CatalogContentsResponse r) => [r.Schemas, r.Tables, r.Views, r.Functions, r.Macros, r.Indexes];
    public static string Digest(CatalogContentsResponse r)
    {
        using var digest = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        digest.AppendData(Encoding.UTF8.GetBytes("vgi.catalog.contents.2.2\0"));
        Span<byte> length = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(length, r.CatalogVersion); digest.AppendData(length);
        foreach (var stream in Streams(r))
        {
            digest.AppendData(new byte[] { stream is null ? (byte)0 : (byte)1 });
            if (stream is null) continue;
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(length, stream.Length); digest.AppendData(length); digest.AppendData(stream);
        }
        return Convert.ToHexStringLower(digest.GetHashAndReset());
    }
    private static CatalogContentsResponse Unchanged(long version, string etag) => new() { CatalogVersion = version, Etag = etag, NotModified = true };

    private sealed class BoundedStorage(IExternalStorage storage, long rawLimit, long storedLimit) : IExternalStorage
    {
        public long RawSize { get; private set; }
        public long StoredSize { get; private set; }
        public async Task<string> UploadAsync(byte[] data, Schema schema, string? contentEncoding, CancellationToken cancellationToken)
        {
            if (data.LongLength > storedLimit) throw new InvalidOperationException("Catalog stored-size limit exceeded.");
            long rawSize = data.LongLength;
            if (contentEncoding is not null)
            {
                using var input = new MemoryStream(data);
                using Stream decoded = contentEncoding switch
                {
                    "zstd" => new ZstdSharp.DecompressionStream(input),
                    "gzip" => new System.IO.Compression.GZipStream(input, System.IO.Compression.CompressionMode.Decompress),
                    _ => throw new InvalidOperationException("Unsupported catalog publication encoding."),
                };
                var buffer = new byte[8192]; rawSize = 0;
                int read;
                while ((read = await decoded.ReadAsync(buffer, cancellationToken).ConfigureAwait(false)) != 0)
                { rawSize += read; if (rawSize > rawLimit) throw new InvalidOperationException("Catalog raw-size limit exceeded."); }
            }
            if (rawSize > rawLimit) throw new InvalidOperationException("Catalog raw-size limit exceeded.");
            RawSize = rawSize; StoredSize = data.LongLength;
            return await storage.UploadAsync(data, schema, contentEncoding, cancellationToken).ConfigureAwait(false);
        }
    }
}
