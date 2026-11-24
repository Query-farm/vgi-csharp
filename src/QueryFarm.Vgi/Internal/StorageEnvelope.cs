using System.Buffers.Binary;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using QueryFarm.VgiRpc.External;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Value-level externalization of worker function storage through the blob vendor (vgi
/// <c>docs/testing/blob-test-catalog.md</c>, "Storage value envelope (v1)"): with
/// <c>VGI_STORAGE_EXTERNALIZE=1</c> and a vendor (<c>VGI_BLOB_VENDOR_URL</c>), every value
/// <see cref="FunctionStorage"/> writes is enveloped, and one of at least
/// <c>VGI_STORAGE_EXTERNALIZE_BYTES</c> bytes is uploaded (purpose <c>fs</c>) before the backend
/// write and stored as a pointer.
/// </summary>
/// <remarks>
/// <code>
/// inline:  0x00 || raw
/// pointer: 0x01 || u64be(size) || sha256(raw) (32 bytes) || object_id (ASCII)
/// </code>
/// Decoding is strict: an empty value, an unknown tag, a pointer of 41 bytes or fewer, an object id
/// outside <c>[A-Za-z0-9._:-]{1,128}</c>, or a pointer read with no vendor are errors. Reads are
/// verified against the pointer's size and SHA-256, never the vendor's own report, and fail with the
/// contract's <c>storage blob &lt;id&gt; ...</c> messages. Deviation from the reference: this file
/// storage knows no attach, so every upload's scope shard is <c>loc-anon</c>.
/// </remarks>
internal static class StorageEnvelope
{
    public const string EnableVariable = "VGI_STORAGE_EXTERNALIZE";
    public const string ThresholdVariable = "VGI_STORAGE_EXTERNALIZE_BYTES";
    private const long DefaultThreshold = 1_048_576;
    private const byte Inline = 0x00;
    private const byte Pointer = 0x01;
    private const int FixedPointerBytes = 1 + 8 + 32;

    private static readonly Lazy<(IEnvelopeBlobs Client, long Threshold)?> s_config = new(Configure);

    /// <summary>Whether values are enveloped (externalization configured).</summary>
    public static bool Enabled => s_config.Value is not null;

    private static (IEnvelopeBlobs, long)? Configure()
    {
        if (Environment.GetEnvironmentVariable(EnableVariable)?.Trim().ToLowerInvariant() is not ("1" or "true" or "yes"))
        {
            return null;
        }

        // The storage backend (direct-storage contract v1): the blob vendor or the operator's S3
        // bucket. GCS has no function-storage blob store in this SDK.
        IEnvelopeBlobs? client = StorageBackendEnvironment.Select() switch
        {
            StorageBackend.Vendor => VendorBlobClient.FromEnvironment() is { } vendor ? new VendorBlobs(vendor) : null,
            StorageBackend.S3 => new S3Blobs(QueryFarm.VgiRpc.S3.S3StorageEnvironment.FromEnvironment()),
            StorageBackend.Gcs => throw new ArgumentException(
                $"{EnableVariable}=1 with {StorageBackendEnvironment.Backend}=gcs: function-storage blobs need the vendor or s3 backend"),
            _ => null,
        };
        if (client is null)
        {
            return null;
        }

        var raw = Environment.GetEnvironmentVariable(ThresholdVariable)?.Trim();
        var threshold = string.IsNullOrEmpty(raw)
            ? DefaultThreshold
            : long.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var t) && t > 0
                ? t
                : throw new ArgumentException($"{ThresholdVariable} must be a positive integer, got '{raw}'");
        return (client, threshold);
    }

    /// <summary>The bytes to store for <paramref name="raw"/>: itself when disabled, else its envelope.</summary>
    public static byte[] Encode(byte[] raw, byte[] executionId)
    {
        if (s_config.Value is not var (client, threshold))
        {
            return raw;
        }

        if (raw.LongLength < threshold)
        {
            return [Inline, .. raw];
        }

        var reference = client.PutAsync(raw, Scope(executionId), $"{Guid.NewGuid():N}.0").GetAwaiter().GetResult();
        var id = Encoding.ASCII.GetBytes(reference.ObjectId);
        var envelope = new byte[FixedPointerBytes + id.Length];
        envelope[0] = Pointer;
        BinaryPrimitives.WriteUInt64BigEndian(envelope.AsSpan(1), (ulong)raw.LongLength);
        Convert.FromHexString(reference.Sha256).CopyTo(envelope, 9);
        id.CopyTo(envelope, FixedPointerBytes);
        return envelope;
    }

    /// <summary>The value stored as <paramref name="stored"/>.</summary>
    /// <exception cref="InvalidOperationException">A malformed envelope, or a blob that cannot be read
    /// (<c>storage blob &lt;id&gt; ...</c>).</exception>
    public static byte[] Decode(byte[] stored)
    {
        if (s_config.Value is not var (client, _))
        {
            return stored;
        }

        if (stored.Length == 0)
        {
            throw new InvalidOperationException("storage value envelope is empty");
        }

        if (stored[0] == Inline)
        {
            return stored[1..];
        }

        if (stored[0] != Pointer)
        {
            throw new InvalidOperationException($"storage value envelope has an unknown tag 0x{stored[0]:x2}");
        }

        if (stored.Length <= FixedPointerBytes)
        {
            throw new InvalidOperationException("storage value pointer is truncated");
        }

        var size = BinaryPrimitives.ReadUInt64BigEndian(stored.AsSpan(1));
        var sha = Convert.ToHexStringLower(stored.AsSpan(9, 32));
        var id = Encoding.ASCII.GetString(stored, FixedPointerBytes, stored.Length - FixedPointerBytes);
        if (id.Length > 128 || !id.All(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or ':' or '-') || size > long.MaxValue)
        {
            throw new InvalidOperationException("storage value pointer has a malformed object id");
        }

        try
        {
            return client.GetAsync(new BlobRef(id, (long)size, sha)).GetAwaiter().GetResult();
        }
        catch (BlobVendorException exc) when (exc.Code is BlobVendorException.Sha256MismatchCode or BlobVendorException.SizeMismatchCode)
        {
            throw new InvalidOperationException(
                $"storage blob {id} failed its integrity check: {(exc.Code == BlobVendorException.Sha256MismatchCode ? "sha256" : "size")} mismatch");
        }
        catch (BlobVendorException exc) when (exc.Code is "not_found" or "not_uploaded" or "NoSuchKey" || exc.Status == 404)
        {
            throw new InvalidOperationException($"storage blob {id} is missing: not found in the blob vendor (expired or deleted?)");
        }
        catch (Exception exc) when (exc is BlobVendorException or HttpRequestException or IOException or TaskCanceledException)
        {
            throw new InvalidOperationException($"storage blob {id} could not be read: {exc.Message}");
        }
    }

    /// <summary>Deletes every blob of one execution, best effort, in the background.</summary>
    public static void DeleteExecution(byte[] executionId)
    {
        if (s_config.Value is not var (client, _))
        {
            return;
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await client.DeleteScopeAsync(Scope(executionId)).ConfigureAwait(false);
            }
            catch (Exception exc) when (exc is BlobVendorException or HttpRequestException or IOException or TaskCanceledException)
            {
                // Clean-up is best effort; the bucket lifecycle removes what this misses.
            }
        });
    }

    private static BlobScope Scope(byte[] executionId)
    {
        var ident = "s." + Convert.ToHexStringLower(executionId);
        return new BlobScope("loc-anon", ident.Length <= 128 ? ident : "s.h" + Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(ident))));
    }

    /// <summary>The blob operations the envelope needs, over the vendor or an S3 bucket.</summary>
    private interface IEnvelopeBlobs
    {
        Task<BlobRef> PutAsync(byte[] data, BlobScope scope, string idempotencyKey);

        Task<byte[]> GetAsync(BlobRef reference);

        Task DeleteScopeAsync(BlobScope scope);
    }

    private sealed class VendorBlobs(VendorBlobClient client) : IEnvelopeBlobs
    {
        public Task<BlobRef> PutAsync(byte[] data, BlobScope scope, string idempotencyKey) =>
            client.PutBytesAsync(data, scope, idempotencyKey);

        public Task<byte[]> GetAsync(BlobRef reference) => client.GetBytesAsync(reference);

        public Task DeleteScopeAsync(BlobScope scope) => client.DeleteScopeAsync(scope);
    }

    /// <summary>Function-storage blobs in the operator's S3 bucket under
    /// <c>&lt;prefix&gt;fs/&lt;scope&gt;/&lt;id&gt;</c>, the scope hashing the blob scope and the id
    /// (<c>fs.&lt;scope&gt;.&lt;hash&gt;</c>) the scope and idempotency key, so a retried write reuses the
    /// object. Reads are verified against the reference's size and SHA-256. Deleting an execution's
    /// blobs is best effort (direct-storage contract v1, Addendum A); a 2-day lifecycle rule on
    /// <c>fs/</c> is the backstop.</summary>
    private sealed class S3Blobs(QueryFarm.VgiRpc.S3.S3Storage storage) : IEnvelopeBlobs
    {
        public async Task<BlobRef> PutAsync(byte[] data, BlobScope scope, string idempotencyKey)
        {
            var dir = ScopeDir(scope);
            var id = $"fs.{dir}.{Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes($"{scope.Shard}\0{scope.ExecutionId}\0{idempotencyKey}")))[..32]}";
            await storage.PutKeyAsync(Key(id), data, "application/octet-stream", null, CancellationToken.None).ConfigureAwait(false);
            return new BlobRef(id, data.LongLength, Convert.ToHexStringLower(SHA256.HashData(data)));
        }

        public async Task<byte[]> GetAsync(BlobRef reference)
        {
            byte[] data;
            try
            {
                data = await storage.GetAsync(Key(reference.ObjectId), reference.Size, CancellationToken.None).ConfigureAwait(false);
            }
            catch (ObjectMissingException)
            {
                throw new BlobVendorException($"blob {reference.ObjectId} not found in the s3 bucket", 404, "NoSuchKey");
            }

            if (data.LongLength != reference.Size)
            {
                throw new BlobVendorException($"blob {reference.ObjectId} size mismatch", code: BlobVendorException.SizeMismatchCode);
            }

            return Convert.ToHexStringLower(SHA256.HashData(data)) == reference.Sha256.ToLowerInvariant()
                ? data
                : throw new BlobVendorException($"blob {reference.ObjectId} sha256 mismatch", code: BlobVendorException.Sha256MismatchCode);
        }

        public Task DeleteScopeAsync(BlobScope scope) => storage.DeletePrefixAsync(storage.FunctionStorageKey(ScopeDir(scope) + "/"));

        private static string ScopeDir(BlobScope scope) =>
            Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes($"{scope.Shard}\0{scope.ExecutionId}")))[..32];

        private string Key(string id) =>
            id.StartsWith("fs.", StringComparison.Ordinal) && id.IndexOf('.', 3) is var dot and > 3
                ? storage.FunctionStorageKey($"{id[3..dot]}/{id}")
                : storage.FunctionStorageKey(id);
    }
}
