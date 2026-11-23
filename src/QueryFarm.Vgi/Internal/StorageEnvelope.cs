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

    private static readonly Lazy<(VendorBlobClient Client, long Threshold)?> s_config = new(Configure);

    /// <summary>Whether values are enveloped (externalization configured).</summary>
    public static bool Enabled => s_config.Value is not null;

    private static (VendorBlobClient, long)? Configure()
    {
        if (Environment.GetEnvironmentVariable(EnableVariable)?.Trim().ToLowerInvariant() is not ("1" or "true" or "yes")
            || VendorBlobClient.FromEnvironment() is not { } client)
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

        var reference = client.PutBytesAsync(raw, Scope(executionId), $"{Guid.NewGuid():N}.0").GetAwaiter().GetResult();
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
            return client.GetBytesAsync(new BlobRef(id, (long)size, sha)).GetAwaiter().GetResult();
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
}
