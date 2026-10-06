using System.Collections.ObjectModel;
using System.Security.Cryptography;
using System.Text;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

/// <summary>Turns a catalog's <see cref="CatalogContentsResult"/> into the wire
/// <see cref="CatalogContentsResponse"/>, enforcing the same rules as vgi-python's
/// <c>Worker._catalog_contents_response</c>.</summary>
/// <remarks>
/// <list type="bullet">
/// <item><c>not_modified</c> needs an etag equal to <c>if_none_match</c> and no schemas; anything
/// else is a catalog bug and fails the call.</item>
/// <item>A full answer carries the catalog's etag, or with
/// <see cref="CatalogContentsEtagMode.ContentHash"/> the snapshot's
/// <see cref="CatalogContentsDigest"/>; an etag equal to <c>if_none_match</c> turns it into
/// <c>not_modified</c>. With no etag, <c>if_none_match</c> is ignored.</item>
/// <item>Schema paths must be unique and each parent present; schemas are sent parents first
/// (stable, so equal-depth schemas keep the catalog's order). Each
/// <see cref="SchemaContents.Path"/> must equal the <see cref="SchemaInfo.Path"/> inside its
/// <see cref="SchemaContents.Schema"/>.</item>
/// </list>
/// </remarks>
public static class CatalogContentsResponder
{
    public static CatalogContentsResponse Respond(
        long catalogVersion, CatalogContentsResult result, string? ifNoneMatch, CatalogContentsEtagMode etagMode)
    {
        if (result.NotModified)
        {
            if (result.Etag is null || ifNoneMatch is null || result.Etag != ifNoneMatch)
            {
                throw new InvalidOperationException(
                    "catalog_contents returned not_modified, but only a catalog whose etag equals if_none_match may " +
                    "(and it must return that etag)");
            }

            if (result.Schemas.Count > 0)
            {
                throw new InvalidOperationException("catalog_contents returned not_modified with schemas; it must return none");
            }

            return new CatalogContentsResponse { CatalogVersion = catalogVersion, Etag = result.Etag, NotModified = true };
        }

        var schemas = Validate(result.Schemas);
        var etag = result.Etag ?? (etagMode == CatalogContentsEtagMode.ContentHash ? schemas.Digest : null);
        if (etag is not null && ifNoneMatch is not null && etag == ifNoneMatch)
        {
            return new CatalogContentsResponse { CatalogVersion = catalogVersion, Etag = etag, NotModified = true };
        }

        return new CatalogContentsResponse { CatalogVersion = catalogVersion, Etag = etag, Schemas = [.. schemas] };
    }

    /// <summary>Checks the snapshot's paths and returns it parents first. A list that is already a
    /// <see cref="ValidatedSchemas"/> (a cached snapshot) is returned as is.</summary>
    internal static ValidatedSchemas Validate(IReadOnlyList<SchemaContents> schemas)
    {
        if (schemas is ValidatedSchemas validated)
        {
            return validated;
        }

        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in schemas)
        {
            if (entry.Path is not { Count: > 0 })
            {
                throw new InvalidOperationException("catalog_contents returned a schema with an empty path");
            }

            var infoPath = EmbeddedIpc.Decode<SchemaInfo>(entry.Schema).Path;
            if (!CatalogRegistry.PathsEqual(entry.Path, infoPath))
            {
                throw new InvalidOperationException(
                    $"catalog_contents returned path [{string.Join(", ", entry.Path)}] for SchemaInfo.path [{string.Join(", ", infoPath)}]");
            }

            if (!keys.Add(CatalogRegistry.PathKey(entry.Path)))
            {
                throw new InvalidOperationException("catalog_contents returned duplicate schema paths");
            }
        }

        foreach (var entry in schemas)
        {
            if (entry.Path.Count > 1 && !keys.Contains(CatalogRegistry.PathKey(entry.Path.Take(entry.Path.Count - 1).ToList())))
            {
                throw new InvalidOperationException(
                    $"catalog_contents returned schema path [{string.Join(", ", entry.Path)}] without its parent");
            }
        }

        return new ValidatedSchemas(schemas.OrderBy(entry => entry.Path.Count).ToList());
    }
}

/// <summary>A snapshot whose paths were checked and that is in wire (parents-first) order. Carries
/// its content-hash digest, computed at most once.</summary>
internal sealed class ValidatedSchemas(IList<SchemaContents> ordered) : ReadOnlyCollection<SchemaContents>(ordered)
{
    private string? _digest;

    public string Digest => _digest ??= CatalogContentsDigest.Compute(this);
}

/// <summary>The <see cref="CatalogContentsEtagMode.ContentHash"/> etag: the hex SHA-256 over a
/// <c>catalog_contents</c> snapshot, byte-for-byte the input vgi-python's
/// <c>catalog_contents_digest()</c> hashes, so the same snapshot gets the same etag from either
/// SDK.</summary>
/// <remarks>Every schema's path and the exact item bytes of every kind, in wire order, each value
/// prefixed by its 8-byte little-endian length and each list by its 8-byte little-endian count, so
/// no two different snapshots share an input. Deterministic because item encoding is (map fields
/// are written in key order).</remarks>
public static class CatalogContentsDigest
{
    public static string Compute(IReadOnlyList<SchemaContents> schemas)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var length = new byte[8];

        void Count(int count)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(length, (ulong)count);
            hash.AppendData(length);
        }

        void Chunk(byte[] data)
        {
            Count(data.Length);
            hash.AppendData(data);
        }

        void Chunks(IReadOnlyList<byte[]> values)
        {
            Count(values.Count);
            foreach (var value in values)
            {
                Chunk(value);
            }
        }

        Count(schemas.Count);
        foreach (var entry in schemas)
        {
            Chunks(entry.Path.Select(Encoding.UTF8.GetBytes).ToList());
            Chunk(entry.Schema);
            Chunks(entry.Tables);
            Chunks(entry.Views);
            Chunks(entry.ScalarFunctions);
            Chunks(entry.AggregateFunctions);
            Chunks(entry.TableFunctions);
            Chunks(entry.ScalarMacros);
            Chunks(entry.TableMacros);
            Chunks(entry.Indexes);
        }

        return Convert.ToHexStringLower(hash.GetHashAndReset());
    }
}
