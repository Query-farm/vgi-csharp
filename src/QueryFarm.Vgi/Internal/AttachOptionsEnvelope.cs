using System.Buffers.Binary;
using Apache.Arrow;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The ATTACH options a worker keeps in <c>attach_opaque_data</c> so every function and
/// transaction hook of the attach can read them (<see cref="Catalog.AttachOptions.FromAttachData"/>):
/// the non-secret options as a plain one-row Arrow batch, the <c>secret</c> ones as a one-row batch
/// encrypted by <see cref="AttachSecretOptions"/>.
/// </summary>
/// <remarks>
/// The section rides in the IDENTITY part of the attach value, after a 0x01 marker, base64-encoded:
/// <c>&lt;identity&gt; 0x01 base64(section) 0x00 &lt;16-byte attach id&gt; &lt;extra&gt;</c>, with
/// <c>section = u32le(plain length) || plain IPC || secret blob</c> (the blob is empty when no secret
/// option has a value). The routing identity is everything before the marker, and the bytes after
/// the first NUL are exactly what they were before this section existed, so code that splits
/// <see cref="AttachContext.ExtraOpaqueData"/> off by hand (first NUL + 16 bytes) keeps working.
/// Only an attach whose request carried options gets a section.
/// </remarks>
internal static class AttachOptionsEnvelope
{
    /// <summary>Separates the routing identity from the options section (a catalog name never
    /// contains it).</summary>
    internal const byte Marker = 0x01;

    /// <summary>The section for <paramref name="request"/>'s options, or <see langword="null"/> when
    /// it carries none. Secret options (per <paramref name="specs"/>) are encrypted under the root
    /// <paramref name="secretRoot"/> returns — called only when a secret option has a value.</summary>
    internal static byte[]? Build(byte[]? options, IReadOnlyList<AttachOptionSpec> specs, byte[] attachId, Func<byte[]> secretRoot)
    {
        if (options is not { Length: > 0 })
        {
            return null;
        }

        var batch = RecordBatchIpc.Read(options);
        var secretNames = specs.Where(s => s.Secret == true).Select(s => s.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var plainColumns = new List<int>();
        var secretColumns = new List<int>();
        for (var i = 0; i < batch.ColumnCount; i++)
        {
            var name = batch.Schema.GetFieldByIndex(i).Name;
            (secretNames.Contains(name) ? secretColumns : plainColumns).Add(i);
        }

        var plain = Subset(batch, plainColumns);
        byte[] blob = [];
        if (secretColumns.Count > 0 && secretColumns.Any(i => !batch.Column(i).IsNull(0)))
        {
            blob = AttachSecretOptions.Encrypt(secretRoot(), attachId, Subset(batch, secretColumns));
        }

        var section = new byte[4 + plain.Length + blob.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(section, (uint)plain.Length);
        plain.CopyTo(section, 4);
        blob.CopyTo(section, 4 + plain.Length);
        return section;
    }

    private static byte[] Subset(RecordBatch batch, List<int> columns)
    {
        if (columns.Count == 0)
        {
            return [];
        }

        var schema = new Schema(columns.Select(batch.Schema.GetFieldByIndex), metadata: null);
        return RecordBatchIpc.Write(new RecordBatch(schema, columns.Select(batch.Column), batch.Length));
    }

    /// <summary>Inserts <paramref name="section"/> before the first NUL of an
    /// <c>&lt;identity&gt;\0&lt;id&gt;&lt;extra&gt;</c> value.</summary>
    internal static byte[] Insert(byte[] attachOpaqueData, byte[] section)
    {
        var nul = System.Array.IndexOf(attachOpaqueData, (byte)0);
        var encoded = System.Text.Encoding.ASCII.GetBytes(Convert.ToBase64String(section));
        return [.. attachOpaqueData.AsSpan(0, nul), Marker, .. encoded, .. attachOpaqueData.AsSpan(nul)];
    }

    /// <summary>Splits the identity part (the bytes before the first NUL) into the routing identity
    /// bytes and the options section (<see langword="null"/> when there is none or it is not valid
    /// base64).</summary>
    internal static (ReadOnlyMemory<byte> Identity, byte[]? Section) Split(ReadOnlyMemory<byte> identityPart)
    {
        var marker = identityPart.Span.IndexOf(Marker);
        if (marker < 0)
        {
            return (identityPart, null);
        }

        var text = System.Text.Encoding.ASCII.GetString(identityPart.Span[(marker + 1)..]);
        var section = new byte[text.Length];
        return Convert.TryFromBase64String(text, section, out var written) && written >= 4
            ? (identityPart[..marker], section[..written])
            : (identityPart[..marker], null);
    }

    /// <summary>The section's two parts.</summary>
    /// <exception cref="ValueError">A malformed section: <c>attach_opaque_data not recognized</c>.</exception>
    internal static (byte[] Plain, byte[] SecretBlob) Parts(byte[] section)
    {
        var plainLength = BinaryPrimitives.ReadUInt32LittleEndian(section);
        if (plainLength > section.Length - 4)
        {
            throw OpaqueSealer.Rejected(OpaqueSealer.AttachField);
        }

        return (section.AsSpan(4, (int)plainLength).ToArray(), section.AsSpan(4 + (int)plainLength).ToArray());
    }
}
