using System.Text;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Catalog;

/// <summary>
/// The decoded <c>attach_opaque_data</c> envelope a <see cref="Worker"/> builds at
/// <c>catalog_attach</c> and every function, catalog RPC and <see cref="TransactionHooks"/> call of
/// that attach receives back as <c>AttachOpaqueData</c>:
/// <c>&lt;identity&gt;\0&lt;16 random bytes&gt;&lt;extra&gt;</c>.
/// </summary>
/// <remarks>
/// <para>Read the payload a <see cref="Worker.OnAttach"/> handler stored in
/// <see cref="Protocol.AttachContext.ExtraOpaqueData"/> with
/// <c>AttachData.Parse(params.AttachOpaqueData).Extra</c>, rather than splitting the bytes by hand.</para>
/// <para>The value is plaintext here on every transport: over HTTP the SDK seals it on the wire and
/// opens it before any worker code runs. On stdio and unix sockets it reaches the client unsealed,
/// so <see cref="Extra"/> must never carry a <c>secret</c> attach option (vgi-opaque-data-sealing.md
/// rule 5) — keep such a value server-side, keyed by something in <see cref="Extra"/>.</para>
/// </remarks>
public sealed class AttachData
{
    private const byte Separator = 0;

    /// <summary>The length of <see cref="AttachId"/>.</summary>
    public const int AttachIdLength = 16;

    private AttachData(string identity, byte[] attachId, byte[] extra)
    {
        Identity = identity;
        AttachId = attachId;
        Extra = extra;
    }

    /// <summary>The routing identity: <see cref="Protocol.AttachContext.Identity"/>, or the attached
    /// catalog name when the handler set none.</summary>
    public string Identity { get; }

    /// <summary>16 cryptographically random bytes minted per <c>ATTACH</c>: two attaches of the same
    /// catalog never share it. Empty when the value carries none (no attach, or a value built
    /// outside the SDK).</summary>
    public byte[] AttachId { get; }

    /// <summary>The bytes the <see cref="Worker.OnAttach"/> handler returned as
    /// <see cref="Protocol.AttachContext.ExtraOpaqueData"/>; empty when it returned none.</summary>
    public byte[] Extra { get; }

    /// <summary>Decodes <paramref name="attachOpaqueData"/> (any <c>*Params.AttachOpaqueData</c>).
    /// An empty or <see langword="null"/> value decodes to the default identity with no attach id
    /// and no extra bytes.</summary>
    public static AttachData Parse(byte[]? attachOpaqueData)
    {
        if (attachOpaqueData is not { Length: > 0 })
        {
            return new AttachData(CatalogRegistry.DefaultIdentity, [], []);
        }

        var separator = Array.IndexOf(attachOpaqueData, Separator);
        if (separator < 0)
        {
            return new AttachData(Encoding.UTF8.GetString(attachOpaqueData), [], []);
        }

        var identity = Encoding.UTF8.GetString(attachOpaqueData, 0, separator);
        var idStart = separator + 1;
        var idLength = Math.Min(AttachIdLength, attachOpaqueData.Length - idStart);
        var attachId = attachOpaqueData.AsSpan(idStart, idLength).ToArray();
        var extra = attachOpaqueData.AsSpan(idStart + idLength).ToArray();
        return new AttachData(identity, attachId, extra);
    }

    /// <summary>Shorthand for <c>Parse(attachOpaqueData).Extra</c>.</summary>
    public static byte[] ExtraOf(byte[]? attachOpaqueData) => Parse(attachOpaqueData).Extra;
}
