using System.Text;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Catalog;

/// <summary>
/// The decoded <c>attach_opaque_data</c> envelope a <see cref="Worker"/> builds at
/// <c>catalog_attach</c> and every function, catalog RPC and <see cref="TransactionHooks"/> call of
/// that attach receives back as <c>AttachOpaqueData</c>:
/// <c>&lt;identity&gt;\0&lt;16 random bytes&gt;&lt;extra&gt;</c>; when the ATTACH carried options the
/// identity part also holds the SDK's options section (<see cref="Options"/>), after a 0x01 marker.
/// </summary>
/// <remarks>
/// <para>Read the payload a <see cref="Worker.OnAttach"/> handler stored in
/// <see cref="Protocol.AttachContext.ExtraOpaqueData"/> with
/// <c>AttachData.Parse(params.AttachOpaqueData).Extra</c>, rather than splitting the bytes by hand.</para>
/// <para>Read ATTACH options — <c>secret</c> ones included — with <see cref="Options"/> (or the
/// <c>AttachOptions</c> property of any function's params / <see cref="TransactionContext"/>): the SDK
/// keeps them in this value itself, secret values encrypted with a key the client never has.</para>
/// <para>The value is plaintext here on every transport: over HTTP the SDK seals it on the wire and
/// opens it before any worker code runs. On stdio and unix sockets it reaches the client unsealed,
/// so <see cref="Extra"/> must never carry a secret (vgi-opaque-data-sealing.md rule 5). There is no
/// need to: the SDK delivers secret options through <see cref="Options"/>.</para>
/// </remarks>
public sealed class AttachData
{
    private const byte Separator = 0;

    /// <summary>The length of <see cref="AttachId"/>.</summary>
    public const int AttachIdLength = 16;

    private readonly byte[]? _optionsSection;
    private AttachOptions? _options;

    private AttachData(string identity, byte[] attachId, byte[] extra, byte[]? optionsSection = null)
    {
        Identity = identity;
        AttachId = attachId;
        Extra = extra;
        _optionsSection = optionsSection;
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

    /// <summary>The attach's ATTACH options as DuckDB delivered them (declared defaults filled
    /// in), <c>secret</c> ones included — decrypted on first access. Empty when the ATTACH carried no
    /// options (or the value was not minted by this SDK).</summary>
    /// <exception cref="Exception">The encrypted part does not open (moved into another attach,
    /// tampered with, or minted under a key this worker does not have): the uniform
    /// <c>attach_opaque_data not recognized</c> error.</exception>
    public AttachOptions Options => _options ??= AttachOptions.FromSection(_optionsSection, AttachId);

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
        var (identityBytes, section) = AttachOptionsEnvelope.Split(attachOpaqueData.AsMemory(0, separator < 0 ? attachOpaqueData.Length : separator));
        var identity = Encoding.UTF8.GetString(identityBytes.Span);
        if (separator < 0)
        {
            return new AttachData(identity, [], []);
        }

        var idStart = separator + 1;
        var idLength = Math.Min(AttachIdLength, attachOpaqueData.Length - idStart);
        var attachId = attachOpaqueData.AsSpan(idStart, idLength).ToArray();
        var extra = attachOpaqueData.AsSpan(idStart + idLength).ToArray();
        return new AttachData(identity, attachId, extra, idLength == AttachIdLength ? section : null);
    }

    /// <summary>Shorthand for <c>Parse(attachOpaqueData).Extra</c>.</summary>
    public static byte[] ExtraOf(byte[]? attachOpaqueData) => Parse(attachOpaqueData).Extra;
}
