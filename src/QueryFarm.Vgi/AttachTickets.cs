using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Attributes;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi;

/// <summary>What an attach ticket carries, once opened.</summary>
/// <param name="IssuedAt">Unix seconds.</param>
/// <param name="ExpiresAt">Unix seconds; <c>0</c> means no expiry.</param>
/// <param name="TicketId">32 lowercase hex: a correlation handle, not a secret.</param>
/// <param name="CatalogName">The catalog the user attached.</param>
/// <param name="DataVersionSpec"><c>""</c> when the user gave none.</param>
/// <param name="ImplementationVersion"><c>""</c> when the user gave none.</param>
/// <param name="OptionsIpc">Arrow IPC stream of the one-row options record, exactly as
/// <see cref="CatalogAttachRequest.Options"/> carries it; empty for none.</param>
public sealed record AttachTicketClaims(
    long IssuedAt,
    long ExpiresAt,
    string TicketId,
    string CatalogName,
    string DataVersionSpec,
    string ImplementationVersion,
    byte[] OptionsIpc);

/// <summary>A ticket this worker cannot accept: malformed, wrong prefix, non-canonical, wrong key,
/// wrong principal, tampered or a bad payload. One type for every cause, so a caller cannot tell a
/// forged ticket from another user's. <c>INVALID_ARGUMENT</c> / <c>attach_ticket_invalid</c>. The
/// message never contains the ticket.</summary>
public sealed class AttachTicketInvalidException(string detail = "attach ticket not accepted")
    : RpcException(
        "AttachTicketInvalidError",
        detail,
        errorKind: AttachTickets.InvalidKind,
        errorCode: ErrorCodes.InvalidArgument,
        errorDetails: ErrorModel.ToJson([new BadRequest([new FieldViolation(AttachTickets.OptionName, detail)])]));

/// <summary>An authentic ticket outside its lifetime. <c>FAILED_PRECONDITION</c> /
/// <c>attach_ticket_expired</c>: the remedy is a fresh export from a logged-in session, not a
/// retry. Only raised once the ticket has opened under the caller's principal.</summary>
public sealed class AttachTicketExpiredException(string detail = "attach ticket has expired")
    : RpcException(
        "AttachTicketExpiredError",
        detail,
        errorKind: AttachTickets.ExpiredKind,
        errorCode: ErrorCodes.FailedPrecondition,
        errorDetails: ErrorModel.ToJson(
            [new PreconditionFailure([new PreconditionViolation("ATTACH_TICKET", AttachTickets.OptionName, detail)])]));

/// <summary>Request for <see cref="IAttachTickets.SealAttachAsync"/>.</summary>
public sealed class SealAttachRequest
{
    /// <summary>The catalog the caller attached.</summary>
    public string CatalogName { get; set; } = "";

    /// <summary>The options the caller attached with, secret ones included: a one-row record as
    /// Arrow IPC, exactly as <see cref="CatalogAttachRequest.Options"/>. Null for none.</summary>
    public byte[]? Options { get; set; }

    /// <summary>As given at ATTACH; <c>""</c> for none.</summary>
    public string DataVersionSpec { get; set; } = "";

    /// <summary>As given at ATTACH; <c>""</c> for none.</summary>
    public string ImplementationVersion { get; set; } = "";

    /// <summary>Requested lifetime; <c>0</c> asks for as long as the worker allows.</summary>
    public long TtlSeconds { get; set; }
}

/// <summary>A sealed attach.</summary>
public sealed class AttachTicket
{
    /// <summary>The <c>vgia1.</c> text. Not a credential, but never logged.</summary>
    public string Ticket { get; set; } = "";

    /// <summary>Unix seconds after which the worker refuses it; <c>+inf</c> when it has no expiry.</summary>
    public double ExpiresAt { get; set; }
}

/// <summary><c>vgi.attach_tickets.v1</c>: sealing a user's ATTACH so a runner holding their grant
/// can replay it.</summary>
/// <remarks>Hosted on HTTP only, and only when <c>VGI_SIGNING_KEY</c> is configured explicitly and
/// the worker can issue grants (see <see cref="Worker.SigningKey"/>).</remarks>
[ProtocolName(AttachTickets.ProtocolName)]
public interface IAttachTickets
{
    /// <summary>Seals the caller's attach of <c>request.CatalogName</c> into a ticket. Anonymous is
    /// <c>action_denied</c>; options are validated against the catalog's declared attach options
    /// (<c>invalid_request</c>). No fresh login is required: a ticket carries no authority.</summary>
    Task<AttachTicket> SealAttachAsync(SealAttachRequest request, ICallContext? ctx = null);
}

/// <summary>
/// Attach tickets: a user's ATTACH, sealed so a runner can replay it later as that user, without
/// ever seeing an option. Normative spec: vgi-python <c>docs/protocol/vgi-attach-tickets.md</c>;
/// byte-exact vectors: <c>attach_ticket_vectors.json</c>.
/// </summary>
/// <remarks>
/// <code>
/// ticket   = "vgia1." base64url_nopad(0x01 || nonce(24) || XChaCha20-Poly1305(payload, aad))
/// key      = VGI_SIGNING_KEY bytes, used as-is when 32 bytes, else SHA-256 of them
/// aad      = "vgi.attach_ticket.v1" 0x00 || UTF-8(principal)
/// payload  = issued_at i64 | expires_at i64 | ticket_id, catalog_name, data_version_spec,
///            implementation_version (u16 len + UTF-8 each) | options (u32 len + Arrow IPC)
/// </code>
/// The AAD binds the principal only, not (domain, principal): a ticket is sealed while the user
/// is logged in and opened when a runner presents their grant (domain <c>grant</c>).
/// </remarks>
public static partial class AttachTickets
{
    /// <summary>Token prefix; the version is in it, so an incompatible format never half-parses.</summary>
    public const string Prefix = "vgia1.";

    /// <summary>The reserved ATTACH option a runner presents a ticket in. No catalog may declare
    /// an attach option with this name, compared case-insensitively.</summary>
    public const string OptionName = "vgi_attach_ticket";

    /// <summary>Wire name of the protocol hosting <c>seal_attach</c>.</summary>
    public const string ProtocolName = "vgi.attach_tickets.v1";

    /// <summary>The protocol's declared version.</summary>
    public const string ProtocolVersion = "1.0.0";

    /// <summary>The envelope's version byte, fixed by this format.</summary>
    public const byte EnvelopeVersion = 0x01;

    /// <summary>Largest options record (serialized Arrow IPC bytes) a ticket may carry.</summary>
    public const int MaxOptionsBytes = 16 * 1024;

    /// <summary>Longest ticket text considered at all.</summary>
    public const int MaxTicketChars = 32 * 1024;

    /// <summary>Allowance for clocks disagreeing between the sealing and redeeming worker.</summary>
    public const long ClockSkewSeconds = 60;

    /// <summary>Error kind of a ticket that cannot be accepted.</summary>
    public const string InvalidKind = "attach_ticket_invalid";

    /// <summary>Error kind of an authentic ticket outside its lifetime.</summary>
    public const string ExpiredKind = "attach_ticket_expired";

    private const int NonceSize = 24;
    private const int MaxText = 0xFFFF;
    private static readonly byte[] s_aadDomain = Encoding.ASCII.GetBytes("vgi.attach_ticket.v1\0");
    private static readonly UTF8Encoding s_strictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    [GeneratedRegex("^[0-9a-f]{32}$")]
    private static partial Regex TicketIdPattern();

    /// <summary>The ticket AAD: <c>"vgi.attach_ticket.v1" 0x00 || UTF-8(principal)</c>.</summary>
    public static byte[] Aad(string principal) => [.. s_aadDomain, .. Encoding.UTF8.GetBytes(principal)];

    /// <summary>The key the envelope uses: 32 bytes as-is, any other length SHA-256'd.</summary>
    public static byte[] NormalizeKey(byte[] key) => key.Length == 32 ? key : SHA256.HashData(key);

    /// <summary>Seals a ticket for <paramref name="principal"/>.</summary>
    /// <param name="signingKey">The worker's signing key (any length).</param>
    /// <param name="principal">The caller the ticket is for; non-empty.</param>
    /// <param name="catalogName">The catalog attached; non-empty.</param>
    /// <param name="optionsIpc">The options record's Arrow IPC bytes, or empty.</param>
    /// <param name="dataVersionSpec"><c>""</c> for none.</param>
    /// <param name="implementationVersion"><c>""</c> for none.</param>
    /// <param name="issuedAt">Unix seconds.</param>
    /// <param name="expiresAt">Unix seconds, or <c>0</c> for no expiry.</param>
    /// <param name="ticketId">Override the random id, for vectors only.</param>
    /// <param name="nonce">A fixed 24-byte nonce, for vectors only.</param>
    /// <exception cref="ArgumentException">An empty principal or catalog, an empty lifetime, a bad
    /// id, or a field too long to encode.</exception>
    public static (string Token, AttachTicketClaims Claims) Mint(
        byte[] signingKey,
        string principal,
        string catalogName,
        byte[] optionsIpc,
        string dataVersionSpec,
        string implementationVersion,
        long issuedAt,
        long expiresAt,
        string? ticketId = null,
        byte[]? nonce = null)
    {
        ArgumentNullException.ThrowIfNull(signingKey);
        if (string.IsNullOrEmpty(principal)) throw new ArgumentException("a ticket needs a principal", nameof(principal));
        if (string.IsNullOrEmpty(catalogName)) throw new ArgumentException("a ticket needs a catalog name", nameof(catalogName));
        var claims = new AttachTicketClaims(
            issuedAt,
            expiresAt,
            ticketId ?? Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16)),
            catalogName,
            dataVersionSpec ?? "",
            implementationVersion ?? "",
            optionsIpc ?? []);
        if (!TicketIdPattern().IsMatch(claims.TicketId)) throw new ArgumentException("ticket_id must be 32 lowercase hex", nameof(ticketId));
        if (expiresAt != 0 && expiresAt <= issuedAt) throw new ArgumentException("expires_at must be 0 or after issued_at", nameof(expiresAt));
        nonce ??= RandomNumberGenerator.GetBytes(NonceSize);
        if (nonce.Length != NonceSize) throw new ArgumentException("nonce must be 24 bytes", nameof(nonce));

        var body = XChaCha20Poly1305.Seal(NormalizeKey(signingKey), nonce, EncodePayload(claims), Aad(principal));
        var envelope = new byte[1 + NonceSize + body.Length];
        envelope[0] = EnvelopeVersion;
        nonce.CopyTo(envelope, 1);
        body.CopyTo(envelope, 1 + NonceSize);
        var token = Prefix + Base64UrlEncode(envelope);
        if (token.Length > MaxTicketChars)
        {
            throw new ArgumentException($"the ticket would be {token.Length} characters; at most {MaxTicketChars} are accepted");
        }

        return (token, claims);
    }

    /// <summary>Verifies a ticket for the calling <paramref name="principal"/> and returns what it
    /// carries.</summary>
    /// <remarks>Order, normative: prefix, length, canonical base64url, the caller, AEAD open under the
    /// caller's principal, strict payload parse, then the lifetime with a 60 s skew. The lifetime is
    /// inside the ciphertext, so it is trusted only after the tag verified.</remarks>
    /// <param name="signingKey">The worker's signing key.</param>
    /// <param name="token">The ticket text, exactly as presented.</param>
    /// <param name="principal">The caller's principal; null or empty (anonymous) never opens one.</param>
    /// <param name="now">Override the clock (Unix seconds), for tests and vectors.</param>
    /// <exception cref="AttachTicketInvalidException">Any cause but the lifetime.</exception>
    /// <exception cref="AttachTicketExpiredException">Authentic but outside its lifetime.</exception>
    public static AttachTicketClaims Open(byte[] signingKey, string token, string? principal, double? now = null)
    {
        ArgumentNullException.ThrowIfNull(signingKey);
        if (token is null || !token.StartsWith(Prefix, StringComparison.Ordinal))
        {
            throw new AttachTicketInvalidException("not an attach ticket");
        }

        if (token.Length > MaxTicketChars)
        {
            throw new AttachTicketInvalidException("attach ticket is too long");
        }

        var envelope = Base64UrlDecodeStrict(token[Prefix.Length..]);
        if (string.IsNullOrEmpty(principal))
        {
            throw new AttachTicketInvalidException("an anonymous caller cannot redeem an attach ticket");
        }

        if (envelope.Length < 1 + NonceSize + XChaCha20Poly1305.TagSize || envelope[0] != EnvelopeVersion)
        {
            throw new AttachTicketInvalidException("attach ticket failed verification");
        }

        byte[] payload;
        try
        {
            payload = XChaCha20Poly1305.Open(
                NormalizeKey(signingKey), envelope.AsSpan(1, NonceSize), envelope.AsSpan(1 + NonceSize), Aad(principal));
        }
        catch (CryptographicException)
        {
            throw new AttachTicketInvalidException("attach ticket failed verification");
        }

        var claims = DecodePayload(payload);
        var current = now ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;
        if (claims.IssuedAt > current + ClockSkewSeconds)
        {
            throw new AttachTicketExpiredException("attach ticket is not yet valid");
        }

        if (claims.ExpiresAt != 0 && current >= claims.ExpiresAt + ClockSkewSeconds)
        {
            throw new AttachTicketExpiredException("attach ticket has expired");
        }

        return claims;
    }

    /// <summary>Replaces a ticket-carrying attach request with the attach it seals.</summary>
    /// <param name="request">The incoming <c>catalog_attach</c> request.</param>
    /// <param name="signingKey">The worker's signing key; null when it has none, where no ticket
    /// can open.</param>
    /// <param name="auth">The caller.</param>
    /// <param name="now">Override the clock, for tests.</param>
    /// <returns><see langword="null"/> when the options carry no <c>vgi_attach_ticket</c> (the
    /// request is untouched); otherwise the request the user originally made -- the sealed catalog
    /// name, options and version specs, with this request's client capabilities.</returns>
    /// <exception cref="StatusException"><c>invalid_request</c>: another option rides beside the
    /// ticket. Checked before the ticket is opened.</exception>
    /// <exception cref="AttachTicketInvalidException">The ticket does not open for this caller.</exception>
    /// <exception cref="AttachTicketExpiredException">The ticket is outside its lifetime.</exception>
    public static CatalogAttachRequest? Redeem(
        CatalogAttachRequest request, byte[]? signingKey, AuthContext? auth, double? now = null)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Options is not { Length: > 0 } optionsBytes)
        {
            return null;
        }

        RecordBatch options;
        try
        {
            options = RecordBatchIpc.Read(optionsBytes);
        }
        catch (Exception exc) when (exc is InvalidOperationException or InvalidDataException or IOException or ArgumentException)
        {
            // Not this framework's to judge: the catalog sees the options as it always has.
            return null;
        }

        using (options)
        {
            var names = options.Schema.FieldsList.Select(f => f.Name).ToList();
            var ticketIndex = names.FindIndex(n => string.Equals(n, OptionName, StringComparison.OrdinalIgnoreCase));
            if (ticketIndex < 0)
            {
                return null;
            }

            var others = names.Where((_, i) => i != ticketIndex).ToList();
            if (others.Count > 0)
            {
                throw InvalidRequest(
                    $"{OptionName} must be the only attach option",
                    others.Select(n => ($"options.{n}", $"not allowed alongside {OptionName}")));
            }

            var column = options.Column(ticketIndex);
            string? token = column switch
            {
                StringArray s when options.Length == 1 && !s.IsNull(0) => s.GetString(0),
                LargeStringArray s when options.Length == 1 && !s.IsNull(0) => s.GetString(0),
                _ => null,
            };
            if (token is null)
            {
                throw new AttachTicketInvalidException($"{OptionName} must be a string");
            }

            if (signingKey is null)
            {
                throw new AttachTicketInvalidException("this worker does not redeem attach tickets");
            }

            var claims = Open(signingKey, token, CallerPrincipal(auth), now);
            if (claims.OptionsIpc.Length > 0)
            {
                try
                {
                    RecordBatchIpc.Read(claims.OptionsIpc).Dispose();
                }
                catch (Exception)
                {
                    throw new AttachTicketInvalidException("attach ticket options are not an Arrow IPC record");
                }
            }

            return new CatalogAttachRequest
            {
                Name = claims.CatalogName,
                Options = claims.OptionsIpc.Length == 0 ? null : claims.OptionsIpc,
                DataVersionSpec = claims.DataVersionSpec.Length == 0 ? null : claims.DataVersionSpec,
                ImplementationVersion = claims.ImplementationVersion.Length == 0 ? null : claims.ImplementationVersion,
                ClientCapabilities = request.ClientCapabilities,
            };
        }
    }

    /// <summary>The ticket lifetime ceiling: the grant keys' maximum when configured, else
    /// <c>VGI_RPC_GRANT_MAX_TTL_SECONDS</c> when set, else none.</summary>
    /// <exception cref="ArgumentException">The environment value is not a positive integer.</exception>
    public static long? ResolveMaxTtl(QueryFarm.VgiRpc.Identity.GrantKeys? grantKeys)
    {
        if (grantKeys is not null)
        {
            return grantKeys.MaxTtlSeconds;
        }

        var raw = (Environment.GetEnvironmentVariable(QueryFarm.VgiRpc.Identity.GrantKeys.MaxTtlEnvironmentVariable) ?? "").Trim();
        if (raw.Length == 0)
        {
            return null;
        }

        if (!long.TryParse(raw, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var value) || value <= 0)
        {
            throw new ArgumentException(
                $"{QueryFarm.VgiRpc.Identity.GrantKeys.MaxTtlEnvironmentVariable}='{raw}' must be a positive integer");
        }

        return value;
    }

    internal static string? CallerPrincipal(AuthContext? auth) =>
        auth is { Authenticated: true, Principal: { Length: > 0 } principal } ? principal : null;

    internal static StatusException InvalidRequest(string message, IEnumerable<(string Field, string Description)> violations) =>
        new(message, ErrorCodes.InvalidArgument, "invalid_request",
            [new BadRequest(violations.Select(v => new FieldViolation(v.Field, v.Description)).ToList())]);

    internal static StatusException ActionDenied(string message) =>
        new(message, ErrorCodes.PermissionDenied, "action_denied",
            [new ErrorInfo(new Dictionary<string, string> { ["action"] = "seal_attach" })]);

    /// <summary>The payload, little-endian, per §2.1.</summary>
    public static byte[] EncodePayload(AttachTicketClaims claims)
    {
        ArgumentNullException.ThrowIfNull(claims);
        if (claims.OptionsIpc.Length > MaxOptionsBytes)
        {
            throw new ArgumentException($"options are {claims.OptionsIpc.Length} bytes; a ticket carries at most {MaxOptionsBytes}");
        }

        using var stream = new MemoryStream();
        Span<byte> word = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(word, claims.IssuedAt);
        stream.Write(word);
        BinaryPrimitives.WriteInt64LittleEndian(word, claims.ExpiresAt);
        stream.Write(word);
        WriteText(stream, claims.TicketId, "ticket_id");
        WriteText(stream, claims.CatalogName, "catalog_name");
        WriteText(stream, claims.DataVersionSpec, "data_version_spec");
        WriteText(stream, claims.ImplementationVersion, "implementation_version");
        BinaryPrimitives.WriteUInt32LittleEndian(word, (uint)claims.OptionsIpc.Length);
        stream.Write(word[..4]);
        stream.Write(claims.OptionsIpc);
        return stream.ToArray();
    }

    private static void WriteText(MemoryStream stream, string value, string field)
    {
        var raw = Encoding.UTF8.GetBytes(value);
        if (raw.Length > MaxText)
        {
            throw new ArgumentException($"{field} is longer than 65535 bytes");
        }

        Span<byte> length = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(length, (ushort)raw.Length);
        stream.Write(length);
        stream.Write(raw);
    }

    /// <summary>Parses strictly: exact lengths, valid UTF-8, field rules, no trailing bytes.</summary>
    private static AttachTicketClaims DecodePayload(byte[] payload)
    {
        var pos = 0;

        ReadOnlySpan<byte> Take(int n)
        {
            if (n < 0 || pos + n > payload.Length)
            {
                throw new AttachTicketInvalidException("attach ticket payload is truncated");
            }

            var chunk = payload.AsSpan(pos, n);
            pos += n;
            return chunk;
        }

        string Text()
        {
            var length = BinaryPrimitives.ReadUInt16LittleEndian(Take(2));
            try
            {
                return s_strictUtf8.GetString(Take(length));
            }
            catch (DecoderFallbackException)
            {
                throw new AttachTicketInvalidException("attach ticket payload is not UTF-8");
            }
        }

        var issuedAt = BinaryPrimitives.ReadInt64LittleEndian(Take(8));
        var expiresAt = BinaryPrimitives.ReadInt64LittleEndian(Take(8));
        var ticketId = Text();
        var catalogName = Text();
        var dataVersionSpec = Text();
        var implementationVersion = Text();
        var optionsLength = BinaryPrimitives.ReadUInt32LittleEndian(Take(4));
        if (optionsLength > MaxOptionsBytes)
        {
            throw new AttachTicketInvalidException("attach ticket options exceed 16 KiB");
        }

        var options = Take((int)optionsLength).ToArray();
        if (pos != payload.Length)
        {
            throw new AttachTicketInvalidException("attach ticket payload has trailing bytes");
        }

        if (!TicketIdPattern().IsMatch(ticketId))
        {
            throw new AttachTicketInvalidException("attach ticket id is not 32 lowercase hex");
        }

        if (catalogName.Length == 0)
        {
            throw new AttachTicketInvalidException("attach ticket names no catalog");
        }

        if (expiresAt != 0 && expiresAt <= issuedAt)
        {
            throw new AttachTicketInvalidException("attach ticket lifetime is empty");
        }

        return new AttachTicketClaims(issuedAt, expiresAt, ticketId, catalogName, dataVersionSpec, implementationVersion, options);
    }

    private static string Base64UrlEncode(byte[] data) =>
        Convert.ToBase64String(data).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    /// <summary>Decodes unpadded base64url, rejecting every spelling but the canonical one.</summary>
    private static byte[] Base64UrlDecodeStrict(string text)
    {
        if (text.Length == 0 || text.Length % 4 == 1
            || !text.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_'))
        {
            throw new AttachTicketInvalidException("attach ticket is not unpadded base64url");
        }

        byte[] raw;
        try
        {
            var standard = text.Replace('-', '+').Replace('_', '/');
            raw = Convert.FromBase64String(standard + new string('=', (4 - (standard.Length % 4)) % 4));
        }
        catch (FormatException)
        {
            throw new AttachTicketInvalidException("attach ticket is not unpadded base64url");
        }

        if (!string.Equals(Base64UrlEncode(raw), text, StringComparison.Ordinal))
        {
            throw new AttachTicketInvalidException("attach ticket is not canonical base64url");
        }

        return raw;
    }
}

/// <summary><c>vgi.attach_tickets.v1</c> over a worker's catalogs.</summary>
/// <remarks>Seals with the worker's signing key, so tickets live exactly as long as that key does.</remarks>
internal sealed class AttachTicketsService(CatalogRegistry catalog, byte[] signingKey, long? maxTtlSeconds) : IAttachTickets
{
    public Task<AttachTicket> SealAttachAsync(SealAttachRequest request, ICallContext? ctx = null)
    {
        ArgumentNullException.ThrowIfNull(request);
        var principal = AttachTickets.CallerPrincipal(ctx?.Auth)
            ?? throw AttachTickets.ActionDenied("an anonymous caller cannot seal an attach ticket");

        var violations = new List<(string, string)>();
        if (request.TtlSeconds < 0)
        {
            violations.Add(("ttl_seconds", "must be 0 (as long as allowed) or positive"));
        }

        var optionNames = new List<string>();
        var hasOptions = false;
        if (request.Options is { Length: > 0 } bytes)
        {
            RecordBatch? batch = null;
            try
            {
                batch = RecordBatchIpc.Read(bytes);
            }
            catch (Exception exc) when (exc is InvalidOperationException or InvalidDataException or IOException or ArgumentException)
            {
                violations.Add(("options", "not an Arrow IPC record"));
            }

            if (batch is not null)
            {
                using (batch)
                {
                    if (batch.Length > 1)
                    {
                        violations.Add(("options", "must be a one-row record"));
                    }
                    else if (batch.Length == 1)
                    {
                        optionNames.AddRange(batch.Schema.FieldsList.Select(f => f.Name));
                        hasOptions = optionNames.Count > 0;
                    }
                }
            }
        }

        var info = catalog.Catalogs.FirstOrDefault(c => c.Name == request.CatalogName);
        if (info is null)
        {
            violations.Add(("catalog_name", $"no catalog named '{request.CatalogName}'"));
        }
        else
        {
            var specs = (info.AttachOptionSpecs ?? []).Select(EmbeddedIpc.Decode<AttachOptionSpec>).ToList();
            var declared = specs.Select(s => s.Name.ToLowerInvariant()).ToHashSet(StringComparer.Ordinal);
            foreach (var name in optionNames)
            {
                if (string.Equals(name, AttachTickets.OptionName, StringComparison.OrdinalIgnoreCase))
                {
                    violations.Add(($"options.{name}", "a ticket cannot seal another ticket"));
                }
                else if (!declared.Contains(name.ToLowerInvariant()))
                {
                    violations.Add(($"options.{name}", "not an attach option this catalog declares"));
                }
            }

            var supplied = optionNames.Select(n => n.ToLowerInvariant()).ToHashSet(StringComparer.Ordinal);
            foreach (var spec in specs.Where(s => s.Required && !supplied.Contains(s.Name.ToLowerInvariant())))
            {
                violations.Add(($"options.{spec.Name}", "required"));
            }
        }

        var optionsIpc = hasOptions ? request.Options! : [];
        if (optionsIpc.Length > AttachTickets.MaxOptionsBytes)
        {
            violations.Add(("options", $"{optionsIpc.Length} bytes; a ticket carries at most {AttachTickets.MaxOptionsBytes}"));
        }

        if (violations.Count > 0)
        {
            throw AttachTickets.InvalidRequest("seal_attach request is invalid", violations);
        }

        var issuedAt = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var ttl = request.TtlSeconds;
        // 0 asks for the ceiling; otherwise the request, capped at the ceiling.
        long? lifetime = ttl == 0 ? maxTtlSeconds : maxTtlSeconds is { } ceiling ? Math.Min(ttl, ceiling) : ttl;
        var expiresAt = lifetime is { } l ? issuedAt + l : 0;
        string token;
        try
        {
            (token, _) = AttachTickets.Mint(
                signingKey, principal, request.CatalogName, optionsIpc,
                request.DataVersionSpec ?? "", request.ImplementationVersion ?? "", issuedAt, expiresAt);
        }
        catch (ArgumentException exc)
        {
            throw AttachTickets.InvalidRequest("seal_attach request is invalid", [("request", exc.Message)]);
        }

        return Task.FromResult(new AttachTicket
        {
            Ticket = token,
            ExpiresAt = expiresAt == 0 ? double.PositiveInfinity : expiresAt,
        });
    }
}
