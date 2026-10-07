using System.Collections.Concurrent;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.Identity;
using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Seals and opens the values a client holds for the worker -- <c>attach_opaque_data</c> and
/// <c>transaction_opaque_data</c> -- per vgi-python <c>docs/protocol/vgi-opaque-data-sealing.md</c>.
/// </summary>
/// <remarks>
/// <para>Envelope: <c>version(1) || nonce(24) || XChaCha20-Poly1305(plaintext, aad)</c>, keyed by
/// the deployment's signing key (32 bytes as-is, else SHA-256 of it). The AAD binds the caller --
/// <c>0x01 domain 0x00 principal</c> when authenticated, <c>0x00 "anonymous"</c> otherwise -- so a
/// value sealed for one caller never opens for another. A transaction value also binds the sealed
/// parent attach, so it cannot be lifted onto another attach, even by the same principal.</para>
/// <para>Every failure to open -- wrong caller, wrong parent attach, tampering, a malformed value,
/// another key -- is the same <c>"&lt;field&gt; not recognized"</c> / <c>INVALID_ARGUMENT</c>, so a
/// probing caller learns nothing about which check failed. There is no plaintext fallback.</para>
/// </remarks>
internal sealed class OpaqueSealer
{
    /// <summary>Envelope version of an attach value.</summary>
    public const byte AttachVersion = 2;

    /// <summary>Envelope version of a transaction value.</summary>
    public const byte TransactionVersion = 2;

    /// <summary>The <c>error_kind</c> of every rejection, whatever failed.</summary>
    public const string RejectedKind = "opaque_data_not_recognized";

    public const string AttachField = "attach_opaque_data";
    public const string TransactionField = "transaction_opaque_data";

    private const int NonceSize = 24;
    private static readonly byte[] s_attachPrefix = Encoding.ASCII.GetBytes("vgi.attach_opaque_data.v1\0");
    private static readonly byte[] s_transactionPrefix = Encoding.ASCII.GetBytes("vgi.transaction_opaque_data.v1\0");
    private static readonly byte[] s_anonymous = [0, .. Encoding.ASCII.GetBytes("anonymous")];

    private readonly byte[] _key;

    public OpaqueSealer(byte[] signingKey)
    {
        ArgumentNullException.ThrowIfNull(signingKey);
        _key = signingKey.Length == XChaCha20Poly1305.KeySize ? signingKey : SHA256.HashData(signingKey);
    }

    /// <summary>The identity half of every AAD.</summary>
    internal static byte[] IdentityTail(AuthContext? auth) =>
        auth is not { Authenticated: true }
            ? s_anonymous
            : [1, .. Encoding.UTF8.GetBytes(auth.Domain ?? ""), 0, .. Encoding.UTF8.GetBytes(auth.Principal ?? "")];

    internal static byte[] AttachAad(AuthContext? auth) => [.. s_attachPrefix, .. IdentityTail(auth)];

    internal static byte[] TransactionAad(AuthContext? auth, byte[] sealedAttach) =>
        [.. s_transactionPrefix, .. IdentityTail(auth), 0, .. sealedAttach];

    /// <summary>The uniform rejection: identical for every cause.</summary>
    internal static ValueError Rejected(string field) => new($"{field} not recognized");

    public byte[] SealAttach(byte[] plaintext, AuthContext? auth) => Seal(plaintext, AttachAad(auth), AttachVersion);

    public byte[] OpenAttach(byte[] envelope, AuthContext? auth) => Open(envelope, AttachAad(auth), AttachVersion, AttachField);

    public byte[] SealTransaction(byte[] plaintext, AuthContext? auth, byte[] sealedAttach) =>
        Seal(plaintext, TransactionAad(auth, sealedAttach), TransactionVersion);

    public byte[] OpenTransaction(byte[] envelope, AuthContext? auth, byte[] sealedAttach) =>
        Open(envelope, TransactionAad(auth, sealedAttach), TransactionVersion, TransactionField);

    private byte[] Seal(byte[] plaintext, byte[] aad, byte version)
    {
        var nonce = RandomNumberGenerator.GetBytes(NonceSize);
        var body = XChaCha20Poly1305.Seal(_key, nonce, plaintext, aad);
        return [version, .. nonce, .. body];
    }

    private byte[] Open(byte[] envelope, byte[] aad, byte version, string field)
    {
        if (envelope.Length < 1 + NonceSize + XChaCha20Poly1305.TagSize || envelope[0] != version)
        {
            throw Rejected(field);
        }

        try
        {
            return XChaCha20Poly1305.Open(_key, envelope.AsSpan(1, NonceSize), envelope.AsSpan(1 + NonceSize), aad);
        }
        catch (CryptographicException)
        {
            throw Rejected(field);
        }
    }
}

/// <summary>The uniform rejection of a client-held value that does not open: <c>ValueError</c>,
/// message exactly <c>"&lt;field&gt; not recognized"</c>, <c>INVALID_ARGUMENT</c>, kind
/// <c>opaque_data_not_recognized</c>, no details -- the wire shape the reference
/// sends (a Python <c>ValueError</c>); named so the wire type matches. A plain exception rather than
/// an <see cref="RpcException"/>, whose message would carry its type as a prefix.</summary>
internal sealed class ValueError(string message) : Exception(message), IRpcErrorModel
{
    public string ErrorCode => ErrorCodes.InvalidArgument;

    public string? ErrorKind => OpaqueSealer.RejectedKind;

    public IReadOnlyList<System.Text.Json.JsonElement> ErrorDetails => [];
}

/// <summary>
/// <see cref="IVgiService"/> with every client-held value sealed on the way out and opened on the
/// way in -- the boundary between the wire and <see cref="VgiServiceImpl"/>, which only ever sees
/// plaintext.
/// </summary>
/// <remarks>
/// <para>A dispatch proxy rather than a hand-written decorator, so the rule covers every method of
/// the contract, including ones added later, without a list to keep in step: it opens any
/// parameter named <c>attachOpaqueData</c> / <c>transactionOpaqueData</c>, any request property
/// <c>AttachOpaqueData</c> / <c>TransactionOpaqueData</c>, and the same two inside an embedded
/// <c>BindCall</c>; it seals <see cref="CatalogAttachResult.AttachOpaqueData"/> and
/// <see cref="TransactionBeginResponse.TransactionOpaqueData"/>. The transaction value is bound to
/// the sealed attach the same call carries.</para>
/// <para>Installed on HTTP, the authenticating transport. An empty or absent value is "not
/// attached", carries no state, and passes as such; anything else must open.</para>
/// </remarks>
public class OpaqueSealingProxy : DispatchProxy
{
    private const string AttachParameter = "attachOpaqueData";
    private const string TransactionParameter = "transactionOpaqueData";
    private static readonly ConcurrentDictionary<Type, (PropertyInfo? Attach, PropertyInfo? Transaction, PropertyInfo? BindCall)> s_shapes = new();

    private IVgiService _inner = null!;
    private OpaqueSealer _sealer = null!;

    internal static IVgiService Wrap(IVgiService inner, OpaqueSealer sealer)
    {
        var proxy = Create<IVgiService, OpaqueSealingProxy>();
        var self = (OpaqueSealingProxy)(object)proxy;
        self._inner = inner;
        self._sealer = sealer;
        return proxy;
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        ArgumentNullException.ThrowIfNull(targetMethod);
        args ??= [];
        var auth = args.OfType<ICallContext>().FirstOrDefault()?.Auth ?? AuthContext.Anonymous;
        var parameters = targetMethod.GetParameters();

        // Attach first: a transaction value opens against the sealed attach beside it.
        byte[]? sealedAttach = null;
        for (var i = 0; i < parameters.Length; i++)
        {
            if (parameters[i].Name == AttachParameter && args[i] is byte[] { Length: > 0 } attach)
            {
                sealedAttach = attach;
                args[i] = _sealer.OpenAttach(attach, auth);
            }
        }

        for (var i = 0; i < parameters.Length; i++)
        {
            if (parameters[i].Name == TransactionParameter && args[i] is byte[] { Length: > 0 } transaction)
            {
                args[i] = _sealer.OpenTransaction(transaction, auth, sealedAttach ?? []);
            }
            else if (args[i] is { } request && request is not byte[] and not ICallContext and not string)
            {
                OpenRequest(request, auth);
            }
        }

        object? result;
        try
        {
            result = targetMethod.Invoke(_inner, args);
        }
        catch (TargetInvocationException exc) when (exc.InnerException is { } inner)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(inner).Throw();
            throw;
        }

        return result switch
        {
            Task<CatalogAttachResult> attached => SealAttachAsync(attached, auth),
            Task<TransactionBeginResponse> begun => SealTransactionAsync(begun, auth, sealedAttach ?? []),
            _ => result,
        };
    }

    private async Task<CatalogAttachResult> SealAttachAsync(Task<CatalogAttachResult> pending, AuthContext auth)
    {
        var result = await pending.ConfigureAwait(false);
        result.AttachOpaqueData = _sealer.SealAttach(result.AttachOpaqueData, auth);
        return result;
    }

    private async Task<TransactionBeginResponse> SealTransactionAsync(
        Task<TransactionBeginResponse> pending, AuthContext auth, byte[] sealedAttach)
    {
        var result = await pending.ConfigureAwait(false);
        if (result.TransactionOpaqueData is { } plaintext)
        {
            result.TransactionOpaqueData = _sealer.SealTransaction(plaintext, auth, sealedAttach);
        }

        return result;
    }

    /// <summary>Opens a request object's own opaque properties, and those of its embedded
    /// <c>BindCall</c>.</summary>
    private void OpenRequest(object request, AuthContext auth)
    {
        var (attachProperty, transactionProperty, bindCallProperty) = s_shapes.GetOrAdd(request.GetType(), static type => (
            Property(type, "AttachOpaqueData"),
            Property(type, "TransactionOpaqueData"),
            Property(type, "BindCall")));

        if (attachProperty is not null || transactionProperty is not null)
        {
            var sealedAttach = attachProperty?.GetValue(request) as byte[];
            if (sealedAttach is { Length: > 0 })
            {
                attachProperty!.SetValue(request, _sealer.OpenAttach(sealedAttach, auth));
            }

            if (transactionProperty?.GetValue(request) is byte[] { Length: > 0 } transaction)
            {
                transactionProperty.SetValue(request, _sealer.OpenTransaction(transaction, auth, sealedAttach ?? []));
            }
        }

        if (bindCallProperty?.GetValue(request) is byte[] { Length: > 0 } bindCall)
        {
            var bind = EmbeddedIpc.Decode<BindRequest>(bindCall);
            OpenRequest(bind, auth);
            bindCallProperty.SetValue(request, EmbeddedIpc.Encode(bind));
        }
    }

    private static PropertyInfo? Property(Type type, string name) =>
        type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance) is { PropertyType: var t, CanWrite: true } p && t == typeof(byte[])
            ? p
            : null;
}
