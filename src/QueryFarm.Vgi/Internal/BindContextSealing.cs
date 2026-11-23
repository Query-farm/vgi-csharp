using System.Security.Cryptography;
using System.Text;
using QueryFarm.VgiRpc.Identity;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Seals the table-buffering bind context a sink init persists to <see cref="FunctionStorage"/>:
/// the received <c>bind_call</c> bytes, which carry <c>secrets</c> (vgi
/// <c>docs/large-values-3.4-sdk-checklist.md</c>, port check 4; vgi-python seals the same record,
/// <c>sealed_put(BUFFERING_INIT, ...)</c>).
/// </summary>
/// <remarks>
/// Envelope: <c>version(1) || nonce(24) || XChaCha20-Poly1305(bind_call, aad)</c>, the key derived
/// from the worker's secret-options root (the configured key, else on HTTP the signing key, else the
/// host-local key file every pooled process on the host shares), the AAD binding the execution id,
/// so a value cannot be replayed into another execution. A value that does not open is an error,
/// never a plaintext fallback.
/// </remarks>
internal static class BindContextSealing
{
    private const byte Version = 1;
    private const int NonceSize = 24;
    private static readonly byte[] s_keyPrefix = Encoding.ASCII.GetBytes("vgi.buffering_bind_context.v1 key\0");
    private static readonly byte[] s_aadPrefix = Encoding.ASCII.GetBytes("vgi.buffering_bind_context.v1\0");

    public static byte[] Seal(byte[] root, byte[] executionId, byte[] bindCall)
    {
        var nonce = RandomNumberGenerator.GetBytes(NonceSize);
        return [Version, .. nonce, .. XChaCha20Poly1305.Seal(Key(root), nonce, bindCall, Aad(executionId))];
    }

    public static byte[] Open(byte[] root, byte[] executionId, byte[] envelope)
    {
        if (envelope.Length < 1 + NonceSize + XChaCha20Poly1305.TagSize || envelope[0] != Version)
        {
            throw Unreadable();
        }

        try
        {
            return XChaCha20Poly1305.Open(Key(root), envelope.AsSpan(1, NonceSize), envelope.AsSpan(1 + NonceSize), Aad(executionId));
        }
        catch (CryptographicException)
        {
            throw Unreadable();
        }
    }

    private static byte[] Key(byte[] root) => SHA256.HashData([.. s_keyPrefix, .. root]);

    private static byte[] Aad(byte[] executionId) => [.. s_aadPrefix, .. executionId];

    private static InvalidOperationException Unreadable() =>
        new("table-buffering bind context does not open (another worker key, or tampered)");
}
