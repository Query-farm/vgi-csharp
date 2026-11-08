using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using QueryFarm.VgiRpc.Identity;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Encrypts the values of <c>secret</c> ATTACH options inside <c>attach_opaque_data</c> so the
/// catalog's functions and transaction hooks can read them back (vgi SPEC-secret-options: the
/// sealing spec's rule 5 forbids a secret in plaintext in any opaque value, on every transport).
/// </summary>
/// <remarks>
/// <para><b>Key.</b> <c>K = HKDF-SHA256(root, salt = empty, info = "vgi.attach_secret_options.v1", 32)</c>;
/// the root is the worker's configured secret-options key, else its HTTP signing/sealing key (on
/// HTTP), else the host-local key file (<see cref="HostLocalKey"/>). The root is never used as the
/// AEAD key itself.</para>
/// <para><b>Blob.</b> <c>version(1) || key id(8) || nonce(24) || XChaCha20-Poly1305(ciphertext || tag)</c>,
/// AAD <c>"vgi.attach_secret_options.v1" 0x00 || attach id</c> (the attach's 16 random bytes), so a
/// blob moved into another attach value does not open. The key id is a SHA-256-derived fingerprint
/// of K that selects among the keys this process knows; it reveals nothing about K.</para>
/// <para>Every failure to open is the sealing spec's uniform <c>attach_opaque_data not recognized</c>.
/// Nothing here logs.</para>
/// </remarks>
internal static class AttachSecretOptions
{
    internal const string Info = "vgi.attach_secret_options.v1";

    /// <summary>The blob's version byte: XChaCha20-Poly1305 with a 24-byte random nonce.</summary>
    internal const byte Version = 1;

    private const int KeyIdSize = 8;
    private const int NonceSize = 24;

    private static readonly byte[] s_aadPrefix = [.. Encoding.ASCII.GetBytes(Info), 0];
    private static readonly byte[] s_keyIdPrefix = Encoding.ASCII.GetBytes("vgi.attach_secret_options.v1 key id\0");

    /// <summary>Derived keys this process can open with, by key id.</summary>
    private static readonly ConcurrentDictionary<string, byte[]> s_keys = new(StringComparer.Ordinal);

    /// <summary>The AEAD key for <paramref name="root"/> (HKDF-SHA256, empty salt, the spec's info).</summary>
    internal static byte[] DeriveKey(byte[] root)
    {
        ArgumentNullException.ThrowIfNull(root);
        if (root.Length == 0)
        {
            throw new ArgumentException("The secret-options root key is empty.", nameof(root));
        }

        return HKDF.DeriveKey(HashAlgorithmName.SHA256, root, 32, salt: [], info: Encoding.ASCII.GetBytes(Info));
    }

    private static byte[] KeyId(byte[] key) => SHA256.HashData([.. s_keyIdPrefix, .. key])[..KeyIdSize];

    /// <summary>Makes <paramref name="root"/>'s derived key available for opening in this process
    /// (an HTTP replica that never attached must still open a value another replica minted with the
    /// same signing key) and returns it.</summary>
    internal static byte[] Register(byte[] root)
    {
        var key = DeriveKey(root);
        s_keys[Convert.ToHexString(KeyId(key))] = key;
        return key;
    }

    internal static byte[] Aad(byte[] attachId) => [.. s_aadPrefix, .. attachId];

    /// <summary>Encrypts <paramref name="plaintext"/> for the attach whose id is
    /// <paramref name="attachId"/>, under <paramref name="root"/>'s derived key.</summary>
    internal static byte[] Encrypt(byte[] root, byte[] attachId, byte[] plaintext)
    {
        var key = Register(root);
        var nonce = RandomNumberGenerator.GetBytes(NonceSize);
        var body = XChaCha20Poly1305.Seal(key, nonce, plaintext, Aad(attachId));
        return [Version, .. KeyId(key), .. nonce, .. body];
    }

    /// <summary>Opens a blob <see cref="Encrypt"/> made for the attach <paramref name="attachId"/>.</summary>
    /// <exception cref="ValueError"><c>attach_opaque_data not recognized</c>, whatever failed.</exception>
    internal static byte[] Decrypt(byte[] blob, byte[] attachId)
    {
        if (blob.Length < 1 + KeyIdSize + NonceSize + XChaCha20Poly1305.TagSize || blob[0] != Version)
        {
            throw Rejected();
        }

        var keyId = Convert.ToHexString(blob, 1, KeyIdSize);
        if (!s_keys.TryGetValue(keyId, out var key))
        {
            // The host-local key is loaded lazily: a stdio/launch process that did not mint this
            // value (a different pool member) still shares the host's key file.
            try
            {
                Register(HostLocalKey.Load());
            }
            catch (Exception exc) when (exc is IOException or UnauthorizedAccessException or InvalidOperationException)
            {
                throw Rejected();
            }

            if (!s_keys.TryGetValue(keyId, out key))
            {
                throw Rejected();
            }
        }

        try
        {
            return XChaCha20Poly1305.Open(
                key, blob.AsSpan(1 + KeyIdSize, NonceSize), blob.AsSpan(1 + KeyIdSize + NonceSize), Aad(attachId));
        }
        catch (CryptographicException)
        {
            throw Rejected();
        }
    }

    private static ValueError Rejected() => OpaqueSealer.Rejected(OpaqueSealer.AttachField);
}

/// <summary>
/// The host-local secret-options root key, shared by every VGI worker of one user on one host
/// (whatever its SDK): 32 random bytes in a file only that user can read. Used on the OS-owned
/// transports (stdio, unix sockets, launcher pools), where the worker has no signing key.
/// </summary>
/// <remarks>
/// Path: <c>$VGI_ATTACH_SECRET_KEY_FILE</c> when set; else
/// <c>$XDG_RUNTIME_DIR/vgi/attach-secret-options.key</c> when that directory exists; else
/// <c>&lt;temp&gt;/vgi-&lt;user&gt;/attach-secret-options.key</c>. Created on first use (directory 0700,
/// file 0600, atomically: a temp file linked into place, never overwriting an existing key); a
/// file of any other length, or one (or a directory) the group or others can access, or owned by
/// another user, is refused.
/// </remarks>
internal static class HostLocalKey
{
    internal const string FileVariable = "VGI_ATTACH_SECRET_KEY_FILE";
    internal const string FileName = "attach-secret-options.key";
    internal const int Size = 32;

    private const UnixFileMode PrivateFile = UnixFileMode.UserRead | UnixFileMode.UserWrite;
    private const UnixFileMode PrivateDirectory = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute;
    private const UnixFileMode GroupOrOther =
        UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute
        | UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;

    private static readonly object s_gate = new();
    private static (string Path, byte[] Key)? s_cached;

    /// <summary>The key file's path, per the rules above.</summary>
    internal static string ResolvePath()
    {
        if (Environment.GetEnvironmentVariable(FileVariable) is { Length: > 0 } explicitPath)
        {
            return Path.GetFullPath(explicitPath);
        }

        if (Environment.GetEnvironmentVariable("XDG_RUNTIME_DIR") is { Length: > 0 } runtime && Directory.Exists(runtime))
        {
            return Path.Combine(runtime, "vgi", FileName);
        }

        return Path.Combine(Path.GetTempPath(), $"vgi-{Environment.UserName}", FileName);
    }

    /// <summary>Reads the key, creating it if it does not exist yet.</summary>
    /// <exception cref="InvalidOperationException">The file or its directory is unusable; the
    /// message names it and the alternatives.</exception>
    internal static byte[] Load()
    {
        var path = ResolvePath();
        lock (s_gate)
        {
            if (s_cached is { } cached && cached.Path == path)
            {
                return cached.Key;
            }

            var key = LoadOrCreate(path);
            s_cached = (path, key);
            return key;
        }
    }

    /// <summary>Forgets the cached key (tests change <c>VGI_ATTACH_SECRET_KEY_FILE</c>).</summary>
    internal static void ResetCache()
    {
        lock (s_gate)
        {
            s_cached = null;
        }
    }

    private static InvalidOperationException Unusable(string path, string why) => new(
        $"The host-local secret-options key '{path}' {why}. Fix or remove it, or configure a key: set "
        + $"VGI_SIGNING_KEY, point {FileVariable} at a private 32-byte key file, or call Worker.AttachSecretOptionsKey.");

    internal static byte[] LoadOrCreate(string path)
    {
        var directory = Path.GetDirectoryName(path)!;
        if (!Directory.Exists(directory))
        {
            if (OperatingSystem.IsWindows())
            {
                Directory.CreateDirectory(directory);
            }
            else
            {
                Directory.CreateDirectory(directory, PrivateDirectory);
            }
        }

        CheckPrivate(directory, isDirectory: true, path);
        if (!File.Exists(path))
        {
            Create(path, directory);
        }

        CheckPrivate(path, isDirectory: false, path);
        var bytes = File.ReadAllBytes(path);
        if (bytes.Length != Size)
        {
            throw Unusable(path, $"holds {bytes.Length} bytes, not exactly {Size} raw random bytes");
        }

        return bytes;
    }

    private static void Create(string path, string directory)
    {
        var temp = Path.Combine(directory, $".{FileName}.{Environment.ProcessId}.{Guid.NewGuid():N}.tmp");
        var options = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode = PrivateFile;
        }

        try
        {
            using (var stream = new FileStream(temp, options))
            {
                stream.Write(RandomNumberGenerator.GetBytes(Size));
                stream.Flush(flushToDisk: true);
            }

            // link(2) the temp file into place: it fails when the key already exists, so two first
            // starts racing here agree on whichever key landed first (File.Move would rename over
            // it on Unix). Windows' no-overwrite move is atomic.
            if (OperatingSystem.IsWindows())
            {
                File.Move(temp, path, overwrite: false);
            }
            else if (NativeIds.Link(temp, path) != 0 && !File.Exists(path))
            {
                throw Unusable(path, "could not be created");
            }
        }
        catch (IOException) when (File.Exists(path))
        {
            // Another process created it first: use theirs.
        }
        finally
        {
            try
            {
                File.Delete(temp);
            }
            catch (IOException)
            {
                // already moved or gone
            }
        }
    }

    private static void CheckPrivate(string target, bool isDirectory, string keyPath)
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        var mode = File.GetUnixFileMode(target);
        if ((mode & GroupOrOther) != 0)
        {
            throw Unusable(keyPath, $"{(isDirectory ? "is in a directory" : "is a file")} the group or others can access (mode {Convert.ToString((int)mode, 8)}"
                + (isDirectory ? $" on '{target}')" : ")"));
        }

        if (Owner(target) is { } owner && NativeIds.EffectiveUserId() is { } self && owner != self)
        {
            throw Unusable(keyPath, $"{(isDirectory ? "is in a directory" : "is a file")} owned by another user (uid {owner})");
        }
    }

    private static uint? Owner(string path)
    {
        try
        {
            return NativeIds.Stat(path, out var status) == 0 ? status.Uid : null;
        }
        catch (Exception exc) when (exc is DllNotFoundException or EntryPointNotFoundException)
        {
            return null;
        }
    }

    /// <summary>The .NET runtime's own native shim (shipped with every runtime) — its normalized
    /// <c>stat</c> avoids per-platform <c>struct stat</c> layouts.</summary>
    private static class NativeIds
    {
        [StructLayout(LayoutKind.Sequential)]
        internal struct FileStatus
        {
            internal int Flags;
            internal int Mode;
            internal uint Uid;
            internal uint Gid;
            internal long Size;
            internal long ATime;
            internal long ATimeNsec;
            internal long MTime;
            internal long MTimeNsec;
            internal long CTime;
            internal long CTimeNsec;
            internal long BirthTime;
            internal long BirthTimeNsec;
            internal long Dev;
            internal long RDev;
            internal long Ino;
            internal uint UserFlags;
        }

        [DllImport("libSystem.Native", EntryPoint = "SystemNative_Stat", CharSet = CharSet.Ansi, SetLastError = true)]
        internal static extern int Stat(string path, out FileStatus output);

        [DllImport("libSystem.Native", EntryPoint = "SystemNative_Link", CharSet = CharSet.Ansi, SetLastError = true)]
        internal static extern int Link(string source, string target);

        [DllImport("libSystem.Native", EntryPoint = "SystemNative_GetEUid")]
        private static extern uint GetEUid();

        internal static uint? EffectiveUserId()
        {
            try
            {
                return GetEUid();
            }
            catch (Exception exc) when (exc is DllNotFoundException or EntryPointNotFoundException)
            {
                return null;
            }
        }
    }
}
