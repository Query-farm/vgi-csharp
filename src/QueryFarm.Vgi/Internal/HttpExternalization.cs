using System.Globalization;
using QueryFarm.VgiRpc.External;
using QueryFarm.VgiRpc.Gcs;
using QueryFarm.VgiRpc.Http;
using QueryFarm.VgiRpc.S3;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The HTTP server's externalization, from the environment: the storage backend
/// (<c>VGI_STORAGE_BACKEND</c>: the blob vendor, the operator's S3 or GCS bucket, or none), with
/// vgi-rpc's blob-vendor wiring
/// (<see cref="ExternalizationOptions.FromEnvironment"/>: results over
/// <c>VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES</c> become vendor pointers, <c>__upload_url__</c> grants
/// come from the vendor, <c>VGI_RPC_MAX_REQUEST_BYTES</c> / <c>VGI_RPC_MAX_UPLOAD_BYTES</c> are
/// advertised) plus VGI's argument-reference capability (vgi <c>docs/large-values-3.4.md</c> §5).
/// </summary>
internal static class HttpExternalization
{
    /// <summary>The capability header that tells the client this worker accepts argument
    /// references, and above how many bytes of arguments IPC to use one.</summary>
    public const string ArgumentsRefBytesHeader = "VGI-Arguments-Ref-Bytes";

    /// <summary>Operator override of the advertised value.</summary>
    public const string ArgumentsRefBytesVariable = "VGI_ARGUMENTS_REF_BYTES";

    /// <summary>Test switch: stop sending the header (behave like a worker without argument
    /// references while still offering upload URLs v2).</summary>
    public const string DisableArgumentsRefVariable = "VGI_TEST_DISABLE_ARGUMENTS_REF";

    /// <summary>The default advertised value: 4 MiB.</summary>
    public const long DefaultArgumentsRefBytes = 4L * 1024 * 1024;

    /// <summary>The hosted-mode request cap when <c>VGI_RPC_MAX_REQUEST_BYTES</c> is unset: 32 MiB
    /// (vgi-python <c>HOSTED_DEFAULT_MAX_REQUEST_BYTES</c>).</summary>
    public const long HostedDefaultMaxRequestBytes = 32L * 1024 * 1024;

    /// <summary>Maximum stream duration (the 3.4 <c>token_ttl</c>, 6 h), passed explicitly.</summary>
    public static readonly TimeSpan TokenTtl = TimeSpan.FromSeconds(21600);

    /// <summary>
    /// The options to serve with, or <see langword="null"/> when the environment configures none.
    /// The argument-reference header is the mandatory capability signal: sent whenever the server
    /// vends upload URLs v2 and can fetch what a grant mints back by id (an object store), and never
    /// otherwise.
    /// </summary>
    /// <exception cref="ArgumentException">A malformed or contradictory value: the worker refuses to start.</exception>
    /// <param name="environment">Variable lookup; defaults to the process environment.</param>
    /// <param name="hosted">Orchard hosted mode: <c>VGI_RPC_MAX_REQUEST_BYTES</c> defaults to
    /// <see cref="HostedDefaultMaxRequestBytes"/>, as vgi-python's hosted mode does.</param>
    public static ExternalizationOptions? FromEnvironment(Func<string, string?>? environment = null, bool hosted = false)
    {
        var read = environment ?? Environment.GetEnvironmentVariable;
        Func<string, string?> env = hosted
            ? name => name == QueryFarm.VgiRpc.External.BlobVendorEnvironment.MaxRequestBytes && string.IsNullOrWhiteSpace(read(name))
                ? HostedDefaultMaxRequestBytes.ToString(CultureInfo.InvariantCulture)
                : read(name)
            : read;
        // Direct-storage contract v1: VGI_STORAGE_BACKEND (or the vendor / bucket variables) picks
        // the blob vendor, the operator's S3 or GCS bucket, or nothing.
        var backend = StorageBackendEnvironment.Select(env);
        if (hosted && backend is StorageBackend.S3 or StorageBackend.Gcs)
        {
            Console.Error.WriteLine(
                $"vgi: Orchard hosted mode with {StorageBackendEnvironment.Backend}={backend.ToString().ToLowerInvariant()}: externalized values go to the operator's bucket, not the Orchard blob vendor");
        }

        var options = backend switch
        {
            StorageBackend.S3 => S3StorageEnvironment.ExternalizationFromEnvironment(env),
            StorageBackend.Gcs => GcsStorageEnvironment.ExternalizationFromEnvironment(env),
            _ => ExternalizationOptions.FromEnvironment(env),
        };
        if (options?.UploadUrlProvider is null || options.EffectiveObjectStore is null
            || (env(DisableArgumentsRefVariable)?.Trim().ToLowerInvariant() is "1" or "true" or "yes"))
        {
            return options;
        }

        return options.WithCapabilityHeaders(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [ArgumentsRefBytesHeader] = ArgumentsRefBytes(env).ToString(CultureInfo.InvariantCulture),
        });
    }

    /// <summary><c>VGI_ARGUMENTS_REF_BYTES</c> when set, else 4 MiB.</summary>
    /// <exception cref="ArgumentException">Not a positive decimal integer of at most 19 digits with
    /// no sign or leading zero (the header's grammar), so a typo stops startup instead of silently
    /// advertising nothing.</exception>
    public static long ArgumentsRefBytes(Func<string, string?>? environment = null)
    {
        var raw = (environment ?? Environment.GetEnvironmentVariable)(ArgumentsRefBytesVariable)?.Trim();
        if (string.IsNullOrEmpty(raw))
        {
            return DefaultArgumentsRefBytes;
        }

        return raw.Length <= 19 && raw[0] != '0' && raw.All(char.IsAsciiDigit)
            && long.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new ArgumentException(
                $"{ArgumentsRefBytesVariable} must be a positive integer number of bytes (no sign, no leading zero, at most 19 digits), got '{raw}'");
    }
}
