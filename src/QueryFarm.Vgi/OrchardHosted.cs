using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.Http;
using QueryFarm.VgiRpc.Http.OAuth;
using QueryFarm.VgiRpc.Identity;

namespace QueryFarm.Vgi;

/// <summary>A hosted-mode configuration error: the worker refuses to start.</summary>
/// <remarks><see cref="Worker.RunFromArgsAsync"/> prints <c>Error: </c> and the message to stderr
/// and exits with status 1; the programmatic entry points (<see cref="Worker.RunHttpAsync"/> and
/// the other <c>Run*</c> methods) throw it.</remarks>
public sealed class OrchardHostedConfigurationException(string message) : InvalidOperationException(message)
{
    internal static OrchardHostedConfigurationException Refuse(string reason) =>
        new($"{OrchardHosted.EnvironmentVariable}=1 (Orchard hosted mode): {reason}");
}

/// <summary>The verified hosted-mode settings, read from the environment.</summary>
/// <param name="Issuer">The Front Desk issuer (<c>iss</c>), matched exactly.</param>
/// <param name="Audience">The one canonical Desk Address (<c>aud</c>), matched exactly.</param>
/// <param name="JwksUri">Where the Front Desk publishes its signing keys.</param>
/// <param name="Algorithms">Accepted JWS algorithms (<c>ES256</c> unless overridden).</param>
public sealed record OrchardHostedConfig(string Issuer, string Audience, string JwksUri, IReadOnlyList<string> Algorithms);

/// <summary>
/// Orchard hosted mode (<c>VGI_ORCHARD_HOSTED=1</c>), the port of vgi-python's <c>vgi.hosted</c>.
/// </summary>
/// <remarks>
/// <para>A hosted worker (a <em>Field Worker</em>) serves VGI over HTTP to callers who hold a
/// <em>Pass</em>: a short-lived RFC 9068 access token (<c>typ: at+jwt</c>, ES256) issued by the
/// Orchard Front Desk for exactly one canonical Desk Address. The worker verifies Passes locally
/// against the Front Desk's JWKS and accepts nothing else. Everything here fails closed:</para>
/// <list type="bullet">
/// <item>Startup is refused unless <c>VGI_JWT_ISSUER</c>, <c>VGI_JWT_AUDIENCE</c> and
/// <c>VGI_JWT_JWKS_URI</c> each hold one non-empty value, and <c>VGI_SIGNING_KEY</c> holds a
/// configured key of at least 32 bytes.</item>
/// <item>Startup is refused when any other way in is configured: <c>VGI_BEARER_TOKENS</c>,
/// sealed-grant keys, a <c>resolveToken</c>/<c>mintGrant</c> hook, <c>VGI_OAUTH_RESOURCE</c>, a
/// proxy-proof gate, Iroh peer identity, a custom <see cref="Worker.HttpAuthenticate"/>, or a
/// transport other than HTTP.</item>
/// <item>Every request except <c>HEAD /health</c> and CORS preflights must present a valid Pass,
/// or gets a 401 with <c>WWW-Authenticate: Bearer error="invalid_token"</c>; authentication runs
/// before the request body is read.</item>
/// <item>The JWKS is cached for at most 10 minutes; once that passes and the re-fetch fails,
/// requests get a 503 rather than being checked against stale keys.</item>
/// </list>
/// <para>The design is in the extension repository's <c>docs/location-delegation.md</c>.</para>
/// </remarks>
public static class OrchardHosted
{
    /// <summary>The environment variable that turns hosted mode on; the hosted base image sets it.</summary>
    public const string EnvironmentVariable = "VGI_ORCHARD_HOSTED";

    /// <summary>The challenge on every hosted-mode 401. No <c>resource_metadata</c>: a Field Worker
    /// never sends a client off to discover an authorization server.</summary>
    public const string Challenge = "Bearer error=\"invalid_token\"";

    /// <summary><c>AuthContext.Domain</c> of a Pass.</summary>
    public const string Domain = "orchard";

    /// <summary>The required JOSE <c>typ</c> (RFC 9068).</summary>
    public const string RequiredTyp = "at+jwt";

    /// <summary>The shortest <c>VGI_SIGNING_KEY</c> a hosted worker accepts.</summary>
    public const int MinSigningKeyBytes = 32;

    /// <summary>The default, and only-by-override, Pass signing algorithm.</summary>
    public static IReadOnlyList<string> DefaultAlgorithms { get; } = ["ES256"];

    /// <summary>Clock skew allowed on <c>exp</c>/<c>nbf</c>.</summary>
    public static TimeSpan Leeway { get; } = TimeSpan.FromSeconds(60);

    /// <summary>Hard upper bound on how long JWKS keys are trusted.</summary>
    public static TimeSpan JwksMaxAge { get; } = TimeSpan.FromMinutes(10);

    /// <summary>Minimum time between JWKS re-fetches triggered by an unknown <c>kid</c>.</summary>
    public static TimeSpan JwksRefreshMinInterval { get; } = TimeSpan.FromSeconds(60);

    /// <summary>Timeout for each JWKS request.</summary>
    public static TimeSpan JwksFetchTimeout { get; } = TimeSpan.FromSeconds(10);

    private static readonly HashSet<string> s_true = new(StringComparer.Ordinal) { "1", "true", "yes", "on" };
    private static readonly HashSet<string> s_false = new(StringComparer.Ordinal) { "", "0", "false", "no", "off" };

    /// <summary>Whether <c>VGI_ORCHARD_HOSTED</c> turns hosted mode on.</summary>
    /// <returns>True for <c>1</c>/<c>true</c>/<c>yes</c>/<c>on</c> (any case); false when unset
    /// or <c>0</c>/<c>false</c>/<c>no</c>/<c>off</c>.</returns>
    /// <param name="environment">Reads an environment variable; <see langword="null"/> for the
    /// process environment.</param>
    /// <exception cref="OrchardHostedConfigurationException">Any other value. An unrecognised
    /// value is a configuration error rather than "off", so a typo cannot quietly start a worker
    /// without authentication.</exception>
    public static bool IsEnabled(Func<string, string?>? environment = null)
    {
        var raw = (environment ?? Environment.GetEnvironmentVariable)(EnvironmentVariable);
        if (raw is null)
        {
            return false;
        }

        var value = raw.Trim().ToLowerInvariant();
        if (s_true.Contains(value))
        {
            return true;
        }

        if (s_false.Contains(value))
        {
            return false;
        }

        throw new OrchardHostedConfigurationException(
            $"{EnvironmentVariable} must be 1 or 0 (or true/false), got '{raw}'");
    }

    /// <summary>Reads and validates the hosted-mode environment.</summary>
    /// <param name="environment">Reads an environment variable; <see langword="null"/> for the
    /// process environment.</param>
    /// <exception cref="OrchardHostedConfigurationException">A required variable is missing,
    /// empty or multi-valued, or <c>VGI_JWT_ALGORITHMS</c> is unusable.</exception>
    public static OrchardHostedConfig ResolveConfig(Func<string, string?>? environment = null)
    {
        var read = environment ?? Environment.GetEnvironmentVariable;
        var issuer = SingleValue(read, "VGI_JWT_ISSUER");
        var audience = SingleValue(read, "VGI_JWT_AUDIENCE");
        var jwksUri = SingleValue(read, "VGI_JWT_JWKS_URI");
        var raw = read("VGI_JWT_ALGORITHMS");
        IReadOnlyList<string> algorithms = DefaultAlgorithms;
        if (!string.IsNullOrWhiteSpace(raw))
        {
            var parsed = raw.Split(',').Select(a => a.Trim()).Where(a => a.Length > 0).ToArray();
            if (parsed.Length == 0 || parsed.Any(a =>
                    a.Equals("none", StringComparison.OrdinalIgnoreCase) || a.StartsWith("hs", StringComparison.OrdinalIgnoreCase)))
            {
                throw OrchardHostedConfigurationException.Refuse(
                    $"VGI_JWT_ALGORITHMS='{raw}' is not allowed; Passes are asymmetric (default ES256), never 'none' or HS*.");
            }

            algorithms = parsed;
        }

        return new OrchardHostedConfig(issuer, audience, jwksUri, algorithms);
    }

    private static string SingleValue(Func<string, string?> read, string name)
    {
        var value = (read(name) ?? "").Trim();
        if (value.Length == 0)
        {
            throw OrchardHostedConfigurationException.Refuse($"{name} must be set (unset and empty are both errors).");
        }

        if (value.Contains(',', StringComparison.Ordinal))
        {
            throw OrchardHostedConfigurationException.Refuse(
                $"{name} must be exactly one value; got a comma-separated list. "
                + "A Pass is issued for one canonical Desk Address and one issuer.");
        }

        return value;
    }

    /// <summary>Refuses to start when the environment configures another credential.</summary>
    /// <param name="environment">Reads an environment variable; <see langword="null"/> for the
    /// process environment.</param>
    /// <exception cref="OrchardHostedConfigurationException">On <c>VGI_BEARER_TOKENS</c>,
    /// <c>VGI_RPC_GRANT_KEYS</c>, <c>VGI_OAUTH_RESOURCE</c> or a <c>VGI_PROXY_PROOF_MODE</c> other
    /// than <c>off</c>.</exception>
    /// <remarks>This SDK reads none of the first three or the proxy-proof mode from the environment
    /// itself, but a deployment that sets them expects them to mean something, and in hosted mode
    /// they cannot: refusing says so instead of silently ignoring them.</remarks>
    public static void CheckEnvironmentConflicts(Func<string, string?>? environment = null)
    {
        var read = environment ?? Environment.GetEnvironmentVariable;
        foreach (var (name, what) in new[]
                 {
                     ("VGI_BEARER_TOKENS", "static bearer tokens"),
                     (GrantKeys.KeysEnvironmentVariable, "sealed-grant keys"),
                     ("VGI_OAUTH_RESOURCE", "OAuth resource metadata (a Field Worker never advertises resource_metadata)"),
                 })
        {
            if (!string.IsNullOrWhiteSpace(read(name)))
            {
                throw OrchardHostedConfigurationException.Refuse(
                    $"{name} is set ({what}); the only credential a hosted worker accepts is a Pass.");
            }
        }

        var proofMode = (read("VGI_PROXY_PROOF_MODE") ?? "off").Trim();
        if (proofMode.Length > 0 && !proofMode.Equals("off", StringComparison.OrdinalIgnoreCase))
        {
            throw OrchardHostedConfigurationException.Refuse(
                "VGI_PROXY_PROOF_MODE must be off; hosted workers authenticate by Pass only.");
        }
    }

    /// <summary>Refuses a hosted worker whose signing key is not stable across restarts.</summary>
    /// <param name="signingKey">The <em>configured</em> key (<see cref="Worker.SigningKey"/>, else
    /// <c>VGI_SIGNING_KEY</c> unless <c>VGI_SIGNING_KEY_MINTED=1</c>), or <see langword="null"/>.</param>
    /// <remarks>The key seals catalog opaque data and HTTP state. A Field Worker's machines stop
    /// and start, and a deployment can run on several of them: a key minted per process makes
    /// every restart (and every other machine) reject the sessions DuckDB already holds.</remarks>
    /// <exception cref="OrchardHostedConfigurationException">The key is missing, minted, or
    /// shorter than 32 bytes.</exception>
    public static void CheckSigningKey(byte[]? signingKey)
    {
        if (signingKey is null)
        {
            throw OrchardHostedConfigurationException.Refuse(
                $"{Worker.SigningKeyEnvironmentVariable} is not set; hosted workers need a stable key shared by every machine "
                + "of the deployment, or every restart breaks attached sessions.");
        }

        if (signingKey.Length < MinSigningKeyBytes)
        {
            throw OrchardHostedConfigurationException.Refuse(
                $"{Worker.SigningKeyEnvironmentVariable} must be at least {MinSigningKeyBytes} bytes.");
        }
    }

    /// <summary>Builds the Pass verifier for <paramref name="config"/>.</summary>
    /// <param name="config">Settings from <see cref="ResolveConfig"/>.</param>
    /// <param name="jwksHttpHandler">The handler JWKS requests go through; <see langword="null"/>
    /// for the default.</param>
    /// <returns>An authenticate delegate that publishes an accepted Pass as an
    /// <see cref="QueryFarm.VgiRpc.Server.AuthContext"/> with domain <c>orchard</c>, principal
    /// <c>sub</c> and the Pass's claims in their JSON types.</returns>
    public static RpcHttpEndpoints.AuthenticateDelegate CreateAuthenticate(
        OrchardHostedConfig config, HttpMessageHandler? jwksHttpHandler = null)
    {
        ArgumentNullException.ThrowIfNull(config);
        return JwtAuth.Create(new JwtAuthOptions
        {
            Issuers = [config.Issuer],
            Audiences = [config.Audience],
            JwksUri = config.JwksUri,
            Domain = Domain,
            PrincipalClaim = "sub",
            Algorithms = config.Algorithms,
            RequiredTyp = RequiredTyp,
            RequireExp = true,
            SingleAudience = true,
            Leeway = Leeway,
            JwksMaxAge = JwksMaxAge,
            JwksRefreshMinInterval = JwksRefreshMinInterval,
            JwksFetchTimeout = JwksFetchTimeout,
            JwksHttpHandler = jwksHttpHandler,
        });
    }

    private const string VerifiedItem = "QueryFarm.Vgi.OrchardHosted.Verified";

    /// <summary>The authenticate delegate the VGI endpoints run behind <see cref="UseGate"/>: the
    /// gate has already verified the Pass, so this only insists that it did.</summary>
    internal static Task RequireVerified(HttpContext context) =>
        context.Items.ContainsKey(VerifiedItem)
            ? Task.CompletedTask
            : throw new AuthFailure(AuthReason.MissingCredential, "Missing Authorization header");

    private static readonly string s_exposedHeaders = string.Join(", ", Cors.ExposedHeaders());

    /// <summary>
    /// Puts the Pass check in front of every request: middleware, so it covers every path --
    /// <c>GET /health</c>, the landing page, unknown paths -- not only the mapped VGI routes, and
    /// runs before any endpoint reads, size-checks or decompresses the body.
    /// </summary>
    internal static void UseGate(
        IApplicationBuilder app, string prefix, RpcHttpEndpoints.AuthenticateDelegate verify, bool externalizationEnabled = false)
    {
        var healthPath = prefix.TrimEnd('/') + "/health";
        app.Use(async (HttpContext context, RequestDelegate next) =>
        {
            var request = context.Request;
            var response = context.Response;
            response.OnStarting(() =>
            {
                StampHeaders(response, externalizationEnabled);
                return Task.CompletedTask;
            });

            // A CORS preflight carries no credentials by design; answer it here, openly.
            if (HttpMethods.IsOptions(request.Method)
                && request.Headers.ContainsKey("Origin")
                && request.Headers.ContainsKey("Access-Control-Request-Method"))
            {
                response.StatusCode = StatusCodes.Status200OK;
                response.Headers["Access-Control-Allow-Methods"] = "GET, HEAD, POST, DELETE, OPTIONS";
                var requested = request.Headers["Access-Control-Request-Headers"].ToString();
                response.Headers["Access-Control-Allow-Headers"] = requested.Length > 0 ? requested : "Authorization, Content-Type";
                response.Headers["Access-Control-Max-Age"] = "7200";
                response.ContentLength = 0;
                return;
            }

            // The Front Desk's liveness probe, and the only anonymous request.
            if (HttpMethods.IsHead(request.Method) && string.Equals(request.Path.Value, healthPath, StringComparison.Ordinal))
            {
                await next(context).ConfigureAwait(false);
                return;
            }

            try
            {
                await verify(context).ConfigureAwait(false);
            }
            catch (AuthUnavailableException unavailable)
            {
                // The worker cannot tell whether the Pass is valid, so it does not claim it isn't.
                response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                response.ContentType = "application/json";
                response.Headers.CacheControl = "no-store";
                response.Headers.RetryAfter = unavailable.RetryAfterSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture);
                await response.WriteAsJsonAsync(
                    new { error = "authentication_unavailable", detail = unavailable.Message },
                    context.RequestAborted).ConfigureAwait(false);
                return;
            }
            catch (Exception exc)
            {
                var (reason, detail) = exc is AuthFailure failure
                    ? (failure.Reason, failure.Detail)
                    : (AuthReason.InvalidCredential, "");
                response.Headers.WWWAuthenticate = Challenge;
                await UnauthorizedResponseWriter.WriteAsync(context, reason, detail, proxyHint: null, context.RequestAborted)
                    .ConfigureAwait(false);
                return;
            }

            context.Items[VerifiedItem] = true;
            await next(context).ConfigureAwait(false);
        });
    }

    /// <summary>Open CORS and the VGI capability headers on every response, 401s and 404s
    /// included, so a client holding any answer can read it and tell what the worker supports.</summary>
    private static void StampHeaders(HttpResponse response, bool externalizationEnabled)
    {
        var headers = response.Headers;
        headers["Access-Control-Allow-Origin"] = "*";
        headers["Access-Control-Expose-Headers"] = s_exposedHeaders;
        AddIfAbsent(headers, "VGI-Supported-Encodings", "zstd, gzip");
        AddIfAbsent(headers, RpcHttpEndpoints.AcceptMaxResponseBytesSupportHeader, "true");
        AddIfAbsent(headers, "VGI-Externalization-Enabled", externalizationEnabled ? "true" : "false");
    }

    private static void AddIfAbsent(IHeaderDictionary headers, string name, string value)
    {
        if (!headers.ContainsKey(name))
        {
            headers[name] = value;
        }
    }
}
