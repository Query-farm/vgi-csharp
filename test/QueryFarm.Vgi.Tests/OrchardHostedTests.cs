using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using QueryFarm.Vgi.ExampleWorker.Hosted;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;
using QueryFarm.VgiRpc.Client.Http;
using QueryFarm.VgiRpc.Http;
using QueryFarm.VgiRpc.Identity;
using QueryFarm.VgiRpc.Server;
using Xunit;

namespace QueryFarm.Vgi.Tests;

/// <summary>
/// Orchard hosted mode (<c>VGI_ORCHARD_HOSTED=1</c>), the port of vgi-python's
/// <c>tests/test_orchard_hosted.py</c>: a real HTTP worker (Kestrel, <see cref="Worker.MapHttp"/>)
/// against a local JWKS holding a freshly generated ES256 key, and every startup refusal. The
/// hosted environment goes through <see cref="Worker.HostedEnvironment"/>, so nothing here sets
/// <c>VGI_ORCHARD_HOSTED</c> for the other tests in the process.
/// </summary>
public sealed class OrchardHostedTests : IAsyncLifetime
{
    private const string Issuer = "https://auth.orchard.test";
    private const string Audience = "https://desk.orchard.test/d/dep_123";
    private const string EcKid = "desk-es256-1";
    private const string RsaKid = "desk-rs256-1";

    private static readonly byte[] s_signingKey = Encoding.UTF8.GetBytes("orchard-hosted-test-signing-key-0001");

    private readonly ECDsa _ec = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    private readonly RSA _rsa = RSA.Create(2048);
    private WebApplication _jwks = null!;
    private string _jwksUri = null!;
    private volatile bool _jwksDown;
    private WebApplication _worker = null!;
    private Uri _url = null!;
    private readonly HttpClient _http = new();
    private TaskCompletionSource<AuthContext> _vgiCaller = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public async ValueTask InitializeAsync()
    {
        var q = _ec.ExportParameters(false).Q;
        var rsa = _rsa.ExportParameters(false);
        var body = JsonSerializer.Serialize(new
        {
            keys = new object[]
            {
                new { kty = "EC", crv = "P-256", x = Base64UrlEncoder.Encode(q.X), y = Base64UrlEncoder.Encode(q.Y), kid = EcKid, use = "sig", alg = "ES256" },
                new { kty = "RSA", n = Base64UrlEncoder.Encode(rsa.Modulus), e = Base64UrlEncoder.Encode(rsa.Exponent), kid = RsaKid, use = "sig", alg = "RS256" },
            },
        });
        var jwksBuilder = WebApplication.CreateSlimBuilder();
        jwksBuilder.Logging.ClearProviders();
        jwksBuilder.WebHost.UseUrls("http://127.0.0.1:0");
        _jwks = jwksBuilder.Build();
        _jwks.MapGet("/jwks", () => _jwksDown
            ? Results.StatusCode(StatusCodes.Status503ServiceUnavailable)
            : Results.Text(body, "application/json"));
        await _jwks.StartAsync();
        _jwksUri = $"{_jwks.Urls.First()}/jwks";

        var worker = HostedWorker(Environment());
        var builder = WebApplication.CreateSlimBuilder(Worker.WorkerHostOptions());
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        _worker = builder.Build();
        _worker.Use(async (HttpContext context, Func<Task> next) =>
        {
            await next();
            if (context.Request.Path.Value?.Contains("/vgi.v3/", StringComparison.Ordinal) == true
                && context.Response.StatusCode == StatusCodes.Status200OK)
            {
                _vgiCaller.TrySetResult(PeerIdentityAuthentication.GetAuth(context));
            }
        });
        worker.MapHttp(_worker);
        await _worker.StartAsync();
        _url = new Uri(_worker.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single());
    }

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _worker.StopAsync();
        await _worker.DisposeAsync();
        await _jwks.StopAsync();
        await _jwks.DisposeAsync();
        _ec.Dispose();
        _rsa.Dispose();
    }

    // ------------------------------------------------------------------ helpers

    private Dictionary<string, string?> Environment(params (string Name, string? Value)[] overrides)
    {
        var environment = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["VGI_ORCHARD_HOSTED"] = "1",
            ["VGI_JWT_ISSUER"] = Issuer,
            ["VGI_JWT_AUDIENCE"] = Audience,
            ["VGI_JWT_JWKS_URI"] = _jwksUri ?? "http://127.0.0.1:9/jwks",
        };
        foreach (var (name, value) in overrides)
        {
            environment[name] = value;
        }

        return environment;
    }

    private static Worker HostedWorker(Dictionary<string, string?> environment, bool signingKey = true)
    {
        var worker = new Worker().CatalogName("test_catalog").DefaultSchema("main");
        worker.HostedEnvironment = name => environment.GetValueOrDefault(name);
        return signingKey ? worker.SigningKey(s_signingKey) : worker;
    }

    private static Dictionary<string, object?> Claims(Action<Dictionary<string, object?>>? change = null)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var claims = new Dictionary<string, object?>
        {
            ["iss"] = Issuer,
            ["aud"] = Audience,
            ["sub"] = "user_01M7KNJYC18T",
            ["kind"] = "user",
            ["email"] = "ada@example.com",
            ["email_verified"] = true,
            ["name"] = "Ada Lovelace",
            ["org_id"] = "org_01M7",
            ["org_name"] = "Acme Analytics",
            ["client_id"] = "vgi-duckdb",
            ["iat"] = now,
            ["nbf"] = now,
            ["exp"] = now + 900,
            ["jti"] = $"jti-{Guid.NewGuid():n}",
        };
        change?.Invoke(claims);
        return claims;
    }

    private string Pass(Dictionary<string, object?>? claims = null, string alg = "ES256", string typ = "at+jwt", string kid = EcKid)
    {
        var header = new Dictionary<string, object?> { ["alg"] = alg, ["typ"] = typ, ["kid"] = kid };
        var signingInput = $"{Segment(header)}.{Segment(claims ?? Claims())}";
        var data = Encoding.ASCII.GetBytes(signingInput);
        var signature = alg switch
        {
            "ES256" => _ec.SignData(data, HashAlgorithmName.SHA256),
            "RS256" => _rsa.SignData(data, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1),
            _ => HMACSHA256.HashData(Encoding.ASCII.GetBytes(new string('k', 32)), data),
        };
        return $"{signingInput}.{Base64UrlEncoder.Encode(signature)}";
    }

    private static string Segment(Dictionary<string, object?> value) =>
        Base64UrlEncoder.Encode(JsonSerializer.SerializeToUtf8Bytes(value));

    private async Task<HttpResponseMessage> Send(HttpMethod method, string path, string? token = null, Dictionary<string, string>? headers = null)
    {
        using var request = new HttpRequestMessage(method, new Uri(_url, path));
        if (method == HttpMethod.Post)
        {
            request.Content = new ByteArrayContent([]);
        }

        if (token is not null)
        {
            request.Headers.TryAddWithoutValidation("Authorization", $"Bearer {token}");
        }

        foreach (var (name, value) in headers ?? [])
        {
            request.Headers.TryAddWithoutValidation(name, value);
        }

        return await _http.SendAsync(request);
    }

    private static string Header(HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values) ? string.Join(",", values)
        : response.Content.Headers.TryGetValues(name, out var content) ? string.Join(",", content) : "";

    private static void AssertUnauthorized(HttpResponseMessage response, string reason = "")
    {
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Equal(OrchardHosted.Challenge, Header(response, "WWW-Authenticate"));
        Assert.DoesNotContain("resource_metadata", Header(response, "WWW-Authenticate"), StringComparison.Ordinal);
        Assert.NotEmpty(Header(response, "VGI-Supported-Encodings"));
        Assert.Equal("true", Header(response, RpcHttpEndpoints.AcceptMaxResponseBytesSupportHeader));
        Assert.Equal("*", Header(response, "Access-Control-Allow-Origin"));
        if (reason.Length > 0)
        {
            Assert.Equal(reason, Header(response, "VGI-Auth-Reason"));
        }
    }

    public interface IVgiClient
    {
        Task<ItemsResponse> CatalogCatalogsAsync();
    }

    // ------------------------------------------------------------------ a running hosted worker

    [Fact]
    public async Task AValidPassReachesTheWorkerWithClaimsInTheirJsonTypes()
    {
        await using var client = new HttpRpcClient(_url, new HttpRpcClientOptions
        {
            Protocol = "vgi.v3",
            ProtocolVersion = Worker.DefaultProtocolVersion,
            DefaultHeaders = new Dictionary<string, string> { ["Authorization"] = $"Bearer {Pass()}" },
        });
        await client.CreateProxy<IVgiClient>().CatalogCatalogsAsync();

        var auth = await _vgiCaller.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal((OrchardHosted.Domain, true, "user_01M7KNJYC18T"), (auth.Domain, auth.Authenticated, auth.Principal));
        Assert.Equal(true, auth.Claims["email_verified"]);
        Assert.IsType<long>(auth.Claims["exp"]);
        Assert.Equal("Acme Analytics", auth.Claims["org_name"]);
    }

    [Fact]
    public async Task HeadHealthIsTheOnlyAnonymousRequest()
    {
        using var response = await Send(HttpMethod.Head, "/health");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotEmpty(Header(response, "VGI-Supported-Encodings"));
    }

    [Theory]
    [InlineData("GET", "/health")]
    [InlineData("OPTIONS", "/health")]
    [InlineData("GET", "/")]
    [InlineData("GET", "/vgi-client.js")]
    [InlineData("POST", "/vgi.v3/catalog_attach")]
    [InlineData("GET", "/no/such/path")]
    public async Task NoPassIsNeverAnonymous(string method, string path)
    {
        using var response = await Send(new HttpMethod(method), path, headers: new() { ["Origin"] = "https://shell.example.com" });
        AssertUnauthorized(response, "missing_credential");
    }

    [Theory]
    [InlineData("garbage")]
    [InlineData("tampered")]
    [InlineData("wrong_aud")]
    [InlineData("multi_aud")]
    [InlineData("expired")]
    [InlineData("wrong_typ")]
    [InlineData("rs256")]
    [InlineData("hs256")]
    [InlineData("wrong_iss")]
    [InlineData("no_exp")]
    public async Task EveryInvalidPassGetsTheSame401(string kind)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var token = kind switch
        {
            "garbage" => "not-a-jwt",
            "tampered" => Tamper(Pass()),
            "wrong_aud" => Pass(Claims(c => c["aud"] = "https://desk.orchard.test/d/dep_other")),
            "multi_aud" => Pass(Claims(c => c["aud"] = new[] { Audience, "https://desk.orchard.test/d/dep_other" })),
            "expired" => Pass(Claims(c =>
            {
                c["iat"] = now - 1200;
                c["nbf"] = now - 1200;
                c["exp"] = now - 120;
            })),
            "wrong_typ" => Pass(typ: "JWT"),
            "rs256" => Pass(alg: "RS256", kid: RsaKid),
            "hs256" => Pass(alg: "HS256"),
            "wrong_iss" => Pass(Claims(c => c["iss"] = "https://evil.test")),
            _ => Pass(Claims(c => c.Remove("exp"))),
        };
        using var response = await Send(HttpMethod.Post, "/vgi.v3/catalog_attach", token, new() { ["Origin"] = "https://shell.example.com" });
        AssertUnauthorized(response, kind == "expired" ? "expired_credential" : "invalid_credential");
    }

    private string Tamper(string token)
    {
        var parts = token.Split('.');
        return $"{parts[0]}.{Segment(Claims(c => c["org_id"] = "org_EVIL"))}.{parts[2]}";
    }

    [Fact]
    public async Task APreflightNeedsNoPassAndCorsIsOpen()
    {
        using var response = await Send(HttpMethod.Options, "/vgi.v3/catalog_attach", headers: new()
        {
            ["Origin"] = "https://shell.example.com",
            ["Access-Control-Request-Method"] = "POST",
            ["Access-Control-Request-Headers"] = "authorization",
        });
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("*", Header(response, "Access-Control-Allow-Origin"));
        Assert.Contains("authorization", Header(response, "Access-Control-Allow-Headers"), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AnUnreachableJwksIs503WithRetryAfterNot401()
    {
        _jwksDown = true;
        using var response = await Send(HttpMethod.Post, "/vgi.v3/catalog_attach", Pass());
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.NotNull(response.Headers.RetryAfter);
    }

    [Fact]
    public void PassClaimsReportsTheCallersPass()
    {
        var auth = new AuthContext(OrchardHosted.Domain, true, "user_01M7KNJYC18T",
            new Dictionary<string, object?> { ["email_verified"] = true, ["exp"] = 1893456000L, ["kind"] = "user", ["other"] = "x" });
        var function = new PassClaimsFunction();
        var input = new RecordBatch(function.ArgumentsSchema, [new Int64Array.Builder().Append(1).Build()], 1);
        var output = function.Process(new ScalarProcessParams { Input = input, OutputSchema = function.OutputSchema, Auth = auth });
        Assert.Equal(
            """{"claims":{"email_verified":true,"exp":1893456000,"kind":"user"},"domain":"orchard","principal":"user_01M7KNJYC18T"}""",
            ((StringArray)output.Column(0)).GetString(0));
    }

    // ------------------------------------------------------------------ startup refusals

    private OrchardHostedConfigurationException Refused(Worker worker, params string[] args) =>
        Assert.Throws<OrchardHostedConfigurationException>(() => worker.ValidateHostedStartup(args.Length == 0 ? ["--http"] : args));

    [Theory]
    [InlineData("VGI_JWT_ISSUER", null)]
    [InlineData("VGI_JWT_AUDIENCE", null)]
    [InlineData("VGI_JWT_JWKS_URI", null)]
    [InlineData("VGI_JWT_ISSUER", "  ")]
    [InlineData("VGI_JWT_AUDIENCE", "")]
    [InlineData("VGI_JWT_JWKS_URI", " ")]
    public void AMissingOrEmptyPassSettingRefuses(string name, string? value) =>
        Assert.Contains(name, Refused(HostedWorker(Environment((name, value)))).Message, StringComparison.Ordinal);

    [Fact]
    public void ACommaSeparatedAudienceRefuses()
    {
        var message = Refused(HostedWorker(Environment(("VGI_JWT_AUDIENCE", $"{Audience},https://desk.orchard.test/d/other")))).Message;
        Assert.Contains("VGI_JWT_AUDIENCE", message, StringComparison.Ordinal);
        Assert.Contains("exactly one", message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSigningKeyMustBeConfiguredAndAtLeast32Bytes()
    {
        Assert.Contains("VGI_SIGNING_KEY", Refused(HostedWorker(Environment(), signingKey: false)).Message, StringComparison.Ordinal);
        var shortKey = HostedWorker(Environment(), signingKey: false).SigningKey(Encoding.UTF8.GetBytes("short-key"));
        Assert.Contains("32 bytes", Refused(shortKey).Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("VGI_BEARER_TOKENS", "tok=alice")]
    [InlineData("VGI_RPC_GRANT_KEYS", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=")]
    [InlineData("VGI_OAUTH_RESOURCE", "https://x.test")]
    [InlineData("VGI_PROXY_PROOF_MODE", "require")]
    [InlineData("VGI_JWT_ALGORITHMS", "ES256,HS256")]
    [InlineData("VGI_JWT_ALGORITHMS", "none")]
    public void AnotherCredentialInTheEnvironmentRefuses(string name, string value) =>
        Assert.Contains(name, Refused(HostedWorker(Environment((name, value)))).Message, StringComparison.Ordinal);

    [Fact]
    public void AnotherCredentialOnTheBuilderRefuses()
    {
        var keys = new GrantKeys([Enumerable.Range(0, 32).Select(i => (byte)i).ToArray()], maxTtlSeconds: 3600);
        Assert.Contains("grant", Refused(HostedWorker(Environment()).SealedGrants(keys)).Message, StringComparison.Ordinal);
        var key = Convert.ToBase64String(Enumerable.Range(0, 32).Select(i => (byte)i).ToArray());
        var flagged = HostedWorker(Environment());
        flagged.ApplyGrantKeyArgs(["--http", "--grant-key", key]);
        Assert.Contains("grant", Refused(flagged).Message, StringComparison.Ordinal);
        Assert.Contains("resolve_token", Refused(HostedWorker(Environment()).Identity(resolveToken: _ => null, introspectPrincipals: ["ops"])).Message, StringComparison.Ordinal);
        Assert.Contains("mint_grant", Refused(HostedWorker(Environment()).Identity(mintGrant: (_, _, _, _) => throw new InvalidOperationException())).Message, StringComparison.Ordinal);
        Assert.Contains("authenticator", Refused(HostedWorker(Environment()).HttpAuthenticate(_ => Task.CompletedTask)).Message, StringComparison.Ordinal);
        Assert.Contains("Iroh", Refused(HostedWorker(Environment()), "--http", "--iroh-issuer", "x").Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("--port", "0")]
    [InlineData("--unix", "/tmp/vgi-orchard-hosted-refuse.sock")]
    [InlineData("--iroh-raw-upstream", "0", "--iroh-issuer", "x")]
    [InlineData("--http", "--unix", "/tmp/x.sock")]
    public void OnlyHttpCarriesAPass(params string[] args)
    {
        var worker = HostedWorker(Environment());
        var exc = Assert.Throws<OrchardHostedConfigurationException>(() => worker.ValidateHostedStartup(args));
        Assert.Contains("only --http", exc.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProgrammaticNonHttpTransportsRefuseToo()
    {
        var worker = HostedWorker(Environment());
        Assert.Throws<OrchardHostedConfigurationException>(() => worker.NewRpcServer(Worker.ServerTransport.Stdio));
        Assert.Throws<OrchardHostedConfigurationException>(() => worker.NewRpcServer(Worker.ServerTransport.Unix));
        Assert.Throws<OrchardHostedConfigurationException>(() => worker.NewRpcServer(Worker.ServerTransport.IrohTcp));
    }

    [Fact]
    public void MapHttpRefusesAConflictWithoutRunFromArgs()
    {
        var builder = WebApplication.CreateSlimBuilder(Worker.WorkerHostOptions());
        using var app = builder.Build();
        var worker = HostedWorker(Environment(("VGI_BEARER_TOKENS", "tok=alice")));
        Assert.Throws<OrchardHostedConfigurationException>(() => worker.MapHttp(app));
    }

    [Fact]
    public void AValidConfigurationPasses() => HostedWorker(Environment()).ValidateHostedStartup(["--http"]);

    [Theory]
    [InlineData("1", true)]
    [InlineData("TRUE", true)]
    [InlineData(" yes ", true)]
    [InlineData("on", true)]
    [InlineData("0", false)]
    [InlineData("false", false)]
    [InlineData("", false)]
    [InlineData("off", false)]
    [InlineData(null, false)]
    public void TheSwitchValues(string? value, bool enabled) =>
        Assert.Equal(enabled, OrchardHosted.IsEnabled(name => name == OrchardHosted.EnvironmentVariable ? value : null));

    [Fact]
    public void AnUnrecognisedSwitchValueRefusesInsteadOfMeaningOff()
    {
        var exc = Assert.Throws<OrchardHostedConfigurationException>(() =>
            OrchardHosted.IsEnabled(name => name == OrchardHosted.EnvironmentVariable ? "yes please" : null));
        Assert.Contains("VGI_ORCHARD_HOSTED", exc.Message, StringComparison.Ordinal);
    }
}
