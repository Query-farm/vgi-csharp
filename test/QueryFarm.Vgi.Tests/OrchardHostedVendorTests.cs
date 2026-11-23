using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using QueryFarm.Vgi.ExampleWorker.BlobTest;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Tests.Arguments;
using QueryFarm.VgiRpc.Client.Http;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.External;
using Xunit;

namespace QueryFarm.Vgi.Tests;

/// <summary>
/// Orchard hosted mode with the blob vendor (vgi-python's hosted mode reads the same
/// <c>VGI_BLOB_VENDOR_URL</c> environment): upload grants carry <c>object_id</c> and a <c>ref</c>
/// sealed for the Pass's principal, a bind that presents the ref resolves for that principal and
/// for no other, and a result over the threshold is externalized. Runs against vgi-rpc-python's
/// fake vendor; set <c>VGI_RPC_FAKE_VENDOR_BIN</c> to its <c>vgi-rpc-fake-vendor</c> executable
/// (CI's vendor lane does).
/// </summary>
public sealed class OrchardHostedVendorTests
{
    private const string Issuer = "https://auth.orchard.test";
    private const string Audience = "https://desk.orchard.test/d/dep_123";
    private const string Kid = "desk-es256-1";
    private const string VendorToken = "test-token";

    private readonly ECDsa _ec = ECDsa.Create(ECCurve.NamedCurves.nistP256);

    public interface IHostedVgi
    {
        Task<BindResponse> BindAsync(BindRequest request);

        Task<TableFunctionPlanResult> TableFunctionPlanAsync(TableFunctionPlanRequest request);
    }

    [Fact]
    public async Task GrantsBindToThePassPrincipalAndLargeResultsAreExternalized()
    {
        await using var vendor = await FakeVendor.StartAsync();
        await using var jwks = await StartJwksAsync();
        var environment = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["VGI_ORCHARD_HOSTED"] = "1",
            ["VGI_JWT_ISSUER"] = Issuer,
            ["VGI_JWT_AUDIENCE"] = Audience,
            ["VGI_JWT_JWKS_URI"] = $"{jwks.Urls.First()}/jwks",
            [BlobVendorEnvironment.Url] = vendor.Url,
            [BlobVendorEnvironment.Token] = VendorToken,
            [BlobVendorEnvironment.ExternalizeThreshold] = "4096",
        };
        var worker = new Worker().CatalogName("test_catalog").DefaultSchema("main")
            .SigningKey(Encoding.UTF8.GetBytes("orchard-hosted-test-signing-key-0001"))
            .RegisterTable(new BlobParamStreamsFunction());
        worker.HostedEnvironment = name => environment.GetValueOrDefault(name);
        var builder = WebApplication.CreateSlimBuilder(Worker.WorkerHostOptions());
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        await using var app = builder.Build();
        worker.MapHttp(app);
        await app.StartAsync(TestContext.Current.CancellationToken);
        var url = new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single());
        var alice = Pass("user_alice");

        // The capability headers, behind the Pass; a 401 tells the truth about externalization too.
        using (var http = new HttpClient())
        {
            using var options = new HttpRequestMessage(HttpMethod.Options, new Uri(url, "/health"));
            options.Headers.TryAddWithoutValidation("Authorization", $"Bearer {alice}");
            using var caps = await http.SendAsync(options, TestContext.Current.CancellationToken);
            Assert.Equal("2", Header(caps, "VGI-Upload-URL-Support"));
            Assert.Equal("4194304", Header(caps, "VGI-Arguments-Ref-Bytes"));
            Assert.Equal("true", Header(caps, "VGI-Externalization-Enabled"));
            Assert.Equal((32L * 1024 * 1024).ToString(System.Globalization.CultureInfo.InvariantCulture), Header(caps, "VGI-Max-Request-Bytes"));
            using var anonymous = await http.SendAsync(new HttpRequestMessage(HttpMethod.Options, new Uri(url, "/health")), TestContext.Current.CancellationToken);
            Assert.Equal(HttpStatusCode.Unauthorized, anonymous.StatusCode);
            Assert.Equal("true", Header(anonymous, "VGI-Externalization-Enabled"));
        }

        await using var aliceClient = Client(url, alice);
        await using var bobClient = Client(url, Pass("user_bob"));

        // A grant: object_id and ref; the arguments go up to the vendor's signed URL.
        var arguments = PublicArgumentApiTests.ArgsBytes(
            ("positional_0", PublicArgumentApiTests.Str(new string('p', 5000))), ("positional_1", PublicArgumentApiTests.Long(400)));
        var sha = Convert.ToHexStringLower(SHA256.HashData(arguments));
        var grant = Assert.Single(await aliceClient.RequestUploadUrlsAsync(
            [new UploadRequest(arguments.Length, sha)], TestContext.Current.CancellationToken));
        Assert.NotEmpty(grant.ObjectId);
        Assert.NotEmpty(grant.Ref);
        using (var put = new HttpClient())
        {
            using var request = new HttpRequestMessage(HttpMethod.Put, grant.Upload) { Content = new ByteArrayContent(arguments) };
            foreach (var (name, value) in grant.Headers)
            {
                if (!request.Headers.TryAddWithoutValidation(name, value))
                {
                    request.Content.Headers.TryAddWithoutValidation(name, value);
                }
            }

            Assert.True((await put.SendAsync(request, TestContext.Current.CancellationToken)).IsSuccessStatusCode);
        }

        var bind = new BindRequest
        {
            FunctionName = "blob_param_streams",
            FunctionType = FunctionType.Table,
            ArgumentsLocation = new ArgumentsLocation { Ref = grant.Ref, Size = arguments.Length, Sha256 = sha },
        };

        // The ref opens for the principal it was granted to, and for no other.
        await aliceClient.CreateProxy<IHostedVgi>().BindAsync(bind);
        var refused = await Assert.ThrowsAnyAsync<Exception>(() => bobClient.CreateProxy<IHostedVgi>().BindAsync(bind));
        Assert.Contains("invalid_arguments_ref", refused.Message, StringComparison.Ordinal);

        // A plan of 400 splits is far over the 4096-byte threshold: a vendor pointer the client
        // fetches, built from the arguments resolved through the reference.
        var before = await vendor.StatsAsync();
        var plan = await aliceClient.CreateProxy<IHostedVgi>().TableFunctionPlanAsync(
            new TableFunctionPlanRequest { BindCall = Nested<BindRequest>.Of(bind) });
        Assert.Equal(400, plan.Splits.Count);
        var after = await vendor.StatsAsync();
        Assert.True(after["puts_rpc"] > before["puts_rpc"], "the plan result was not externalized");
        Assert.True(after["gets_upload"] >= 1, "the argument object was never fetched");
    }

    private static HttpRpcClient Client(Uri url, string pass) => new(url, new HttpRpcClientOptions
    {
        Protocol = "vgi.v3",
        ProtocolVersion = Worker.DefaultProtocolVersion,
        DefaultHeaders = new Dictionary<string, string> { ["Authorization"] = $"Bearer {pass}" },
        ExternalLocation = new ClientExternalConfig { UrlValidator = null },
    });

    private static string Header(HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values) ? string.Join(",", values) : "";

    private async Task<WebApplication> StartJwksAsync()
    {
        var q = _ec.ExportParameters(false).Q;
        var body = JsonSerializer.Serialize(new
        {
            keys = new object[] { new { kty = "EC", crv = "P-256", x = Base64UrlEncoder.Encode(q.X), y = Base64UrlEncoder.Encode(q.Y), kid = Kid, use = "sig", alg = "ES256" } },
        });
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        var jwks = builder.Build();
        jwks.MapGet("/jwks", () => Results.Text(body, "application/json"));
        await jwks.StartAsync();
        return jwks;
    }

    private string Pass(string subject)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var header = new Dictionary<string, object?> { ["alg"] = "ES256", ["typ"] = "at+jwt", ["kid"] = Kid };
        var claims = new Dictionary<string, object?>
        {
            ["iss"] = Issuer,
            ["aud"] = Audience,
            ["sub"] = subject,
            ["iat"] = now,
            ["nbf"] = now,
            ["exp"] = now + 900,
            ["jti"] = $"jti-{Guid.NewGuid():n}",
        };
        var signingInput = $"{Segment(header)}.{Segment(claims)}";
        return $"{signingInput}.{Base64UrlEncoder.Encode(_ec.SignData(Encoding.ASCII.GetBytes(signingInput), HashAlgorithmName.SHA256))}";
    }

    private static string Segment(Dictionary<string, object?> value) =>
        Base64UrlEncoder.Encode(JsonSerializer.SerializeToUtf8Bytes(value));

    /// <summary>vgi-rpc-python's <c>vgi-rpc-fake-vendor --test-routes</c>.</summary>
    private sealed class FakeVendor : IAsyncDisposable
    {
        private readonly Process _process;
        private readonly HttpClient _http = new();

        private FakeVendor(Process process, string url)
        {
            _process = process;
            Url = url;
        }

        public string Url { get; }

        public static async Task<FakeVendor> StartAsync()
        {
            var bin = System.Environment.GetEnvironmentVariable("VGI_RPC_FAKE_VENDOR_BIN");
            if (string.IsNullOrEmpty(bin))
            {
                Assert.Skip("Set VGI_RPC_FAKE_VENDOR_BIN to vgi-rpc-python's vgi-rpc-fake-vendor to run the hosted blob-vendor test.");
            }

            var start = new ProcessStartInfo(bin) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
            foreach (var arg in new[] { "--test-routes", "--token", $"{VendorToken}=tenant-a", "--port", "0" })
            {
                start.ArgumentList.Add(arg);
            }

            var process = Process.Start(start)!;
            process.ErrorDataReceived += (_, _) => { };
            process.BeginErrorReadLine();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            while (await process.StandardOutput.ReadLineAsync(timeout.Token) is { } line)
            {
                if (line.StartsWith("URL:", StringComparison.Ordinal))
                {
                    return new FakeVendor(process, line[4..].Trim());
                }
            }

            process.Kill();
            throw new InvalidOperationException("the fake vendor exited before printing URL:");
        }

        public async Task<Dictionary<string, long>> StatsAsync()
        {
            using var doc = JsonDocument.Parse(await _http.GetStringAsync($"{Url}/__stats__", TestContext.Current.CancellationToken));
            return doc.RootElement.EnumerateObject().Where(p => p.Value.ValueKind == JsonValueKind.Number).ToDictionary(p => p.Name, p => p.Value.GetInt64());
        }

        public async ValueTask DisposeAsync()
        {
            _http.Dispose();
            try
            {
                _process.Kill(entireProcessTree: true);
                await _process.WaitForExitAsync();
            }
            catch (InvalidOperationException)
            {
            }

            _process.Dispose();
        }
    }
}
