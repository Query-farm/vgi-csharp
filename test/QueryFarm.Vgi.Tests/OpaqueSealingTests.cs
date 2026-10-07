using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Attributes;
using QueryFarm.VgiRpc.Client.Http;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.Http;
using QueryFarm.VgiRpc.Server;
using Xunit;

namespace QueryFarm.Vgi.Tests;

/// <summary>Client-held values (<c>attach_opaque_data</c>, <c>transaction_opaque_data</c>) are
/// sealed over HTTP, bound to the caller and to the parent attach, rejected uniformly, never
/// plaintext with a secret, and never logged (vgi-python
/// <c>docs/protocol/vgi-opaque-data-sealing.md</c>).</summary>
[Collection("environment")]
public sealed class OpaqueSealingTests
{
    private const string PrincipalHeader = "X-Test-Principal";
    private const string Secret = "sk-sealing-secret-0123456789";

    [ProtocolName("vgi.v2")]
    public interface IVgiView
    {
        Task<CatalogAttachResult> CatalogAttachAsync(CatalogAttachRequest request);

        Task<ItemsResponse> CatalogSchemasAsync(byte[] attachOpaqueData, byte[]? transactionOpaqueData);

        Task<TransactionBeginResponse> CatalogTransactionBeginAsync(byte[] attachOpaqueData);

        Task CatalogTransactionCommitAsync(byte[] attachOpaqueData, byte[] transactionOpaqueData);
    }

    /// <summary><c>example</c> (transactional) and <c>vault</c>, whose required <c>api_key</c> is
    /// secret.</summary>
    private static Worker NewWorker() => new Worker()
        .CatalogName("example")
        .DefaultSchema("main")
        .RegisterCatalog(new CatalogInfo { Name = "example" })
        .RegisterCatalog(new CatalogInfo
        {
            Name = "vault",
            AttachOptionSpecs =
            [
                EmbeddedIpc.Encode(AttachOptionSpecBuilder.Build("api_key", "API key", StringType.Default, null, required: true, secret: true)),
            ],
        }, exclusive: true)
        .HttpAuthenticate(context =>
        {
            var principal = context.Request.Headers[PrincipalHeader].ToString();
            if (principal.Length > 0)
            {
                PeerIdentityAuthentication.SetAuth(context, new AuthContext("test", true, principal));
            }

            return Task.CompletedTask;
        });

    private static byte[] Options(string name, string value)
    {
        using var batch = new RecordBatch(
            new Schema([new Field(name, StringType.Default, true)], null),
            [new StringArray.Builder().Append(value).Build()], 1);
        return RecordBatchIpc.Write(batch);
    }

    private sealed class Server : IAsyncDisposable
    {
        private readonly WebApplication _app;

        private Server(WebApplication app, Uri address)
        {
            _app = app;
            Address = address;
        }

        public Uri Address { get; }

        public static async Task<Server> StartAsync(Worker worker)
        {
            var builder = WebApplication.CreateSlimBuilder(Worker.WorkerHostOptions());
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            var app = builder.Build();
            worker.MapHttp(app);
            await app.StartAsync(TestContext.Current.CancellationToken);
            return new Server(app, new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single()));
        }

        public HttpRpcClient Client(string? principal)
        {
            var headers = new Dictionary<string, string>();
            if (principal is not null)
            {
                headers[PrincipalHeader] = principal;
            }

            return new HttpRpcClient(Address, new HttpRpcClientOptions
            {
                Protocol = "vgi.v2",
                ProtocolVersion = Worker.DefaultProtocolVersion,
                DefaultHeaders = headers,
            });
        }

        public async ValueTask DisposeAsync()
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
    }

    private static (string, string, string, string?) Shape(RpcException error) =>
        (error.ErrorType, error.ErrorMessage, error.ErrorCode, error.ErrorKind);

    [Fact]
    public async Task OverHttp_ValuesAreSealedBoundAndRejectedUniformly()
    {
        await using var server = await Server.StartAsync(NewWorker());
        await using var aliceClient = server.Client("alice");
        await using var bobClient = server.Client("bob");
        await using var anonymousClient = server.Client(null);
        var alice = aliceClient.CreateProxy<IVgiView>();
        var bob = bobClient.CreateProxy<IVgiView>();

        var attached = (await alice.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData;
        // Sealed: not the plaintext shape <identity>\0<id>.
        Assert.Equal(OpaqueSealer.AttachVersion, attached[0]);
        Assert.False(attached.AsSpan().StartsWith("example\0"u8));
        Assert.NotEmpty((await alice.CatalogSchemasAsync(attached, null)).Items);

        // Replayed as another principal, or anonymously.
        var asBob = await Assert.ThrowsAnyAsync<RpcException>(() => bob.CatalogSchemasAsync(attached, null));
        var asAnonymous = await Assert.ThrowsAnyAsync<RpcException>(
            () => anonymousClient.CreateProxy<IVgiView>().CatalogSchemasAsync(attached, null));

        // One flipped byte.
        var tampered = attached.ToArray();
        tampered[^1] ^= 0x01;
        var flipped = await Assert.ThrowsAnyAsync<RpcException>(() => alice.CatalogSchemasAsync(tampered, null));

        // The unsealed shape this SDK used to issue, and still issues on stdio/unix.
        var plaintext = (await new VgiServiceImpl(Registry()).CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData;
        Assert.True(plaintext.AsSpan().StartsWith("example\0"u8));
        var unsealed = await Assert.ThrowsAnyAsync<RpcException>(() => alice.CatalogSchemasAsync(plaintext, null));

        Assert.Equal("attach_opaque_data not recognized", asBob.ErrorMessage);
        Assert.Equal(ErrorCodes.InvalidArgument, asBob.ErrorCode);
        Assert.Equal("opaque_data_not_recognized", asBob.ErrorKind);
        Assert.Empty(asBob.ErrorDetails);
        Assert.All([asAnonymous, flipped, unsealed], e => Assert.Equal(Shape(asBob), Shape(e)));
    }

    [Fact]
    public async Task OverHttp_ATransactionIsBoundToItsAttach()
    {
        await using var server = await Server.StartAsync(NewWorker());
        await using var aliceClient = server.Client("alice");
        await using var bobClient = server.Client("bob");
        var alice = aliceClient.CreateProxy<IVgiView>();

        var first = (await alice.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData;
        var second = (await alice.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData;
        var transaction = (await alice.CatalogTransactionBeginAsync(first)).TransactionOpaqueData!;
        Assert.Equal(OpaqueSealer.TransactionVersion, transaction[0]);
        Assert.NotEmpty((await alice.CatalogSchemasAsync(first, transaction)).Items);

        // Same principal, another attach.
        var lifted = await Assert.ThrowsAnyAsync<RpcException>(() => alice.CatalogTransactionCommitAsync(second, transaction));
        // Right attach, another principal: the attach itself is refused first.
        var otherCaller = await Assert.ThrowsAnyAsync<RpcException>(
            () => bobClient.CreateProxy<IVgiView>().CatalogTransactionCommitAsync(first, transaction));
        var tampered = transaction.ToArray();
        tampered[5] ^= 0x80;
        var flipped = await Assert.ThrowsAnyAsync<RpcException>(() => alice.CatalogTransactionCommitAsync(first, tampered));

        Assert.Equal(("transaction_opaque_data not recognized", ErrorCodes.InvalidArgument, "opaque_data_not_recognized"),
            (lifted.ErrorMessage, lifted.ErrorCode, lifted.ErrorKind));
        Assert.Empty(lifted.ErrorDetails);
        Assert.Equal(Shape(lifted), Shape(flipped));
        Assert.Equal("attach_opaque_data not recognized", otherCaller.ErrorMessage);
        await alice.CatalogTransactionCommitAsync(first, transaction);
    }

    /// <summary>A secret attach option is in no value, sealed (HTTP) or not (stdio/unix), and no raw
    /// value reaches the worker's output.</summary>
    [Fact]
    public async Task SecretsNeverInAValue_AndValuesNeverLogged()
    {
        var output = new StringWriter();
        var (stdout, stderr) = (Console.Out, Console.Error);
        Console.SetOut(output);
        Console.SetError(output);
        byte[] sealedValue;
        try
        {
            await using var server = await Server.StartAsync(NewWorker().OnAttach(VaultAttach));
            await using var client = server.Client("alice");
            var view = client.CreateProxy<IVgiView>();
            sealedValue = (await view.CatalogAttachAsync(new CatalogAttachRequest { Name = "vault", Options = Options("api_key", Secret) })).AttachOpaqueData;
            await view.CatalogSchemasAsync(sealedValue, null);
            await Assert.ThrowsAnyAsync<RpcException>(() => server.Client("bob").CreateProxy<IVgiView>().CatalogSchemasAsync(sealedValue, null));
        }
        finally
        {
            Console.SetOut(stdout);
            Console.SetError(stderr);
        }

        var log = output.ToString();
        Assert.DoesNotContain(Convert.ToHexString(sealedValue), log, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(Convert.ToBase64String(sealedValue), log, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, log, StringComparison.Ordinal);
        Assert.False(Contains(sealedValue, Encoding.UTF8.GetBytes(Secret)));

        // The unsealed transports: the value is plaintext, and still carries no secret.
        var registry = Registry();
        registry.OnAttach = VaultAttach;
        var plaintext = (await new VgiServiceImpl(registry).CatalogAttachAsync(
            new CatalogAttachRequest { Name = "vault", Options = Options("api_key", Secret) })).AttachOpaqueData;
        Assert.True(plaintext.AsSpan().StartsWith("vault\0"u8));
        Assert.False(Contains(plaintext, Encoding.UTF8.GetBytes(Secret)));
    }

    [Fact]
    public void OnlyHttpIsSealed_AndAConfiguredKeyIsShared()
    {
        var key = "opaque-sealing-test-key"u8.ToArray();
        var sealer = new OpaqueSealer(key);
        var auth = new AuthContext("test", true, "alice");
        var envelope = sealer.SealAttach("example\0x"u8.ToArray(), auth);
        // A second worker configured with the same key opens what the first sealed.
        Assert.Equal("example\0x"u8.ToArray(), new OpaqueSealer(key).OpenAttach(envelope, auth));
        Assert.Throws<ValueError>(() => new OpaqueSealer("another key"u8.ToArray()).OpenAttach(envelope, auth));
        // Domain is part of the identity, as for state tokens.
        Assert.Throws<ValueError>(() => sealer.OpenAttach(envelope, new AuthContext("other", true, "alice")));
    }

    private static AttachContext? VaultAttach(CatalogAttachRequest request) =>
        request.Name == "vault" && request.Options is not { Length: > 0 }
            ? throw new InvalidOperationException("vault requires api_key")
            : null;

    private static CatalogRegistry Registry()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalog(new CatalogInfo { Name = "example" });
        registry.RegisterCatalog(new CatalogInfo { Name = "vault" }, exclusive: true);
        return registry;
    }

    private static bool Contains(byte[] haystack, byte[] needle) => haystack.AsSpan().IndexOf(needle) >= 0;
}
