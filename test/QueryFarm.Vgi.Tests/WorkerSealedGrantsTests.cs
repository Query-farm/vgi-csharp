using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Client.Http;
using QueryFarm.VgiRpc.Http;
using QueryFarm.VgiRpc.Identity;
using QueryFarm.VgiRpc.Server;
using Xunit;

namespace QueryFarm.Vgi.Tests;

/// <summary>Sealed grants and <c>resolve_token</c> bearers through the worker
/// (IDENTITY_V1_SPEC.md §9): opt-in, HTTP-only, and accepted back on vgi.v2 calls.</summary>
[Collection("environment")]
public sealed class WorkerSealedGrantsTests
{
    private const string PrincipalHeader = "X-Test-Principal";

    private static readonly GrantKeys s_keys = new([Enumerable.Range(0, 32).Select(i => (byte)i).ToArray()], maxTtlSeconds: 3600);

    private static Worker NewWorker() => new Worker().CatalogName("test_catalog").DefaultSchema("main");

    /// <summary>Keys alone host vgi_rpc.Identity.v1 with issue_grant -- on HTTP only.</summary>
    [Fact]
    public void KeysHostIssueGrantOnHttpOnly()
    {
        var worker = NewWorker().SealedGrants(s_keys);
        var http = worker.NewRpcServer(Worker.ServerTransport.Http);
        Assert.Equal(["issue_grant"], http.MethodsForProtocol(IdentityProtocol.ProtocolName)!.Keys);
        Assert.Null(worker.NewRpcServer(Worker.ServerTransport.Stdio).HostedIdentity);
        Assert.Null(worker.NewRpcServer(Worker.ServerTransport.Unix).HostedIdentity);
    }

    [Fact]
    public void NoKeysChangeNothing()
    {
        WithGrantEnvironment(null, () =>
            Assert.Null(NewWorker().NewRpcServer(Worker.ServerTransport.Http).HostedIdentity));
    }

    /// <summary>VGI_RPC_GRANT_KEYS and --grant-key configure grants; a malformed key stops startup.</summary>
    [Fact]
    public void KeysComeFromTheEnvironmentOrTheCommandLine()
    {
        var key = Convert.ToBase64String(Enumerable.Repeat((byte)5, 32).ToArray());
        WithGrantEnvironment(key, () =>
        {
            Assert.Equal(5, NewWorker().ResolveGrantKeys()!.Keys[0][0]);
            // The environment never reaches the non-HTTP transports.
            Assert.Null(NewWorker().NewRpcServer(Worker.ServerTransport.Stdio).HostedIdentity);
        });
        WithGrantEnvironment("not-a-key", () =>
            Assert.Throws<ArgumentException>(() => NewWorker().NewRpcServer(Worker.ServerTransport.Http)));
        WithGrantEnvironment(null, () =>
        {
            var worker = NewWorker();
            var cli = Convert.ToBase64String(Enumerable.Repeat((byte)7, 32).ToArray());
            worker.ApplyGrantKeyArgs(["--http", "--grant-key", cli, "--grant-key", key]);
            var keys = worker.ResolveGrantKeys()!;
            Assert.Equal((7, 2), (keys.Keys[0][0], keys.Keys.Count));
        });
    }

    /// <summary>A worker's own minter wins over the sealed one; resolveToken still needs its allowlist.</summary>
    [Fact]
    public void HooksAndAllowlistRulesStillHold()
    {
        static IssuedGrant Mint(string principal, string purpose, List<string> scopes, long ttl) => new("custom", 1.0);
        var worker = NewWorker().SealedGrants(s_keys).Identity(mintGrant: Mint);
        var identity = worker.NewRpcServer(Worker.ServerTransport.Http).HostedIdentity!;
        var fresh = new AuthContext("t", true, "alice",
            new Dictionary<string, object?> { ["auth_time"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds() });
        Assert.Equal("custom", identity.IssueGrant("x", [], 60, new StubContext(fresh)).Token);

        WithAllowlist(null, () => Assert.Throws<InvalidOperationException>(() =>
            NewWorker().SealedGrants(s_keys).Identity(resolveToken: _ => null).NewRpcServer(Worker.ServerTransport.Http)));
    }

    public interface IGrantClient
    {
        Task<IssuedGrant> IssueGrantAsync(string purpose, List<string> scopes, long ttlSeconds);
    }

    public interface IVgiClient
    {
        Task<ItemsResponse> CatalogCatalogsAsync();
    }

    /// <summary>End to end over HTTP: mint with a fresh-auth caller, then call a vgi.v2 method with
    /// <c>Bearer &lt;grant&gt;</c> and observe it authenticated as the grant's owner.</summary>
    [Fact]
    public async Task AMintedGrantAuthenticatesAVgiCallAsItsOwner()
    {
        var worker = NewWorker().SealedGrants(s_keys).HttpAuthenticate(context =>
        {
            var principal = context.Request.Headers[PrincipalHeader].ToString();
            if (principal.Length > 0)
            {
                PeerIdentityAuthentication.SetAuth(context, new AuthContext("test", true, principal,
                    new Dictionary<string, object?> { ["auth_time"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds() }));
                return Task.CompletedTask;
            }

            // A bearer is not ours: let the identity bearers try it.
            return string.IsNullOrEmpty(context.Request.Headers.Authorization.ToString())
                ? Task.CompletedTask
                : throw new AuthFailure(AuthReason.InvalidCredential);
        });

        var builder = WebApplication.CreateSlimBuilder(Worker.WorkerHostOptions());
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        await using var app = builder.Build();
        AuthContext? vgiCaller = null;
        app.Use(async (HttpContext context, Func<Task> next) =>
        {
            await next();
            if (context.Request.Path.Value?.Contains("/vgi.v2/", StringComparison.Ordinal) == true)
            {
                vgiCaller = PeerIdentityAuthentication.GetAuth(context);
            }
        });
        worker.MapHttp(app);
        await app.StartAsync(TestContext.Current.CancellationToken);
        var address = new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single());

        await using var minter = new HttpRpcClient(address, new HttpRpcClientOptions
        {
            Protocol = IdentityProtocol.ProtocolName,
            DefaultHeaders = new Dictionary<string, string> { [PrincipalHeader] = "owner@example.com" },
        });
        var grant = await minter.CreateProxy<IGrantClient>().IssueGrantAsync("nightly", ["read"], 600);
        Assert.StartsWith(SealedGrants.TokenPrefix, grant.Token);

        await using var automation = new HttpRpcClient(address, new HttpRpcClientOptions
        {
            Protocol = "vgi.v2",
            ProtocolVersion = Worker.DefaultProtocolVersion,
            DefaultHeaders = new Dictionary<string, string> { ["Authorization"] = $"Bearer {grant.Token}" },
        });
        await automation.CreateProxy<IVgiClient>().CatalogCatalogsAsync();

        Assert.NotNull(vgiCaller);
        Assert.Equal(("grant", true, "owner@example.com"), (vgiCaller!.Domain, vgiCaller.Authenticated, vgiCaller.Principal));
        Assert.Equal(grant.GrantId, vgiCaller.Claims["grant_id"]);

        // A grant cannot mint another grant: it carries no auth_time.
        await using var regrant = new HttpRpcClient(address, new HttpRpcClientOptions
        {
            Protocol = IdentityProtocol.ProtocolName,
            DefaultHeaders = new Dictionary<string, string> { ["Authorization"] = $"Bearer {grant.Token}" },
        });
        var refused = await Assert.ThrowsAsync<QueryFarm.VgiRpc.Errors.RpcException>(
            () => regrant.CreateProxy<IGrantClient>().IssueGrantAsync("again", [], 60));
        Assert.Equal("stale_auth", refused.ErrorKind);
        await app.StopAsync(TestContext.Current.CancellationToken);
    }

    private sealed class StubContext(AuthContext auth) : ICallContext
    {
        public AuthContext Auth => auth;

        public QueryFarm.VgiRpc.Identity.PeerEvidenceSet PeerEvidence => QueryFarm.VgiRpc.Identity.PeerEvidenceSet.Empty;

        public void EmitLog(QueryFarm.VgiRpc.Logging.VgiLogLevel level, string message, IReadOnlyDictionary<string, object?>? extra = null)
        {
        }
    }

    private static void WithGrantEnvironment(string? keys, Action action)
    {
        var previous = Environment.GetEnvironmentVariable(GrantKeys.KeysEnvironmentVariable);
        Environment.SetEnvironmentVariable(GrantKeys.KeysEnvironmentVariable, keys);
        try
        {
            action();
        }
        finally
        {
            Environment.SetEnvironmentVariable(GrantKeys.KeysEnvironmentVariable, previous);
        }
    }

    private static void WithAllowlist(string? principals, Action action)
    {
        var previous = Environment.GetEnvironmentVariable("VGI_INTROSPECT_PRINCIPALS");
        Environment.SetEnvironmentVariable("VGI_INTROSPECT_PRINCIPALS", principals);
        try
        {
            action();
        }
        finally
        {
            Environment.SetEnvironmentVariable("VGI_INTROSPECT_PRINCIPALS", previous);
        }
    }
}
