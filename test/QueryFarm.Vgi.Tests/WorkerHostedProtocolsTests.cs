using QueryFarm.VgiRpc.Attributes;
using QueryFarm.VgiRpc.Identity;
using QueryFarm.VgiRpc.Server;
using Xunit;

namespace QueryFarm.Vgi.Tests;

[ProtocolName("test.Reports.v1")]
public interface IReportsProtocol
{
    string Ping(string value);
}

public sealed class ReportsProtocol : IReportsProtocol
{
    public string Ping(string value) => value;
}

[ProtocolName("test.Reports.v1")]
public interface IReportsAgain
{
    string Pong(string value);
}

public sealed class ReportsAgain : IReportsAgain
{
    public string Pong(string value) => value;
}

[ProtocolName("vgi.v2")]
public interface IImpersonatesVgi
{
    string Ping(string value);
}

public sealed class ImpersonatesVgi : IImpersonatesVgi
{
    public string Ping(string value) => value;
}

[ProtocolName("vgi_rpc.Reports.v1")]
public interface IClaimsReserved
{
    string Ping(string value);
}

/// <summary>The worker's single server-construction path: the hosted-protocols hook and the
/// Identity opt-in.</summary>
[Collection("environment")]
public sealed class WorkerHostedProtocolsTests
{
    private static Worker NewWorker() => new Worker().CatalogName("test_catalog").DefaultSchema("main");

    [Theory]
    [InlineData("Stdio")]
    [InlineData("Unix")]
    [InlineData("IrohTcp")]
    [InlineData("Http")]
    public void TheHookIsHostedOnEveryTransportAfterVgi(string transportName)
    {
        var transport = Enum.Parse<Worker.ServerTransport>(transportName);
        var calls = 0;
        var worker = NewWorker().HostedProtocols(() =>
        {
            calls++;
            return [HostedProtocol.For<IReportsProtocol>(new ReportsProtocol())];
        });

        var server = worker.NewRpcServer(transport);

        Assert.Equal(1, calls);
        Assert.Equal(["vgi.v2", "test.Reports.v1", "vgi_rpc.Reflection.v1"], server.HostedProtocols);
    }

    [Fact]
    public void NoHookHostsVgiAndReflectionOnly() =>
        Assert.Equal(["vgi.v2", "vgi_rpc.Reflection.v1"], NewWorker().NewRpcServer(Worker.ServerTransport.Stdio).HostedProtocols);

    [Fact]
    public void AReservedNameIsRefusedNamingTheHook()
    {
        var worker = NewWorker().HostedProtocols(() => [new HostedProtocol(typeof(IClaimsReserved), new object())]);
        var error = Assert.Throws<ArgumentException>(() => worker.NewRpcServer(Worker.ServerTransport.Stdio));
        Assert.Contains("Worker.HostedProtocols hook", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWorkersOwnNameIsRefused()
    {
        var worker = NewWorker().HostedProtocols(() => [HostedProtocol.For<IImpersonatesVgi>(new ImpersonatesVgi())]);
        var error = Assert.Throws<ArgumentException>(() => worker.NewRpcServer(Worker.ServerTransport.Stdio));
        Assert.Contains("the worker's own protocol", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ADuplicateNameIsRefused()
    {
        var worker = NewWorker().HostedProtocols(() =>
            [HostedProtocol.For<IReportsProtocol>(new ReportsProtocol()), HostedProtocol.For<IReportsAgain>(new ReportsAgain())]);
        var error = Assert.Throws<ArgumentException>(() => worker.NewRpcServer(Worker.ServerTransport.Stdio));
        Assert.Contains("twice", error.Message, StringComparison.Ordinal);
    }

    /// <summary>A vgi-rpc refusal (here: an implementation of the wrong type) still names the hook.</summary>
    [Fact]
    public void AnRpcLayerRefusalNamesTheHook()
    {
        var worker = NewWorker().HostedProtocols(() => [new HostedProtocol(typeof(IReportsProtocol), new ReportsAgain())]);
        var error = Assert.Throws<ArgumentException>(() => worker.NewRpcServer(Worker.ServerTransport.Stdio));
        Assert.StartsWith("Worker.HostedProtocols hook", error.Message, StringComparison.Ordinal);
    }

    private static TokenIdentity? Resolve(string token) => new("someone");

    private static IssuedGrant Mint(string principal, string purpose, List<string> scopes, long ttl) => new("t", 1.0);

    /// <summary>Identity is hosted on HTTP only, and only the methods whose hooks exist.</summary>
    [Fact]
    public void IdentityIsHostedOnHttpOnly()
    {
        var worker = NewWorker().Identity(resolveToken: Resolve, introspectPrincipals: ["proxy"]);
        Assert.DoesNotContain(IdentityProtocol.ProtocolName, worker.NewRpcServer(Worker.ServerTransport.Stdio).HostedProtocols);
        Assert.DoesNotContain(IdentityProtocol.ProtocolName, worker.NewRpcServer(Worker.ServerTransport.Unix).HostedProtocols);
        var http = worker.NewRpcServer(Worker.ServerTransport.Http);
        Assert.Equal("vgi_rpc.Identity.v1", http.HostedProtocols[^1]);
        Assert.Equal(["introspect_token"], http.MethodsForProtocol(IdentityProtocol.ProtocolName)!.Keys);
    }

    [Fact]
    public void IdentityIsAbsentUnlessOptedInto() =>
        Assert.DoesNotContain(IdentityProtocol.ProtocolName, NewWorker().NewRpcServer(Worker.ServerTransport.Http).HostedProtocols);

    /// <summary>Introspection without an allowlist refuses to start -- there is no permissive default.</summary>
    [Fact]
    public void ResolvingWithoutAnAllowlistRefusesToStart()
    {
        var previous = Environment.GetEnvironmentVariable("VGI_INTROSPECT_PRINCIPALS");
        Environment.SetEnvironmentVariable("VGI_INTROSPECT_PRINCIPALS", null);
        try
        {
            var worker = NewWorker().Identity(resolveToken: Resolve);
            var error = Assert.Throws<InvalidOperationException>(() => worker.NewRpcServer(Worker.ServerTransport.Http));
            Assert.Contains("VGI_INTROSPECT_PRINCIPALS", error.Message, StringComparison.Ordinal);

            Environment.SetEnvironmentVariable("VGI_INTROSPECT_PRINCIPALS", " , proxy ,");
            Assert.Contains(IdentityProtocol.ProtocolName, worker.NewRpcServer(Worker.ServerTransport.Http).HostedProtocols);
        }
        finally
        {
            Environment.SetEnvironmentVariable("VGI_INTROSPECT_PRINCIPALS", previous);
        }
    }

    /// <summary>A worker that mints but resolves nothing is not an oracle and needs no allowlist.</summary>
    [Fact]
    public void MintingNeedsNoAllowlist()
    {
        var http = NewWorker().Identity(mintGrant: Mint).NewRpcServer(Worker.ServerTransport.Http);
        Assert.Equal(["issue_grant"], http.MethodsForProtocol(IdentityProtocol.ProtocolName)!.Keys);
    }
}
