using Apache.Arrow;
using QueryFarm.Vgi.ExampleWorker.Cache;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Logging;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests.Fixtures;

public sealed class CacheIdentityFixtureTests
{
    [Theory]
    [InlineData("alice", true, "alice")]
    [InlineData("bob", true, "bob")]
    [InlineData("unverified", false, "anonymous")]
    [InlineData(null, false, "anonymous")]
    public async Task InitPassesVerifiedIdentityToTheCacheableProducer(string? principal, bool authenticated, string expected)
    {
        var registry = new CatalogRegistry();
        registry.RegisterTable(new CacheWhoamiFunction());
        var service = new VgiServiceImpl(registry);
        var attach = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
        var bind = new BindRequest
        {
            FunctionName = "cache_whoami",
            SchemaPath = ["data"],
            FunctionType = FunctionType.Table,
            Arguments = [],
            AttachOpaqueData = attach.AttachOpaqueData,
        };
        ICallContext? context = principal is null ? null : new Context(new AuthContext("bearer", authenticated, principal));
        var stream = await service.InitAsync(new InitRequest { BindCall = EmbeddedIpc.Encode(bind) }, context);

        using var output = new OutputCollector(stream.OutputSchema);
        await Assert.IsAssignableFrom<ProducerState>(stream.State).ProduceAsync(output, null, TestContext.Current.CancellationToken);

        Assert.True(output.Finished);
        Assert.Equal(expected, Assert.IsType<StringArray>(Assert.IsType<RecordBatch>(output.EmittedBatch).Column(0)).GetString(0));
        Assert.Equal("300", output.EmittedMetadata!["vgi.cache.ttl"]);
    }

    private sealed class Context(AuthContext auth) : ICallContext
    {
        public AuthContext Auth => auth;

        public void EmitLog(VgiLogLevel level, string message, IReadOnlyDictionary<string, object?>? extra = null)
        {
        }
    }
}
