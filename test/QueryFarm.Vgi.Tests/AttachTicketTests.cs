using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using QueryFarm.Vgi.ExampleWorker.TicketProbe;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Attributes;
using QueryFarm.VgiRpc.Client.Http;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.Identity;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests;

/// <summary>Attach tickets (<c>vgi.attach_tickets.v1</c>), against the cross-SDK vectors
/// (vgi-python <c>vgi/_test_fixtures/attach_ticket_vectors.json</c>, copied verbatim) and end to end
/// over HTTP with the <c>ticket_probe</c> fixture.</summary>
[Collection("environment")]
public sealed class AttachTicketTests
{
    private static readonly JsonElement s_vectors = LoadVectors();

    private static JsonElement LoadVectors()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Vectors", "attach_ticket_vectors.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(path, System.Text.Encoding.UTF8));
        return doc.RootElement.Clone();
    }

    private static JsonElement Defaults => s_vectors.GetProperty("defaults");

    private static byte[] KeyOf(JsonElement c) =>
        Convert.FromBase64String((c.TryGetProperty("signing_key_b64", out var k) ? k : Defaults.GetProperty("signing_key_b64")).GetString()!);

    private static double NowOf(JsonElement c) =>
        (c.TryGetProperty("now", out var n) ? n : Defaults.GetProperty("now")).GetDouble();

    private static IEnumerable<JsonElement> Section(string name) => s_vectors.GetProperty(name).EnumerateArray();

    public static TheoryData<string> Names(string section) => new(Section(section).Select(c => c.GetProperty("name").GetString()!));

    private static JsonElement Case(string section, string name) =>
        Section(section).Single(c => c.GetProperty("name").GetString() == name);

    // ------------------------------------------------------------------ vectors

    [Fact]
    public void Defaults_MatchThisPortsConstants()
    {
        Assert.Equal(AttachTickets.ClockSkewSeconds, Defaults.GetProperty("clock_skew_seconds").GetInt64());
        Assert.Equal(AttachTickets.MaxOptionsBytes, Defaults.GetProperty("max_options_bytes").GetInt32());
        Assert.Equal(AttachTickets.MaxTicketChars, Defaults.GetProperty("max_ticket_chars").GetInt32());
        Assert.Equal(AttachTickets.EnvelopeVersion, Defaults.GetProperty("envelope_version").GetInt32());
    }

    [Theory]
    [MemberData(nameof(Names), "mint")]
    public void Mint_ReproducesTheVectorByteForByte(string name)
    {
        var c = Case("mint", name);
        var options = Convert.FromBase64String(c.GetProperty("options_ipc_b64").GetString()!);
        var (token, claims) = AttachTickets.Mint(
            KeyOf(c),
            c.GetProperty("principal").GetString()!,
            c.GetProperty("catalog_name").GetString()!,
            options,
            c.GetProperty("data_version_spec").GetString()!,
            c.GetProperty("implementation_version").GetString()!,
            c.GetProperty("issued_at").GetInt64(),
            c.GetProperty("expires_at").GetInt64(),
            c.GetProperty("ticket_id").GetString(),
            Convert.FromHexString(c.GetProperty("nonce_hex").GetString()!));

        Assert.Equal(c.GetProperty("aad_hex").GetString(), Convert.ToHexStringLower(AttachTickets.Aad(c.GetProperty("principal").GetString()!)));
        Assert.Equal(c.GetProperty("payload_hex").GetString(), Convert.ToHexStringLower(AttachTickets.EncodePayload(claims)));
        Assert.Equal(c.GetProperty("token").GetString(), token);
    }

    [Theory]
    [MemberData(nameof(Names), "accept")]
    public void Accept_OpensToTheVectorClaims(string name)
    {
        var c = Case("accept", name);
        var claims = AttachTickets.Open(KeyOf(c), c.GetProperty("token").GetString()!, c.GetProperty("principal").GetString(), NowOf(c));
        var expected = c.GetProperty("claims");
        Assert.Equal(expected.GetProperty("issued_at").GetInt64(), claims.IssuedAt);
        Assert.Equal(expected.GetProperty("expires_at").GetInt64(), claims.ExpiresAt);
        Assert.Equal(expected.GetProperty("ticket_id").GetString(), claims.TicketId);
        Assert.Equal(expected.GetProperty("catalog_name").GetString(), claims.CatalogName);
        Assert.Equal(expected.GetProperty("data_version_spec").GetString(), claims.DataVersionSpec);
        Assert.Equal(expected.GetProperty("implementation_version").GetString(), claims.ImplementationVersion);
        Assert.Equal(Convert.FromBase64String(expected.GetProperty("options_ipc_b64").GetString()!), claims.OptionsIpc);
    }

    [Theory]
    [MemberData(nameof(Names), "reject")]
    public void Reject_RefusesWithTheVectorKind(string name)
    {
        var c = Case("reject", name);
        var error = Assert.ThrowsAny<RpcException>(() =>
            AttachTickets.Open(KeyOf(c), c.GetProperty("token").GetString()!, c.GetProperty("principal").GetString(), NowOf(c)));
        Assert.Equal(c.GetProperty("error_kind").GetString(), error.ErrorKind);
        Assert.DoesNotContain(c.GetProperty("token").GetString()!, error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(Names), "redeem")]
    public void Redeem_AppliesTheRulesOfSection6(string name)
    {
        var c = Case("redeem", name);
        var incoming = new CatalogAttachRequest
        {
            Name = "whatever_the_runner_typed",
            Options = OptionsIpc(c.GetProperty("options").EnumerateObject().Select(p => (p.Name, p.Value.GetString()!))),
            ClientCapabilities = [1, 2, 3],
        };
        var principal = c.GetProperty("principal").GetString();
        var auth = string.IsNullOrEmpty(principal) ? AuthContext.Anonymous : new AuthContext("grant", true, principal);

        if (c.TryGetProperty("error_kind", out var kind))
        {
            var error = Assert.ThrowsAny<RpcException>(() => AttachTickets.Redeem(incoming, KeyOf(c), auth, NowOf(c)));
            Assert.Equal(kind.GetString(), error.ErrorKind);
            return;
        }

        var restored = AttachTickets.Redeem(incoming, KeyOf(c), auth, NowOf(c));
        if (c.GetProperty("result").ValueKind == JsonValueKind.Null)
        {
            Assert.Null(restored);
            return;
        }

        var result = c.GetProperty("result");
        Assert.NotNull(restored);
        Assert.Equal(result.GetProperty("catalog_name").GetString(), restored!.Name);
        Assert.Equal(result.GetProperty("data_version_spec").GetString(), restored.DataVersionSpec);
        Assert.Equal(result.GetProperty("implementation_version").GetString(), restored.ImplementationVersion);
        Assert.Equal(incoming.ClientCapabilities, restored.ClientCapabilities);
        var expected = result.GetProperty("options").EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetString());
        Assert.Equal(expected, OptionsOf(restored.Options));
    }

    [Fact]
    public void Redeem_WithoutASigningKeyIsInvalid()
    {
        var token = Case("accept", "probe_with_options_opens").GetProperty("token").GetString()!;
        var request = new CatalogAttachRequest { Name = "x", Options = OptionsIpc([(AttachTickets.OptionName, token)]) };
        var error = Assert.Throws<AttachTicketInvalidException>(() =>
            AttachTickets.Redeem(request, null, new AuthContext("grant", true, "alice"), 1790000010));
        Assert.Equal(ErrorCodes.InvalidArgument, error.ErrorCode);
    }

    [Fact]
    public void Redeem_ANonStringTicketIsInvalid()
    {
        using var batch = new RecordBatch(
            new Schema([new Field(AttachTickets.OptionName, Int64Type.Default, true)], null),
            [new Int64Array.Builder().Append(1).Build()], 1);
        var request = new CatalogAttachRequest { Name = "x", Options = RecordBatchIpc.Write(batch) };
        Assert.Throws<AttachTicketInvalidException>(() =>
            AttachTickets.Redeem(request, KeyOf(Defaults), new AuthContext("grant", true, "alice")));
    }

    // ------------------------------------------------------------------ reserved name

    [Fact]
    public void ReservedOptionName_IsADefinitionAndStartupError()
    {
        Assert.Throws<ArgumentException>(() =>
            AttachOptionSpecBuilder.Build("VGI_Attach_Ticket", "", StringType.Default, null));

        // A spec built by hand, past the builder, is still refused when the catalog registers.
        var spec = new AttachOptionSpec
        {
            Name = "vgi_attach_TICKET",
            Description = "",
            Type = SchemaIpc.WriteSchemaOnly(new Schema([new Field("value", StringType.Default, true)], null)),
        };
        Assert.Throws<ArgumentException>(() =>
            new Worker().RegisterCatalog(new CatalogInfo { Name = "c", AttachOptionSpecs = [EmbeddedIpc.Encode(spec)] }));
    }

    // ------------------------------------------------------------------ hosting

    private static readonly GrantKeys s_grantKeys = new([Enumerable.Range(0, 32).Select(i => (byte)(i + 100)).ToArray()], maxTtlSeconds: 3600);
    private static readonly byte[] s_signingKey = "attach-ticket-test-signing-key"u8.ToArray();

    private static Worker ProbeWorker() =>
        TicketProbeSetup.Register(new Worker().CatalogName("example").DefaultSchema("main"))
            .OnAttach(TicketProbeSetup.Handle);

    private static bool HostsTickets(Worker worker, Worker.ServerTransport transport = Worker.ServerTransport.Http) =>
        worker.NewRpcServer(transport).HostedProtocols.Contains(AttachTickets.ProtocolName);

    [Fact]
    public void Hosting_NeedsAnExplicitKeyGrantsAndHttp()
    {
        WithEnvironment(new() { ["VGI_SIGNING_KEY"] = null, [GrantKeys.KeysEnvironmentVariable] = null }, () =>
        {
            Assert.True(HostsTickets(ProbeWorker().SigningKey(s_signingKey).SealedGrants(s_grantKeys)));
            var server = ProbeWorker().SigningKey(s_signingKey).SealedGrants(s_grantKeys).NewRpcServer(Worker.ServerTransport.Http);
            Assert.Equal(["seal_attach"], server.MethodsForProtocol(AttachTickets.ProtocolName)!.Keys);

            // Off HTTP there is no caller identity.
            Assert.False(HostsTickets(ProbeWorker().SigningKey(s_signingKey).SealedGrants(s_grantKeys), Worker.ServerTransport.Stdio));
            Assert.False(HostsTickets(ProbeWorker().SigningKey(s_signingKey).SealedGrants(s_grantKeys), Worker.ServerTransport.Unix));
            // No key, or no way to issue grants: absent.
            Assert.False(HostsTickets(ProbeWorker().SealedGrants(s_grantKeys)));
            Assert.False(HostsTickets(ProbeWorker().SigningKey(s_signingKey)));
            // A worker's own minter is a way to issue grants.
            Assert.True(HostsTickets(ProbeWorker().SigningKey(s_signingKey)
                .Identity(mintGrant: (_, _, _, _) => new IssuedGrant("g", 1.0))));
        });
    }

    [Fact]
    public void Hosting_FromTheEnvironmentButNeverAMintedKey()
    {
        var grantKey = Convert.ToBase64String(Enumerable.Repeat((byte)9, 32).ToArray());
        WithEnvironment(new() { ["VGI_SIGNING_KEY"] = "stable", [GrantKeys.KeysEnvironmentVariable] = grantKey }, () =>
        {
            Assert.True(HostsTickets(ProbeWorker()));
            WithEnvironment(new() { ["VGI_SIGNING_KEY_MINTED"] = "1" }, () => Assert.False(HostsTickets(ProbeWorker())));
        });
        WithEnvironment(new() { ["VGI_SIGNING_KEY"] = "", [GrantKeys.KeysEnvironmentVariable] = grantKey }, () =>
            Assert.False(HostsTickets(ProbeWorker())));
    }

    // ------------------------------------------------------------------ seal_attach

    private sealed class Ctx(AuthContext auth) : ICallContext
    {
        public AuthContext Auth => auth;

        public PeerEvidenceSet PeerEvidence => PeerEvidenceSet.Empty;

        public void EmitLog(VgiRpc.Logging.VgiLogLevel level, string message, IReadOnlyDictionary<string, object?>? extra = null)
        {
        }
    }

    /// <summary>The service a worker hosting <c>ticket_probe</c> would host, with
    /// <paramref name="maxTtlSeconds"/> as its grant maximum.</summary>
    private static IAttachTickets Service(long? maxTtlSeconds = 3600)
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalog(TicketProbeSetup.Info, exclusive: true);
        return new AttachTicketsService(registry, s_signingKey, maxTtlSeconds);
    }

    private static readonly AuthContext s_alice = new("bearer", true, "alice");

    [Fact]
    public async Task SealAttach_AnonymousIsActionDenied()
    {
        var service = Service();
        var error = await Assert.ThrowsAnyAsync<RpcException>(() => service.SealAttachAsync(
            new SealAttachRequest { CatalogName = TicketProbeSetup.CatalogName, Options = OptionsIpc([("api_key", "k")]) },
            new Ctx(AuthContext.Anonymous)));
        Assert.Equal(("action_denied", ErrorCodes.PermissionDenied), (error.ErrorKind, error.ErrorCode));
    }

    [Fact]
    public async Task SealAttach_ReportsEveryViolationTogether()
    {
        var service = Service();
        var error = await Assert.ThrowsAnyAsync<RpcException>(() => service.SealAttachAsync(
            new SealAttachRequest
            {
                CatalogName = TicketProbeSetup.CatalogName,
                Options = OptionsIpc([("Region", "x"), ("nope", "y"), ("Vgi_Attach_Ticket", "z")]),
                TtlSeconds = -1,
            },
            new Ctx(s_alice)));
        Assert.Equal(("invalid_request", ErrorCodes.InvalidArgument), (error.ErrorKind, error.ErrorCode));
        var fields = error.GetBadRequest()!.FieldViolations.Select(v => v.Field).ToList();
        Assert.Equal(["ttl_seconds", "options.nope", "options.Vgi_Attach_Ticket", "options.api_key"], fields);

        var unknown = await Assert.ThrowsAnyAsync<RpcException>(() => service.SealAttachAsync(
            new SealAttachRequest { CatalogName = "no_such_catalog" }, new Ctx(s_alice)));
        Assert.Equal(["catalog_name"], unknown.GetBadRequest()!.FieldViolations.Select(v => v.Field));
    }

    [Theory]
    [InlineData(0L, 3600L)]
    [InlineData(60L, 60L)]
    [InlineData(999_999L, 3600L)]
    public async Task SealAttach_LifetimeIsCappedAtTheGrantMaximum(long ttl, long expected)
    {
        var service = Service();
        var before = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var sealedTicket = await service.SealAttachAsync(
            new SealAttachRequest { CatalogName = TicketProbeSetup.CatalogName, Options = OptionsIpc([("api_key", "k")]), TtlSeconds = ttl },
            new Ctx(s_alice));
        var claims = AttachTickets.Open(s_signingKey, sealedTicket.Ticket, "alice");
        Assert.Equal(expected, claims.ExpiresAt - claims.IssuedAt);
        Assert.InRange(claims.IssuedAt, before, before + 5);
        Assert.Equal(claims.ExpiresAt, sealedTicket.ExpiresAt);
    }

    [Fact]
    public async Task SealAttach_NoMaximumMeansNoExpiry()
    {
        var sealedTicket = await Service(maxTtlSeconds: null).SealAttachAsync(
            new SealAttachRequest { CatalogName = TicketProbeSetup.CatalogName, Options = OptionsIpc([("api_key", "k")]) },
            new Ctx(s_alice));
        Assert.Equal(double.PositiveInfinity, sealedTicket.ExpiresAt);
        Assert.Equal(0, AttachTickets.Open(s_signingKey, sealedTicket.Ticket, "alice").ExpiresAt);

        // The ceiling: grant keys' maximum, else the environment, else none.
        await WithEnvironmentAsync(new() { [GrantKeys.MaxTtlEnvironmentVariable] = null }, () =>
        {
            Assert.Null(AttachTickets.ResolveMaxTtl(null));
            Assert.Equal(3600, AttachTickets.ResolveMaxTtl(s_grantKeys));
            return Task.CompletedTask;
        });
        await WithEnvironmentAsync(new() { [GrantKeys.MaxTtlEnvironmentVariable] = "120" }, () =>
        {
            Assert.Equal(120, AttachTickets.ResolveMaxTtl(null));
            return Task.CompletedTask;
        });
    }

    // ------------------------------------------------------------------ end to end over HTTP

    public interface IGrantClient
    {
        Task<IssuedGrant> IssueGrantAsync(string purpose, List<string> scopes, long ttlSeconds);
    }

    [ProtocolName("vgi.v2")]
    public interface IVgiAttachClient
    {
        Task<CatalogAttachResult> CatalogAttachAsync(CatalogAttachRequest request);
    }

    /// <summary>The probe row, read through the fixture's own table function from the attach state
    /// the server returned. Over HTTP that state is sealed for its caller, so it is opened here as
    /// the worker would open it for <paramref name="caller"/> -- which also proves it was sealed for
    /// exactly that caller.</summary>
    private static (string Region, string Digest) ProbeRow(CatalogAttachResult attached, AuthContext caller)
    {
        var plaintext = new OpaqueSealer(s_signingKey).OpenAttach(attached.AttachOpaqueData, caller);
        var function = new TicketProbeFunction();
        var producer = function.CreateProducer(new TableInitParams
        {
            FunctionName = function.Name,
            Arguments = new TableArguments([], new Dictionary<string, IArrowArray>()),
            OutputSchema = function.OutputSchema,
            AttachOpaqueData = plaintext,
        });
        using var output = new OutputCollector(function.OutputSchema);
        producer.Produce(output);
        var batch = output.EmittedBatch!;
        Assert.Equal(1, batch.Length);
        return (((StringArray)batch.Column("region")).GetString(0), ((StringArray)batch.Column("api_key_sha256")).GetString(0));
    }

    /// <summary>Alice (a fresh login) attaches, seals and mints a grant; a runner holding only her
    /// grant reattaches with only the ticket and reads the same row; Bob's grant with her ticket is
    /// refused.</summary>
    [Fact]
    public async Task EndToEnd_ARunnerWithTheGrantReattachesWithOnlyTheTicket()
    {
        var worker = ProbeWorker().SigningKey(s_signingKey).SealedGrants(s_grantKeys)
            .HttpAuthenticate(TestBearers.Create(grantsConfigured: true));
        var builder = WebApplication.CreateSlimBuilder(Worker.WorkerHostOptions());
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        await using var app = builder.Build();
        worker.MapHttp(app);
        await app.StartAsync(TestContext.Current.CancellationToken);
        var address = new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single());

        HttpRpcClient Client(string protocol, string bearer, string? version = null) => new(address, new HttpRpcClientOptions
        {
            Protocol = protocol,
            ProtocolVersion = version,
            DefaultHeaders = new Dictionary<string, string> { ["Authorization"] = $"Bearer {bearer}" },
        });

        var options = OptionsIpc([("region", "eu-west-2"), ("api_key", "sk-test-0123456789")]);

        // Alice, logged in: attach, seal, mint.
        await using var aliceVgi = Client("vgi.v2", "vgi-test-alice", Worker.DefaultProtocolVersion);
        var original = await aliceVgi.CreateProxy<IVgiAttachClient>().CatalogAttachAsync(
            new CatalogAttachRequest { Name = TicketProbeSetup.CatalogName, Options = options });
        Assert.Equal(("eu-west-2", "0d3b56072291"), ProbeRow(original, new AuthContext("bearer", true, "alice")));

        await using var aliceTickets = Client(AttachTickets.ProtocolName, "vgi-test-alice", AttachTickets.ProtocolVersion);
        var sealedTicket = await aliceTickets.CreateProxy<IAttachTickets>().SealAttachAsync(new SealAttachRequest
        {
            CatalogName = TicketProbeSetup.CatalogName,
            Options = options,
        });
        Assert.StartsWith(AttachTickets.Prefix, sealedTicket.Ticket);

        await using var aliceIdentity = Client(IdentityProtocol.ProtocolName, "vgi-test-alice");
        var aliceGrant = await aliceIdentity.CreateProxy<IGrantClient>().IssueGrantAsync("nightly", [], 600);

        // The runner: only the grant, only the ticket.
        await using var runner = Client("vgi.v2", aliceGrant.Token, Worker.DefaultProtocolVersion);
        var reattached = await runner.CreateProxy<IVgiAttachClient>().CatalogAttachAsync(new CatalogAttachRequest
        {
            Name = "anything",
            Options = OptionsIpc([(AttachTickets.OptionName, sealedTicket.Ticket)]),
        });
        Assert.Equal(("eu-west-2", "0d3b56072291"), ProbeRow(reattached, new AuthContext("grant", true, "alice")));

        // Without the ticket the required api_key is missing: the secret did not travel again.
        await Assert.ThrowsAnyAsync<RpcException>(() => runner.CreateProxy<IVgiAttachClient>().CatalogAttachAsync(
            new CatalogAttachRequest { Name = TicketProbeSetup.CatalogName, Options = OptionsIpc([("region", "eu-west-2")]) }));

        // Another option beside the ticket is refused before the ticket is opened.
        var beside = await Assert.ThrowsAnyAsync<RpcException>(() => runner.CreateProxy<IVgiAttachClient>().CatalogAttachAsync(
            new CatalogAttachRequest
            {
                Name = TicketProbeSetup.CatalogName,
                Options = OptionsIpc([(AttachTickets.OptionName, sealedTicket.Ticket), ("region", "us-west-1")]),
            }));
        Assert.Equal("invalid_request", beside.ErrorKind);

        // Bob's grant with Alice's ticket.
        await using var bobIdentity = Client(IdentityProtocol.ProtocolName, "vgi-test-bob");
        var bobGrant = await bobIdentity.CreateProxy<IGrantClient>().IssueGrantAsync("nightly", [], 600);
        await using var bobRunner = Client("vgi.v2", bobGrant.Token, Worker.DefaultProtocolVersion);
        var refused = await Assert.ThrowsAnyAsync<RpcException>(() => bobRunner.CreateProxy<IVgiAttachClient>().CatalogAttachAsync(
            new CatalogAttachRequest { Name = "anything", Options = OptionsIpc([(AttachTickets.OptionName, sealedTicket.Ticket)]) }));
        Assert.Equal((AttachTickets.InvalidKind, ErrorCodes.InvalidArgument), (refused.ErrorKind, refused.ErrorCode));
        Assert.DoesNotContain(sealedTicket.Ticket, refused.Message, StringComparison.Ordinal);

        // An anonymous caller cannot seal.
        await using var anonymous = new HttpRpcClient(address, new HttpRpcClientOptions
        {
            Protocol = AttachTickets.ProtocolName,
            ProtocolVersion = AttachTickets.ProtocolVersion,
        });
        var denied = await Assert.ThrowsAnyAsync<RpcException>(() => anonymous.CreateProxy<IAttachTickets>().SealAttachAsync(
            new SealAttachRequest { CatalogName = TicketProbeSetup.CatalogName, Options = options }));
        Assert.Equal("action_denied", denied.ErrorKind);
        await app.StopAsync(TestContext.Current.CancellationToken);
    }

    // ------------------------------------------------------------------ helpers

    private static byte[] OptionsIpc(IEnumerable<(string Name, string Value)> options)
    {
        var list = options.ToList();
        using var batch = new RecordBatch(
            new Schema(list.Select(o => new Field(o.Name, StringType.Default, true)), null),
            list.Select(o => (IArrowArray)new StringArray.Builder().Append(o.Value).Build()).ToList(),
            1);
        return RecordBatchIpc.Write(batch);
    }

    private static Dictionary<string, string?> OptionsOf(byte[]? ipc)
    {
        if (ipc is not { Length: > 0 })
        {
            return [];
        }

        using var batch = RecordBatchIpc.Read(ipc);
        return batch.Schema.FieldsList.Select((f, i) => (f.Name, Value: ((StringArray)batch.Column(i)).GetString(0)))
            .ToDictionary(p => p.Name, p => (string?)p.Value);
    }

    private static void WithEnvironment(Dictionary<string, string?> values, Action action) =>
        WithEnvironmentAsync(values, () =>
        {
            action();
            return Task.CompletedTask;
        }).GetAwaiter().GetResult();

    private static async Task WithEnvironmentAsync(Dictionary<string, string?> values, Func<Task> action)
    {
        var previous = values.Keys.ToDictionary(k => k, Environment.GetEnvironmentVariable);
        foreach (var (k, v) in values)
        {
            Environment.SetEnvironmentVariable(k, v);
        }

        try
        {
            await action();
        }
        finally
        {
            foreach (var (k, v) in previous)
            {
                Environment.SetEnvironmentVariable(k, v);
            }
        }
    }
}
