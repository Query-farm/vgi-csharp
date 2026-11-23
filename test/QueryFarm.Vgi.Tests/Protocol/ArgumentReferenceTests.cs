using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.Tests.Arguments;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.Identity;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

/// <summary>Protocol 3.4 argument references and the raw-preserving <c>bind_call</c> (vgi
/// <c>docs/large-values-3.4.md</c> §2, §3.3 rule 11, §3.4 tests 3 and 4, §5a).</summary>
[Collection("environment")]
public sealed class ArgumentReferenceTests : IDisposable
{
    private const string Payload = "the argument travels by reference";
    private const string Secret = "s3cr3t-value-never-in-plaintext";
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "vgi-argref-tests-" + Guid.NewGuid().ToString("N"));
    private readonly string? _previousKeyFile = Environment.GetEnvironmentVariable(HostLocalKey.FileVariable);

    public ArgumentReferenceTests()
    {
        Environment.SetEnvironmentVariable(HostLocalKey.FileVariable, Path.Combine(_dir, "keys", HostLocalKey.FileName));
        HostLocalKey.ResetCache();
    }

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(HostLocalKey.FileVariable, _previousKeyFile);
        HostLocalKey.ResetCache();
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private static readonly byte[] s_arguments = PublicArgumentApiTests.ArgsBytes(("positional_0", PublicArgumentApiTests.Str(Payload)));

    private static string Sha256Hex(byte[] data) => Convert.ToHexStringLower(SHA256.HashData(data));

    private static ArgumentsLocation Location(byte[] data) =>
        new() { Ref = [1, 2, 3, 4], Size = data.Length, Sha256 = Sha256Hex(data) };

    private static BindRequest Bind(byte[]? arguments, ArgumentsLocation? location, byte[]? attach = null) => new()
    {
        FunctionName = Echo.FunctionName,
        FunctionType = FunctionType.Table,
        Arguments = arguments,
        ArgumentsLocation = location,
        AttachOpaqueData = attach,
    };

    private static string Corpus()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "conformance", "nested-binary", "manifest.json")))
            directory = directory.Parent;
        Assert.NotNull(directory);
        return Path.Combine(directory.FullName, "conformance", "nested-binary");
    }

    private static byte[] Column(byte[] ipc, string name)
    {
        using var reader = new ArrowStreamReader(new MemoryStream(ipc));
        using var batch = reader.ReadNextRecordBatch();
        return ((BinaryArray)batch.Column(batch.Schema.GetFieldIndex(name))).GetBytes(0).ToArray();
    }

    [Theory]
    [InlineData("bind-both-null.arrow")]
    [InlineData("bind-both-set.arrow")]
    public async Task TheWorkerRefusesAnInvalidCombination(string file)
    {
        var registry = new CatalogRegistry();
        registry.RegisterTable(new Echo());
        var service = new VgiServiceImpl(registry);
        var bytes = File.ReadAllBytes(Path.Combine(Corpus(), file));

        var bind = await Assert.ThrowsAsync<RpcException>(() => service.BindAsync(EmbeddedIpc.Decode<BindRequest>(bytes), new Ctx()));
        Assert.Equal((BindArguments.InvalidBindRequestKind, ErrorCodes.InvalidArgument), (bind.ErrorKind, bind.ErrorCode));
        Assert.Equal(BindArguments.InvalidBindRequestMessage, bind.ErrorMessage);

        var init = await Assert.ThrowsAsync<RpcException>(() =>
            service.InitAsync(new InitRequest { BindCall = Nested<BindRequest>.FromBytes(bytes) }, new Ctx()));
        Assert.Equal(BindArguments.InvalidBindRequestKind, init.ErrorKind);
    }

    [Fact]
    public async Task ALocationResolvesAtTheTurnsEntryAndUserCodeSeesTheArguments()
    {
        var echo = new Echo();
        var registry = new CatalogRegistry();
        registry.RegisterTable(echo);
        var service = new VgiServiceImpl(registry);
        var ctx = new Ctx { Objects = { [Sha256Hex(s_arguments)] = s_arguments } };
        var location = Location(s_arguments);

        await service.BindAsync(Bind(null, location), ctx);
        var bindCall = Nested<BindRequest>.Of(Bind(null, location));
        await service.InitAsync(new InitRequest { BindCall = bindCall }, ctx);
        await service.TableFunctionCardinalityAsync(new TableFunctionCardinalityRequest { BindCall = bindCall }, ctx);

        Assert.Equal([Payload, Payload, Payload], echo.Seen);
        Assert.Equal(3, ctx.Resolves);
        // The wrapper is untouched: still the location, never the value.
        Assert.Null(bindCall.Value.Arguments);
        Assert.Null(EmbeddedIpc.Decode<BindRequest>(bindCall.Bytes.ToArray()).Arguments);
    }

    [Fact]
    public async Task AGoneObjectFailsWithThePreparedStatementMessage()
    {
        var registry = new CatalogRegistry();
        registry.RegisterTable(new Echo());
        var service = new VgiServiceImpl(registry);

        var error = await Assert.ThrowsAsync<RpcException>(() => service.BindAsync(Bind(null, Location(s_arguments)), new Ctx()));

        Assert.Equal((StateRefErrors.ArgumentsRefMissingKind, ErrorCodes.NotFound), (error.ErrorKind, error.ErrorCode));
        Assert.Equal("this prepared statement's large argument has expired; prepare it again", error.ErrorMessage);
    }

    [Fact]
    public async Task ATamperedReferenceIsInvalidArgumentsRefNamingTheKind()
    {
        var registry = new CatalogRegistry();
        registry.RegisterTable(new Echo());
        var ctx = new Ctx { Tampered = true };

        var error = await Assert.ThrowsAsync<RpcException>(() =>
            new VgiServiceImpl(registry).BindAsync(Bind(null, Location(s_arguments)), ctx));

        Assert.Equal((StateRefErrors.InvalidArgumentsRefKind, ErrorCodes.InvalidArgument), (error.ErrorKind, error.ErrorCode));
        Assert.StartsWith("invalid_arguments_ref: ", error.ErrorMessage);
    }

    [Theory]
    [InlineData(0, "aa")]
    [InlineData(10, "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789")]
    public void AMalformedLocationIsInvalidArgumentsRef(long size, string sha256)
    {
        var bind = Bind(null, new ArgumentsLocation { Ref = [1], Size = size, Sha256 = sha256 });
        var error = Assert.Throws<RpcException>(() => BindArguments.Validate(bind));
        Assert.Equal(StateRefErrors.InvalidArgumentsRefKind, error.ErrorKind);
    }

    [Fact]
    public void TheSplitFingerprintUsesSizeAndDigestNeverTheRef()
    {
        var location = Location(s_arguments);
        var a = BindArguments.FingerprintInput(Bind(null, location));
        var b = BindArguments.FingerprintInput(Bind(s_arguments, new ArgumentsLocation { Ref = [9, 9], Size = location.Size, Sha256 = location.Sha256 }));
        Assert.Equal(a, b); // resolved or not, any ref: the same input
        Assert.Equal(41, a.Length);
        Assert.Equal(0x01, a[0]);
        Assert.Equal([0x00, .. s_arguments], BindArguments.FingerprintInput(Bind(s_arguments, null)));
    }

    /// <summary>§3.4 test 4 (rule 11): with opaque sealing active, the request-entry opening path
    /// leaves <c>bind_call</c>'s bytes as received (sealed envelopes and all) while the handler sees
    /// the opened attach.</summary>
    [Fact]
    public async Task ReEncodingAfterOpeningEmitsTheReceivedBindCall()
    {
        var root = "unit-test-signing-key-0123456789abcdef"u8.ToArray();
        var echo = new Echo();
        var registry = new CatalogRegistry();
        registry.RegisterTable(echo);
        var inner = new VgiServiceImpl(registry) { SecretOptionsRoot = () => root };
        var sealer = new OpaqueSealer(root);
        IVgiService service = OpaqueSealingProxy.Wrap(inner, sealer);
        var attach = (await inner.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData;
        var sealedAttach = sealer.SealAttach(attach, AuthContext.Anonymous);
        var received = EmbeddedIpc.Encode(new InitRequest { BindCall = Nested<BindRequest>.Of(Bind(s_arguments, null, sealedAttach)) });

        var request = EmbeddedIpc.Decode<InitRequest>(received);
        var bindCallBytes = request.BindCall.Bytes.ToArray();
        await service.InitAsync(request, new Ctx());

        Assert.Equal(attach, echo.InitAttach);
        Assert.Equal(bindCallBytes, Column(EmbeddedIpc.Encode(request), "bind_call"));
        Assert.Equal(received, EmbeddedIpc.Encode(request));
        Assert.Equal(sealedAttach, request.BindCall.Value.AttachOpaqueData);
    }

    [Fact]
    public async Task BufferingPersistsTheReferenceFormSealed()
    {
        var registry = new CatalogRegistry();
        var buffering = new Buffer();
        registry.RegisterTableBuffering(buffering);
        var service = new VgiServiceImpl(registry);
        var ctx = new Ctx { Objects = { [Sha256Hex(s_arguments)] = s_arguments } };
        var bind = new BindRequest
        {
            FunctionName = buffering.Name,
            FunctionType = FunctionType.Table,
            ArgumentsLocation = Location(s_arguments),
            Secrets = PublicArgumentApiTests.SecretsBytes(("s", [("token", Secret)])),
            InputSchema = SchemaIpc.WriteSchemaOnly(buffering.OutputSchema),
        };
        var executionId = Guid.NewGuid().ToByteArray();

        await service.InitAsync(new InitRequest { BindCall = Nested<BindRequest>.Of(bind), Phase = VgiInitPhase.TableBuffering, ExecutionId = executionId }, ctx);
        var stored = Directory.GetFiles(Path.Combine(Path.GetTempPath(), "vgi_csharp_buffering"), "*.bin", SearchOption.AllDirectories)
            .Where(path => path.Contains(Convert.ToHexString(executionId), StringComparison.OrdinalIgnoreCase))
            .Select(File.ReadAllBytes)
            .ToList();
        Assert.NotEmpty(stored);
        Assert.All(stored, value => Assert.True(value.AsSpan().IndexOf(Encoding.UTF8.GetBytes(Secret)) < 0));

        await service.TableBufferingCombineAsync(new TableBufferingCombineRequest { FunctionName = buffering.Name, ExecutionId = executionId, StateIds = [] }, ctx);
        Assert.Equal(Payload, buffering.CombinedPayload);
        FunctionStorage.DeleteAll(executionId);
    }

    private sealed class Ctx : ICallContext
    {
        public Dictionary<string, byte[]> Objects { get; } = [];

        public bool Tampered { get; init; }

        public int Resolves { get; private set; }

        public AuthContext Auth => AuthContext.Anonymous;

        public void EmitLog(QueryFarm.VgiRpc.Logging.VgiLogLevel level, string message, IReadOnlyDictionary<string, object?>? extra = null)
        {
        }

        public Task<byte[]> ResolveArgumentRefAsync(byte[] reference, long size, string sha256, CancellationToken cancellationToken = default)
        {
            Resolves++;
            if (Tampered)
                throw StateRefErrors.InvalidArgumentsRef("argument reference does not open");
            return Objects.TryGetValue(sha256, out var data)
                ? Task.FromResult(data)
                : throw StateRefErrors.ArgumentsRefMissing("the uploaded argument object is gone");
        }
    }

    private sealed class Echo : ITableFunction
    {
        public const string FunctionName = "argref_echo";

        public List<string?> Seen { get; } = [];

        public byte[]? InitAttach { get; private set; }

        public string Name => FunctionName;

        public Schema ArgumentsSchema { get; } = new([new Field("payload", StringType.Default, true)], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, true)], metadata: null);

        public void Bind(TableBindParams bindParams) => Seen.Add(bindParams.Arguments.Positional(0) as string);

        public long? Cardinality(TableBindParams bindParams)
        {
            Seen.Add(bindParams.Arguments.Positional(0) as string);
            return 1;
        }

        public ITableFunctionProducer CreateProducer(TableInitParams initParams)
        {
            Seen.Add(initParams.Arguments.Positional(0) as string);
            InitAttach = initParams.AttachOpaqueData;
            return new Done();
        }

        private sealed class Done : ITableFunctionProducer
        {
            public void Produce(OutputCollector output) => output.Finish();
        }
    }

    private sealed class Buffer : QueryFarm.Vgi.Buffering.ITableBufferingFunction
    {
        public string? CombinedPayload { get; private set; }

        public string Name => "argref_buffer";

        public Schema ArgumentsSchema { get; } = new([new Field("payload", StringType.Default, true)], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, true)], metadata: null);

        public Schema ResolveOutputSchema(QueryFarm.Vgi.TableInOut.TableInOutBindParams bindParams) => OutputSchema;

        public byte[] Process(RecordBatch batch, QueryFarm.Vgi.Buffering.TableBufferingProcessParams processParams) => processParams.ExecutionId;

        public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, QueryFarm.Vgi.Buffering.TableBufferingCombineParams combineParams)
        {
            CombinedPayload = combineParams.Arguments.Positional(0) as string;
            return [];
        }

        public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, QueryFarm.Vgi.Buffering.TableBufferingFinalizeParams finalizeParams) =>
            throw new NotSupportedException();
    }
}
