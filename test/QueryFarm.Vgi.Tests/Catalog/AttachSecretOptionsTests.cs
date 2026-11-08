using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Identity;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests.Catalog;

/// <summary>Secret ATTACH options are encrypted inside <c>attach_opaque_data</c> (vgi
/// SPEC-secret-options): never plaintext on stdio or HTTP, readable through
/// <see cref="AttachOptions.FromAttachData"/> / the params' <c>AttachOptions</c>, bound to their attach,
/// keyed by HKDF from a root; the host-local key file is private, reused and agreed on.</summary>
[Collection("environment")]
public sealed class AttachSecretOptionsTests : IDisposable
{
    private const string Token = "tok-123-very-secret";
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "vgi-secret-tests-" + Guid.NewGuid().ToString("N"));
    private readonly string? _previousKeyFile = Environment.GetEnvironmentVariable(HostLocalKey.FileVariable);

    public AttachSecretOptionsTests()
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

    private static byte[] Options(string? token, string region = "eu", bool transactional = true)
    {
        var tokenArray = new StringArray.Builder();
        if (token is null) tokenArray.AppendNull();
        else tokenArray.Append(token);
        var schema = new Schema(
            [new Field("region", StringType.Default, true), new Field("token", StringType.Default, true), new Field("transactional", BooleanType.Default, true)],
            metadata: null);
        return RecordBatchIpc.Write(new RecordBatch(schema,
            [new StringArray.Builder().Append(region).Build(), tokenArray.Build(), new BooleanArray.Builder().Append(transactional).Build()], 1));
    }

    private static CatalogRegistry Registry()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalog(new CatalogInfo
        {
            Name = "c",
            AttachOptionSpecs =
            [
                AttachOptions.Spec("region", "r", StringType.Default),
                AttachOptions.Spec("token", "t", StringType.Default, secret: true),
                AttachOptions.Spec("transactional", "x", BooleanType.Default, defaultValue: false),
            ],
        });
        return registry;
    }

    private static bool Contains(byte[] haystack, string needle) => haystack.AsSpan().IndexOf(Encoding.UTF8.GetBytes(needle)) >= 0;

    [Fact]
    public async Task OnStdioTheSecretIsEncryptedAndReadableThroughTheAccessor()
    {
        var registry = Registry();
        registry.OnAttach = _ => new AttachContext { ExtraOpaqueData = [7, 0, 8] };
        var attach = (await new VgiServiceImpl(registry).CatalogAttachAsync(new CatalogAttachRequest { Name = "c", Options = Options(Token) })).AttachOpaqueData;

        Assert.False(Contains(attach, Token));
        var options = AttachOptions.FromAttachData(attach);
        Assert.Equal(Token, options.GetString("token"));
        Assert.Equal("eu", options.GetString("region"));
        Assert.True(options.GetBoolean("transactional"));

        // Routing identity and the handler's bytes are unchanged, also for code that splits the
        // extra bytes off by hand (first NUL + 16 bytes).
        var data = AttachData.Parse(attach);
        Assert.Equal(("c", 16), (data.Identity, data.AttachId.Length));
        Assert.Equal([7, 0, 8], data.Extra);
        Assert.Equal([7, 0, 8], attach[(System.Array.IndexOf(attach, (byte)0) + 17)..]);

        // Hooks see it too.
        Assert.Equal(Token, new TransactionContext { AttachOpaqueData = attach }.AttachOptions.GetString("token"));
    }

    [Fact]
    public async Task FunctionsReadTheSecretFromTheirParams()
    {
        var registry = Registry();
        var function = new OptionsProbe();
        registry.RegisterTable(function, "c");
        var service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c", Options = Options(Token) })).AttachOpaqueData;
        var bind = new BindRequest { FunctionName = function.Name, FunctionType = FunctionType.Table, Arguments = [], AttachOpaqueData = attach };

        await service.BindAsync(bind);
        await service.InitAsync(new InitRequest { BindCall = EmbeddedIpc.Encode(bind) });

        Assert.Equal((Token, Token), (function.BindToken, function.InitToken));
    }

    [Fact]
    public async Task OnTheSealedPathTheSecretIsEncryptedInsideTheSealAndFunctionsReadIt()
    {
        var root = "unit-test-signing-key-0123456789abcdef"u8.ToArray();
        var registry = Registry();
        var function = new OptionsProbe();
        registry.RegisterTable(function, "c");
        IVgiService service = OpaqueSealingProxy.Wrap(new VgiServiceImpl(registry) { SecretOptionsRoot = () => root }, new OpaqueSealer(root));

        var sealedAttach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c", Options = Options(Token) })).AttachOpaqueData;
        Assert.False(Contains(sealedAttach, Token));
        var bind = new BindRequest { FunctionName = function.Name, FunctionType = FunctionType.Table, Arguments = [], AttachOpaqueData = sealedAttach };
        await service.InitAsync(new InitRequest { BindCall = EmbeddedIpc.Encode(bind) });

        Assert.Equal(Token, function.InitToken);
        Assert.False(Contains(function.InitAttach!, Token));
    }

    [Fact]
    public async Task ASecretBlobMovedIntoAnotherAttachDoesNotOpen()
    {
        var service = new VgiServiceImpl(Registry());
        var first = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c", Options = Options(Token) })).AttachOpaqueData;
        var second = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c", Options = Options("other") })).AttachOpaqueData;

        // first's identity part (with its options section) + second's attach id.
        var moved = first[..System.Array.IndexOf(first, (byte)0)].Concat(second[System.Array.IndexOf(second, (byte)0)..]).ToArray();
        var error = Assert.ThrowsAny<Exception>(() => AttachOptions.FromAttachData(moved).GetString("token"));
        Assert.Equal("attach_opaque_data not recognized", error.Message);

        // One flipped byte in the encrypted part.
        var data = (byte[])first.Clone();
        var marker = System.Array.IndexOf(data, AttachOptionsEnvelope.Marker);
        var nul = System.Array.IndexOf(data, (byte)0);
        var section = Convert.FromBase64String(Encoding.ASCII.GetString(data, marker + 1, nul - marker - 1));
        section[^1] ^= 0x01;
        var tampered = data[..(marker + 1)].Concat(Encoding.ASCII.GetBytes(Convert.ToBase64String(section))).Concat(data[nul..]).ToArray();
        Assert.Equal("attach_opaque_data not recognized", Assert.ThrowsAny<Exception>(() => AttachOptions.FromAttachData(tampered)).Message);
    }

    [Fact]
    public async Task AnUnsuppliedSecretNeedsNoKey()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        // An unusable key file: world-readable.
        var bad = Path.Combine(_dir, "bad.key");
        Directory.CreateDirectory(_dir);
        File.WriteAllBytes(bad, new byte[32]);
        File.SetUnixFileMode(bad, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.OtherRead);
        Environment.SetEnvironmentVariable(HostLocalKey.FileVariable, bad);
        HostLocalKey.ResetCache();
        var service = new VgiServiceImpl(Registry());

        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c", Options = Options(null) })).AttachOpaqueData;
        Assert.Null(AttachOptions.FromAttachData(attach).GetString("token"));
        Assert.Equal("eu", AttachOptions.FromAttachData(attach).GetString("region"));

        // A supplied one fails the ATTACH, naming the file.
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c", Options = Options(Token) }));
        Assert.Contains(bad, error.Message);
        Assert.DoesNotContain(Token, error.Message);
    }

    [Fact]
    public void TheHostLocalKeyIsPrivateAndReused()
    {
        var path = HostLocalKey.ResolvePath();
        var key = HostLocalKey.Load();
        Assert.Equal(32, key.Length);
        Assert.Equal(key, File.ReadAllBytes(path));
        HostLocalKey.ResetCache();
        Assert.Equal(key, HostLocalKey.Load());
        if (!OperatingSystem.IsWindows())
        {
            Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(path));
            Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute, File.GetUnixFileMode(Path.GetDirectoryName(path)!));
        }
    }

    [Fact]
    public async Task ConcurrentFirstStartsAgreeOnOneKey()
    {
        var path = Path.Combine(_dir, "race", HostLocalKey.FileName);
        var keys = await Task.WhenAll(Enumerable.Range(0, 24).Select(_ => Task.Run(() => HostLocalKey.LoadOrCreate(path))));
        Assert.All(keys, k => Assert.Equal(keys[0], k));
        Assert.Equal(keys[0], File.ReadAllBytes(path));
        Assert.Single(Directory.GetFiles(Path.GetDirectoryName(path)!));
    }

    [Fact]
    public void BadKeyFilesAreRefused()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        var dir = Path.Combine(_dir, "bad");
        Directory.CreateDirectory(dir, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        var shortKey = Path.Combine(dir, "short.key");
        File.WriteAllBytes(shortKey, new byte[31]);
        File.SetUnixFileMode(shortKey, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        Assert.Contains("not exactly 32", Assert.Throws<InvalidOperationException>(() => HostLocalKey.LoadOrCreate(shortKey)).Message);

        var groupReadable = Path.Combine(dir, "group.key");
        File.WriteAllBytes(groupReadable, new byte[32]);
        File.SetUnixFileMode(groupReadable, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead);
        var error = Assert.Throws<InvalidOperationException>(() => HostLocalKey.LoadOrCreate(groupReadable));
        Assert.Contains(groupReadable, error.Message);
        Assert.Contains("VGI_ATTACH_SECRET_KEY_FILE", error.Message);

        var openDir = Path.Combine(_dir, "open");
        Directory.CreateDirectory(openDir);
        File.SetUnixFileMode(openDir, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute | UnixFileMode.OtherExecute);
        Assert.Throws<InvalidOperationException>(() => HostLocalKey.LoadOrCreate(Path.Combine(openDir, HostLocalKey.FileName)));
    }

    [Fact]
    public void TheAeadKeyIsAnHkdfSubkeyOfTheRoot()
    {
        var root = RandomNumberGenerator.GetBytes(32);
        var key = AttachSecretOptions.DeriveKey(root);
        Assert.NotEqual(root, key);
        Assert.Equal(HKDF.DeriveKey(HashAlgorithmName.SHA256, root, 32, [], Encoding.ASCII.GetBytes("vgi.attach_secret_options.v1")), key);

        var attachId = RandomNumberGenerator.GetBytes(16);
        var blob = AttachSecretOptions.Encrypt(root, attachId, "payload"u8.ToArray());
        Assert.Equal("payload"u8.ToArray(), AttachSecretOptions.Decrypt(blob, attachId));
        // The ciphertext opens under the subkey, not under the root.
        var nonce = blob.AsSpan(1 + 8, 24);
        var body = blob.AsSpan(1 + 8 + 24);
        Assert.Equal("payload"u8.ToArray(), XChaCha20Poly1305.Open(key, nonce, body, AttachSecretOptions.Aad(attachId)));
        Assert.ThrowsAny<CryptographicException>(() => XChaCha20Poly1305.Open(root, blob.AsSpan(1 + 8, 24), blob.AsSpan(1 + 8 + 24), AttachSecretOptions.Aad(attachId)));
    }

    /// <summary>Records the token its bind and init read through <c>AttachOptions</c>.</summary>
    private sealed class OptionsProbe : ITableFunction
    {
        public string? BindToken { get; private set; }

        public string? InitToken { get; private set; }

        public byte[]? InitAttach { get; private set; }

        public string Name => "options_probe";

        public Schema ArgumentsSchema { get; } = new([], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, true)], metadata: null);

        public void Bind(TableBindParams bindParams) => BindToken = bindParams.AttachOptions.GetString("token");

        public ITableFunctionProducer CreateProducer(TableInitParams initParams)
        {
            InitToken = initParams.AttachOptions.GetString("token");
            InitAttach = initParams.AttachOpaqueData;
            return new Done();
        }

        private sealed class Done : ITableFunctionProducer
        {
            public void Produce(OutputCollector output) => output.Finish();
        }
    }
}
