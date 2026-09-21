using System.Buffers.Binary;
using System.Security.Cryptography;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary>
/// Cacheable fixtures whose results depend on a secret — back <c>cache/secret_scope.test</c>. The
/// C++ result cache keys a secret-dependent result on a FINGERPRINT of the secrets its bind resolved
/// (never their values), so a result is reused while the secret is unchanged and recomputed the
/// moment it is rotated, re-fielded or dropped. Each fixture reads the <c>vgi_example</c> secret's
/// <c>secret_string</c> and advertises cacheability — one per cache path the fingerprint has to
/// reach, each declaring its secret a different way:
///
/// <list type="bullet">
/// <item><see cref="SecretCacheNonceFunction"/> — producer; the secret is declared in
/// <see cref="ITableFunction.RequiredSecrets"/>. Also the <c>data.secret_cache_nonce</c> table.</item>
/// <item><see cref="SecretCachedScalarFunction"/> — per-value scalar; the secret is a
/// <see cref="SecretAttribute"/> parameter.</item>
/// <item><see cref="SecretCachedLateralFunction"/> — per-value blended map, called under
/// <c>LATERAL</c>; the secret is requested from <c>Bind</c> (the two-phase bind).</item>
/// </list>
///
/// Every output carries a nonce minted only when the worker really runs: an equal nonce proves a
/// cache HIT, a different one a MISS. It is RANDOM rather than a counter because a pooled worker may
/// run several processes, and a per-process counter repeats across them. (<c>cache_nonce</c> gets
/// around that with a file-backed cross-process counter — see <see cref="MonotonicNonceFunction"/> —
/// but these fixtures only ever compare nonces for equality, which randomness answers without
/// shared state.) Mirrors vgi-python's <c>vgi/_test_fixtures/secret_cache.py</c>.
/// </summary>
internal static class SecretCache
{
    /// <summary>The secret type every fixture here reads — registered by this worker's
    /// <c>RegisterSecretType("vgi_example", ...)</c>.</summary>
    public const string SecretType = "vgi_example";

    /// <summary>Long enough that the TTL never lapses mid-test.</summary>
    public const long TtlSeconds = 300;

    /// <summary>A value unique to this invocation across every process in a worker pool — 56 random
    /// bits from the OS RNG (the same width as vgi-python's <c>os.urandom(7)</c>), so it is always
    /// non-negative as a <c>BIGINT</c>.</summary>
    public static long MintNonce()
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        RandomNumberGenerator.Fill(bytes[1..]);
        return BinaryPrimitives.ReadInt64BigEndian(bytes);
    }

    /// <summary>The <c>secret_string</c> of the resolved <c>vgi_example</c> secret in a bind/init
    /// call's secrets payload, or <see langword="null"/> when no such secret resolved.</summary>
    public static string? SecretString(byte[]? secrets) =>
        SecretArgCodec.FieldString(SecretArgCodec.FindByType(SecretArgCodec.Decode(secrets), SecretType), "secret_string");

    /// <summary><c>(secret_string VARCHAR, nonce BIGINT)</c> — shared by the producer and the
    /// blended map.</summary>
    public static Schema OutputSchema() => new(
        [
            new Field("secret_string", StringType.Default, nullable: true),
            new Field("nonce", Int64Type.Default, nullable: true),
        ],
        metadata: null);
}

/// <summary><c>secret_cache_nonce()</c> — ONE row: the secret's <c>secret_string</c> (NULL with no
/// secret) and a nonce minted in <see cref="CreateProducer"/>, which runs only on a cache MISS, so the
/// nonce is stable across HITs. A rotated secret must MISS and report the new value; restoring the
/// original secret must HIT the entry it produced. Advertises <c>vgi.cache.ttl</c> on its batch.
///
/// The secret is declared STATICALLY (<see cref="RequiredSecrets"/>), so the C++ side resolves it
/// before the first bind — the path the other two fixtures in this file don't take. Also backs the
/// <c>data.secret_cache_nonce</c> table (<see cref="CacheDataTables"/>), the SAME instance registered
/// once under both names. vgi-python pre-binds that table (<c>inline_bind=True</c>, no bind RPC);
/// this SDK has no inline bind, so a table scan takes the ordinary bind-RPC path, which
/// <c>secret_scope.test</c> accepts.</summary>
public sealed class SecretCacheNonceFunction : ITableFunction
{
    public string Name => "secret_cache_nonce";

    public string SchemaName => "main";

    public string Description => "One row with a secret's value and a per-invocation nonce; cacheable per secret";

    public IReadOnlyList<string> Categories => ["generator", "cache", "secret", "testing"];

    public IReadOnlyList<RequiredSecret> RequiredSecrets { get; } = [new RequiredSecret { SecretType = SecretCache.SecretType }];

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = SecretCache.OutputSchema();

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(SecretCache.SecretString(initParams.Secrets), SecretCache.MintNonce(), initParams.OutputSchema);

    private sealed class Producer(string? secretString, long nonce, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                var secretBuilder = new StringArray.Builder();
                if (secretString is null)
                {
                    secretBuilder.AppendNull();
                }
                else
                {
                    secretBuilder.Append(secretString);
                }

                var nonceBuilder = new Int64Array.Builder().Append(nonce);
                output.Emit(
                    new RecordBatch(outputSchema, [secretBuilder.Build(), nonceBuilder.Build()], 1),
                    CacheMetadata.Ttl(SecretCache.TtlSeconds));
            }

            output.Finish();
        }
    }
}

/// <summary><c>secret_cached_scalar(value)</c> — <c>'&lt;secret_string&gt;|&lt;nonce&gt;'</c> for
/// every row, memoized per value per secret (<c>vgi.cache.per_value</c>). With no secret resolved
/// the label is <c>'|&lt;nonce&gt;'</c>: a dropped secret is a state <c>secret_scope.test</c> drives,
/// and this function must serve it rather than fail. Here that needs nothing special — a
/// <see cref="SecretAttribute"/> parameter is simply <see langword="null"/> when no such secret
/// exists (vgi-python's framework omits the argument instead, which is why its fixture defaults it).
///
/// ONE nonce per compute call, shared by the batch, so a served value keeps the nonce of the call
/// that produced it. <c>per_value</c> is a test choice, as on <c>cached_double_scalar</c>: the point
/// is coverage of the tier, not economics.</summary>
public sealed class SecretCachedScalarFunction : ScalarFn
{
    private static readonly IReadOnlyDictionary<string, string> PerValue = CacheMetadata.PerValue(SecretCache.TtlSeconds);

    public override string Name => "secret_cached_scalar";

    public override string Description => "Returns '<secret_string>|<nonce>' per value; memoized per value per secret";

    public override FunctionStability? Stability => FunctionStability.Consistent;

    public override IReadOnlyDictionary<string, string>? CacheControlMetadata => PerValue;

    private void Compute(
        [Param(Doc = "Any value; the output ignores it")] Int64Array value,
        [Secret(SecretType = SecretCache.SecretType)] IReadOnlyDictionary<string, IArrowArray>? secret,
        StringArray.Builder result)
    {
        var label = $"{SecretArgCodec.FieldString(secret, "secret_string") ?? ""}|{SecretCache.MintNonce()}";
        for (var i = 0; i < value.Length; i++)
        {
            result.Append(label);
        }
    }
}

/// <summary><c>secret_cached_lateral(x)</c> — a 1→1 blended map (see <see cref="CachedDoubleFunction"/>
/// for the blended contract) emitting the secret's <c>secret_string</c> (NULL with no secret) and a
/// nonce on every row, ONE nonce per <c>Process</c> call. Advertises <c>vgi.cache.per_value</c> so a
/// correlated <c>LATERAL</c> call is memoized per input value per secret.
///
/// Requests the secret from <see cref="Bind"/> (<see cref="SecretsAccessor.Get"/>, the two-phase
/// bind, as <c>secret_in_out</c> does) rather than declaring it — so the secret is discovered at bind
/// time, and the fingerprint must still cover it.</summary>
public sealed class SecretCachedLateralFunction : ITableInOutFunction
{
    public string Name => "secret_cached_lateral";

    public string SchemaName => "main";

    public string Description => "Blended map emitting a secret's value and a per-call nonce; memoized per secret";

    public IReadOnlyList<string> Categories => ["blended", "cache", "secret", "test"];

    public bool InputFromArgs => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("x", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = SecretCache.OutputSchema();

    public void Bind(TableInOutBindParams bindParams) => bindParams.Secrets.Get(SecretCache.SecretType);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) =>
        new Processor(SecretCache.SecretString(initParams.Secrets), initParams.OutputSchema);

    private sealed class Processor(string? secretString, Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var nonce = SecretCache.MintNonce();
            var secretBuilder = new StringArray.Builder();
            var nonceBuilder = new Int64Array.Builder();
            for (var i = 0; i < input.Length; i++)
            {
                if (secretString is null)
                {
                    secretBuilder.AppendNull();
                }
                else
                {
                    secretBuilder.Append(secretString);
                }

                nonceBuilder.Append(nonce);
            }

            output.Emit(
                new RecordBatch(outputSchema, [secretBuilder.Build(), nonceBuilder.Build()], input.Length),
                CacheMetadata.PerValue(SecretCache.TtlSeconds));
        }
    }
}
