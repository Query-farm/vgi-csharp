using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>scoped_secret_demo(path)</c> — resolves the <c>vgi_example</c> secret whose SCOPE longest-prefix
/// matches <c>path</c> (a DYNAMIC lookup: the scope value comes from this call's OWN argument, so it
/// can only be resolved by round-tripping through the C++ extension's SecretManager — see
/// <see cref="Internal.SecretsAccessor"/>'s doc comment). Emits one row: <c>path</c> echoed back as
/// <c>scope</c>, <c>found</c> (whether a matching secret was resolved), and <c>secret_keys</c> (its
/// field names, comma-joined). Backs <c>secret/secret_scoped.test</c>.
/// </summary>
public sealed class ScopedSecretDemoFunction : ITableFunction
{
    private const string SecretType = "vgi_example";

    public string Name => "scoped_secret_demo";

    public string Description => "Demo: resolves scoped secret based on argument";

    public IReadOnlyList<string> Categories => ["generator", "secret"];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("path", StringType.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("scope", StringType.Default, nullable: true),
            new Field("found", BooleanType.Default, nullable: true),
            new Field("secret_keys", StringType.Default, nullable: true),
        ],
        metadata: null);

    public void Bind(TableBindParams bindParams) =>
        bindParams.Secrets.Get(SecretType, scope: bindParams.Arguments.StringPositional(0));

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var path = initParams.Arguments.StringPositional(0);
        var resolved = SecretArgCodec.Decode(initParams.Secrets);
        var secret = SecretArgCodec.FindByType(resolved, SecretType);
        return new Producer(path, secret, initParams.OutputSchema);
    }

    private sealed class Producer(string path, IReadOnlyDictionary<string, IArrowArray>? secret, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted)
            {
                output.Finish();
                return;
            }

            _emitted = true;

            var found = secret is not null;
            var keys = found ? string.Join(",", secret!.Keys) : "";

            var batch = new RecordBatch(
                outputSchema,
                [
                    new StringArray.Builder().Append(path).Build(),
                    new BooleanArray.Builder().Append(found).Build(),
                    new StringArray.Builder().Append(keys).Build(),
                ],
                1);
            output.Emit(batch);
            output.Finish();
        }
    }
}

/// <summary>
/// <c>multi_secret_demo(path)</c> — requests the <c>vgi_example</c> secret for BOTH
/// <c>s3://bucket-a/</c> and <c>s3://bucket-b/</c> scopes in ONE bind (two <see cref="SecretsAccessor.Get"/>
/// calls in the same, non-retry <see cref="Bind"/> — both pending lookups travel in the SAME
/// secret-scope-request round trip), then at read time selects whichever resolved secret's scope
/// longest-prefix-matches the ACTUAL call-site <c>path</c> via
/// <see cref="SecretArgCodec.ForScopeOfType"/> and returns its <c>api_key</c> (empty when neither
/// scope matches). Proves resolved secrets are kept keyed by NAME (so two same-type secrets both
/// survive one bind) — backs <c>secret/secret_multi_scope.test</c>.
/// </summary>
public sealed class MultiSecretDemoFunction : ITableFunction
{
    private const string SecretType = "vgi_example";

    public string Name => "multi_secret_demo";

    public string Description => "Demo: two same-type scoped secrets resolved in one bind";

    public IReadOnlyList<string> Categories => ["generator", "secret"];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("path", StringType.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("api_key", StringType.Default, nullable: true)], metadata: null);

    public void Bind(TableBindParams bindParams)
    {
        bindParams.Secrets.Get(SecretType, scope: "s3://bucket-a/");
        bindParams.Secrets.Get(SecretType, scope: "s3://bucket-b/");
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var path = initParams.Arguments.StringPositional(0);
        var resolved = SecretArgCodec.Decode(initParams.Secrets);
        var secret = SecretArgCodec.ForScopeOfType(resolved, path, SecretType);
        var apiKey = SecretArgCodec.FieldString(secret, "api_key") ?? "";
        return new Producer(apiKey, initParams.OutputSchema);
    }

    private sealed class Producer(string apiKey, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted)
            {
                output.Finish();
                return;
            }

            _emitted = true;
            var batch = new RecordBatch(outputSchema, [new StringArray.Builder().Append(apiKey).Build()], 1);
            output.Emit(batch);
            output.Finish();
        }
    }
}
