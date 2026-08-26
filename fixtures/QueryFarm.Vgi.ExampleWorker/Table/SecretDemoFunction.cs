using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>secret_demo()</c> — outputs the resolved <c>vgi_example</c> secret's fields as
/// <c>(key, value, arrow_type)</c> rows; zero rows when no matching secret exists.
///
/// Unlike <see cref="SecretFieldFunction"/>/<see cref="ReturnSecretValueFunction"/> (which declare
/// their secret STATICALLY via <c>[Secret]</c>, pre-resolved before the first bind), this function
/// requests its secret DYNAMICALLY from <see cref="Bind"/> via <see cref="TableBindParams.Secrets"/>'s
/// <see cref="SecretsAccessor.Get"/> — the first bind attempt always registers a pending lookup (see
/// <see cref="SecretsAccessor"/>'s doc comment), so this function's bind ALWAYS round-trips through the
/// C++ extension's two-phase secret-scope retry before returning a real <c>BindResponse</c>. Exercises
/// that path directly (<c>secret/secret_no_secret.test</c>/<c>secret_table_function.test</c>) and,
/// via <c>example.data.secret_demo_table</c> (a function-backed <c>CatalogTable</c> over this SAME
/// instance — see <c>Program.cs</c>), the intersection with table schema derivation
/// (<c>secret/secret_function_backed_table.test</c>).
/// </summary>
public sealed class SecretDemoFunction : ITableFunction
{
    private const string SecretType = "vgi_example";

    public string Name => "secret_demo";

    public string Description => "Outputs secret contents as key-value rows";

    public IReadOnlyList<string> Categories => ["generator", "secret"];

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("key", StringType.Default, nullable: true),
            new Field("value", StringType.Default, nullable: true),
            new Field("arrow_type", StringType.Default, nullable: true),
        ],
        metadata: null);

    public void Bind(TableBindParams bindParams) => bindParams.Secrets.Get(SecretType);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var resolved = SecretArgCodec.Decode(initParams.Secrets);
        var secret = SecretArgCodec.FindByType(resolved, SecretType);
        return new Producer(secret, initParams.OutputSchema);
    }

    private sealed class Producer(IReadOnlyDictionary<string, IArrowArray>? secret, Schema outputSchema) : ITableFunctionProducer
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

            if (secret is null || secret.Count == 0)
            {
                output.Finish();
                return;
            }

            var keyBuilder = new StringArray.Builder();
            var valueBuilder = new StringArray.Builder();
            var typeBuilder = new StringArray.Builder();

            foreach (var (key, array) in secret)
            {
                keyBuilder.Append(key);
                valueBuilder.Append(ScalarArgCodec.ReadScalar(array)?.ToString() ?? "");
                typeBuilder.Append(array.Data.DataType.Name);
            }

            var batch = new RecordBatch(
                outputSchema,
                [keyBuilder.Build(), valueBuilder.Build(), typeBuilder.Build()],
                keyBuilder.Length);
            output.Emit(batch);
            output.Finish();
        }
    }
}
