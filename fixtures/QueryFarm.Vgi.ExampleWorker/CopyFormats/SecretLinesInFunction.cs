using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;

namespace QueryFarm.Vgi.ExampleWorker.CopyFormats;

/// <summary>
/// <c>COPY ... FROM (FORMAT 'secret_lines_in')</c> — ignores the source file's contents entirely
/// and emits a single row holding the resolved <c>vgi_example</c> secret's <c>api_key</c> (scoped
/// by the SOURCE path — a dynamic lookup, see <see cref="SecretsAccessor.Get"/>), or
/// <c>"NONE"</c> when no secret matches. Backs test/sql/integration/copy_from/secrets.test.
/// </summary>
public sealed class SecretLinesInFunction : CopyFromFunction
{
    private const string DefaultSecretType = "vgi_example";

    public override string Name => "secret_lines_in";

    public override string Description => "Secret-forwarding reader for tests";

    public override Schema ArgumentsSchema { get; } = new(
        [TableArgFields.NamedWithDoc("secret_type", StringType.Default, "The VGI secret type to request")],
        metadata: null);

    protected override void OnBind(TableBindParams bindParams, CopyFromContext copyFrom) =>
        bindParams.Secrets.Get(bindParams.Arguments.StringNamed("secret_type", DefaultSecretType), scope: copyFrom.FilePath);

    protected override void Read(string path, TableInitParams initParams, Schema expectedSchema, Action<RecordBatch> emit)
    {
        var secretType = initParams.Arguments.StringNamed("secret_type", DefaultSecretType);
        var resolved = SecretArgCodec.Decode(initParams.Secrets);
        var secret = SecretArgCodec.FindByType(resolved, secretType);
        var apiKey = SecretArgCodec.FieldString(secret, "api_key") ?? "NONE";

        var builder = new StringArray.Builder();
        builder.Append(apiKey);
        emit(new RecordBatch(expectedSchema, [builder.Build()], 1));
    }
}
