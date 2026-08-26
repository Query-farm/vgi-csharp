using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;

namespace QueryFarm.Vgi.ExampleWorker.CopyFormats;

/// <summary>
/// <c>COPY ... TO (FORMAT 'secret_lines_out')</c> — requests a <c>vgi_example</c> secret scoped by
/// the destination path (a DYNAMIC lookup — see <see cref="SecretsAccessor.Get"/>'s two-phase-retry
/// doc comment) and writes its resolved <c>api_key</c> plus the row count into the file. No
/// matching secret is a silent miss (<c>api_key=NONE</c>), not an error. Backs
/// test/sql/integration/copy_to/secrets.test.
/// </summary>
public sealed class SecretLinesOutFunction : CopyToFunction
{
    private const string DefaultSecretType = "vgi_example";
    private const string CountNamespace = "secret_lines";
    private const string CountKey = "count";

    public override string Name => "secret_lines_out";

    public override string Description => "Secret-forwarding writer for tests";

    public override Schema ArgumentsSchema { get; } = new(
        [TableArgFields.NamedWithDoc("secret_type", StringType.Default, "The VGI secret type to request")],
        metadata: null);

    protected override void OnBind(TableInOutBindParams bindParams, CopyToContext copyTo) =>
        bindParams.Secrets.Get(bindParams.Arguments.StringNamed("secret_type", DefaultSecretType), scope: copyTo.FilePath);

    protected override void Write(RecordBatch batch, TableBufferingProcessParams processParams, string filePath) =>
        processParams.Storage.Append(CountNamespace, CountKey, BitConverter.GetBytes((long)batch.Length));

    protected override void Close(TableBufferingCombineParams combineParams, string filePath)
    {
        var totalRows = combineParams.Storage.ScanLog(CountNamespace, CountKey).Sum(bytes => BitConverter.ToInt64(bytes));
        var secretType = combineParams.Arguments.StringNamed("secret_type", DefaultSecretType);
        var resolved = SecretArgCodec.Decode(combineParams.Secrets);
        var secret = SecretArgCodec.FindByType(resolved, secretType);
        var apiKey = SecretArgCodec.FieldString(secret, "api_key") ?? "NONE";

        File.WriteAllText(filePath, $"api_key={apiKey}\nrows={totalRows}\n");
    }
}
