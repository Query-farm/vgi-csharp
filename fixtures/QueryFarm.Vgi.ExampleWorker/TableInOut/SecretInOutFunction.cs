using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// <c>secret_in_out(data TABLE)</c> — appends the resolved <c>vgi_example</c> secret's
/// <c>secret_string</c> field as a constant column on every input row. <see cref="Bind"/> requests the
/// secret DYNAMICALLY (<see cref="SecretsAccessor.Get"/>, same two-phase mechanism as
/// <c>secret_demo</c>/<c>scoped_secret_demo</c>) — exercising the intersection of a two-phase secret
/// bind with a table-in-out function's INPUT stream: the bind must retry with resolved secrets AND
/// still preserve/extend the TABLE argument's input schema. Backs <c>secret/secret_table_in_out.test</c>.
/// </summary>
public sealed class SecretInOutFunction : ITableInOutFunction
{
    private const string SecretType = "vgi_example";

    public string Name => "secret_in_out";

    public string Description => "Append a resolved secret value to each input row";

    public IReadOnlyList<string> Categories => ["transform", "secret"];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public void Bind(TableInOutBindParams bindParams) => bindParams.Secrets.Get(SecretType);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams)
    {
        var fields = bindParams.InputSchema.FieldsList.ToList();
        fields.Add(new Field("secret_string", StringType.Default, nullable: true));
        return new Schema(fields, metadata: null);
    }

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams)
    {
        var resolved = SecretArgCodec.Decode(initParams.Secrets);
        var secret = SecretArgCodec.FindByType(resolved, SecretType);
        var secretString = SecretArgCodec.FieldString(secret, "secret_string");
        return new Processor(secretString, initParams.OutputSchema);
    }

    private sealed class Processor(string? secretString, Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var arrays = new List<IArrowArray>(input.ColumnCount + 1);
            for (var i = 0; i < input.ColumnCount; i++)
            {
                arrays.Add(input.Column(i));
            }

            var builder = new StringArray.Builder();
            for (var i = 0; i < input.Length; i++)
            {
                if (secretString is null)
                {
                    builder.AppendNull();
                }
                else
                {
                    builder.Append(secretString);
                }
            }

            arrays.Add(builder.Build());
            output.Emit(new RecordBatch(outputSchema, arrays, input.Length));
        }
    }
}
