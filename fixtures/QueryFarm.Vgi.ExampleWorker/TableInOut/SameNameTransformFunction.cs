using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// Backs <c>same_name_schemas.test</c>'s table-in-out half: <c>test_same_name_transform(data
/// TABLE)</c> registered once per schema (<c>main</c>/<c>data</c>), each instance tagging every
/// output row with ITS OWN schema name (<c>"main:1"</c>/<c>"data:1"</c>) — a bind that resolved to
/// the wrong schema's implementation reads as a plausible-looking but wrong tag, not a crash.
/// </summary>
public sealed class SameNameTransformFunction(string schemaName, string description) : ITableInOutFunction
{
    public string Name => "test_same_name_transform";

    public string SchemaName => schemaName;

    public string Description => description;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("tag", StringType.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor(schemaName, initParams.OutputSchema);

    private sealed class Processor(string schemaName, Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var column = input.Column(0);
            var builder = new StringArray.Builder();
            for (var i = 0; i < input.Length; i++)
            {
                var v = NumericArrayMath.ReadAsDouble(column, i);
                if (v is null)
                {
                    builder.AppendNull();
                    continue;
                }

                builder.Append($"{schemaName}:{(long)v.Value}");
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], input.Length));
        }
    }
}
