using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary><c>test_same_name_cached()</c> — the RESULT-CACHE member of the schema-disambiguation
/// family (see <c>Scalar.SameNameFunctions</c>/<c>TableInOut.SameNameTransformFunction</c>/
/// <c>Aggregate.SameNameAggFunction</c>): registered under BOTH <c>main</c> and <c>data</c> schemas of
/// one catalog identity, each instance tagging its single output row with its OWN schema name — a
/// cache key that dropped the schema dimension would let <c>data</c>'s scan return <c>main</c>'s
/// cached row (<c>same_name_schemas.test</c>).</summary>
public sealed class SameNameCachedFunction(string schemaName) : ITableFunction
{
    public string Name => "test_same_name_cached";

    public string SchemaName => schemaName;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("tag", StringType.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(schemaName, initParams.OutputSchema);

    private sealed class Producer(string schemaName, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                var builder = new StringArray.Builder();
                builder.Append(schemaName);
                output.Emit(new RecordBatch(outputSchema, [builder.Build()], 1), CacheMetadata.Ttl(300));
            }

            output.Finish();
        }
    }
}
