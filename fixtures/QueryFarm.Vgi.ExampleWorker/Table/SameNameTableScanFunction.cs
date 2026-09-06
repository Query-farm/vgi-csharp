using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary><c>test_same_name_table_scan()</c> — the DECLARATIVE-TABLE member of the
/// schema-disambiguation family (see <c>Scalar.SameNameFunctions</c>/
/// <c>TableInOut.SameNameTransformFunction</c>/<c>Aggregate.SameNameAggFunction</c>/
/// <c>Cache.SameNameCachedFunction</c>): registered under BOTH the <c>main</c> and <c>data</c>
/// schemas of one catalog identity, each instance tagging its single output row with its OWN schema
/// name. Unlike the other four — every one of which is reached by CALLING the function directly —
/// this one is only ever reached through <c>catalog_table_scan_branches_get</c>/the inlined
/// <see cref="Protocol.TableInfo.ScanFunction"/>, i.e. the RPC surface that tells the client which
/// function backs the declarative <c>test_same_name_table</c> registered in each schema. That makes
/// it the end-to-end regression guard for protocol 1.5.0's
/// <see cref="Protocol.ScanFunctionResult.SchemaName"/>/<see cref="Protocol.ScanBranch.SchemaName"/>:
/// a client that ignored those and fell back to guessing would happily serve <c>main</c>'s row for a
/// scan of <c>data</c>'s table (<c>table/same_name_schemas.test</c>).</summary>
public sealed class SameNameTableScanFunction(string schemaName) : ITableFunction
{
    public string Name => "test_same_name_table_scan";

    public string SchemaName => schemaName;

    public string Description => $"Schema-disambiguation probe; the {schemaName}-schema table producer";

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
                output.Emit(new RecordBatch(outputSchema, [builder.Build()], 1));
            }

            output.Finish();
        }
    }
}
