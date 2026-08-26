using Apache.Arrow;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// A zero-argument table function that emits one fixed, pre-built <see cref="RecordBatch"/> and
/// finishes — the read path a real catalog table backs itself with (M6's "function-backed table"
/// pattern, see <c>docs/catalog-interface.md</c>): <c>Worker.RegisterCatalogTable</c> both registers
/// the <see cref="QueryFarm.Vgi.Catalog.CatalogTable"/> AND (via <see cref="CatalogTable.ScanFunction"/>)
/// this same zero-arg function, independently callable as <c>schema.name()</c> too.
/// </summary>
public sealed class StaticRowsFunction(string name, string schemaName, RecordBatch data, long? cardinality = null) : ITableFunction
{
    public string Name => name;

    public string SchemaName => schemaName;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema => data.Schema;

    /// <summary>Best-effort cardinality reported via the per-bind <c>table_function_cardinality</c>
    /// RPC — <see langword="null"/> (the default) unless the caller opts in, since most fixtures
    /// using this class don't need it (see <see cref="DataSchemaTables.TenThousandTable"/>'s
    /// "legacy path" for a caller that does).</summary>
    public long? Cardinality(TableBindParams bindParams) => cardinality;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(data);

    private sealed class Producer(RecordBatch data) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                output.Emit(data);
            }

            output.Finish();
        }
    }
}
