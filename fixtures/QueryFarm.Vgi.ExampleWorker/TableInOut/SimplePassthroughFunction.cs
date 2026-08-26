using Apache.Arrow;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// A name/description-parameterized passthrough table-in-out function — registration-surface
/// stand-ins for canonical fixtures whose full behavioral parity is out of scope for this pass
/// (<c>function_registration.test</c> only pins their name/TABLE-argument shape, not their SQL
/// behavior, which lives in test files not yet ported). Each instance is otherwise a well-behaved,
/// correct no-finalize echo — safe to attach even though it doesn't implement its namesake's full
/// original semantics (setting-based filtering, row repetition, cancellable slow processing).
/// </summary>
public sealed class SimplePassthroughFunction(string name, string description) : ITableInOutFunction
{
    public string Name => name;

    public string Description => description;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => bindParams.InputSchema;

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new PassthroughProcessor();

    private sealed class PassthroughProcessor : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output) => output.Emit(input);
    }
}
