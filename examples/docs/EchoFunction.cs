using Apache.Arrow;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.DocsExamples;

public sealed class EchoFunction : ITableInOutFunction
{
    public string Name => "echo";

    public string Description => "Return each input batch unchanged";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => bindParams.InputSchema;

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor();

    private sealed class Processor : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output) => output.Emit(input);
    }
}
