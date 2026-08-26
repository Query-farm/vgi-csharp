using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;

namespace QueryFarm.Vgi.ExampleWorker.Accumulate;

/// <summary>
/// <c>accumulate_read(name VARCHAR)</c> — reads an existing collection's rows (including its
/// <c>_timestamp</c> column) without mutating it. Errors for an unknown collection name.
/// </summary>
public sealed class AccumulateReadFunction : ITableFunction
{
    public string Name => "accumulate_read";

    public string Description => "Reads an accumulated collection's rows without appending";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("name", StringType.Default, nullable: false)], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableBindParams bindParams) =>
        Store(bindParams.AttachOpaqueData, bindParams.Arguments).ReadPinnedSchema()
            ?? throw NotFound(bindParams.Arguments);

    public void Bind(TableBindParams bindParams)
    {
        if (!Store(bindParams.AttachOpaqueData, bindParams.Arguments).Exists)
        {
            throw NotFound(bindParams.Arguments);
        }
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new BatchListProducer(Store(initParams.AttachOpaqueData, initParams.Arguments).ReadAllSegments());

    private static AccumulateStore Store(byte[] attachOpaqueData, TableArguments arguments) =>
        new(attachOpaqueData, arguments.StringPositional(0));

    private static InvalidOperationException NotFound(TableArguments arguments) =>
        new($"accumulate_read: no accumulation named '{arguments.StringPositional(0)}' in this session.");
}
