using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.WritableTxWorker;

/// <summary><c>tag_tx(TABLE)</c>: every input column, plus <c>tx_id</c> — the transaction id this
/// processor saw (lowercase hex, NULL for none).</summary>
public sealed class TagTxFunction : ITableInOutFunction
{
    public string Name => "tag_tx";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public FunctionStability? Stability => FunctionStability.Volatile;

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) =>
        new([.. bindParams.InputSchema.FieldsList, new Field("tx_id", StringType.Default, true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) =>
        new Processor(initParams.OutputSchema, TxAttach.TxId(initParams.TransactionOpaqueData));

    private sealed class Processor(Schema outputSchema, string? tx) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var tag = new StringArray.Builder();
            for (var i = 0; i < input.Length; i++)
            {
                if (tx is null) tag.AppendNull();
                else tag.Append(tx);
            }

            output.Emit(new RecordBatch(outputSchema, [.. Enumerable.Range(0, input.ColumnCount).Select(input.Column), tag.Build()], input.Length));
        }
    }
}
