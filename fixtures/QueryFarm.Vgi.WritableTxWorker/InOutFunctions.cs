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

/// <summary>
/// <c>items_update</c>, the UPDATE function of <c>main.items</c>. Its input is the SET columns plus
/// <c>rowid</c> last. Non-transactional attach: applied at once (qty must be &gt;= 0). Transactional
/// attach: staged under the transaction id, checked by the commit hook. An asynchronous processor
/// (<see cref="IAsyncTableInOutProcessor"/>).
/// </summary>
public sealed class ItemsUpdateFunction(Schema userColumns) : ITableInOutFunction
{
    public string Name => "items_update";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Table("data"), TableArgFields.Named("write_options", BinaryType.Default)], metadata: null);

    public Schema OutputSchema => userColumns;

    public FunctionStability? Stability => FunctionStability.Volatile;

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) =>
        WriteResults.Schema(WriteRequest.From(bindParams.Arguments).ResultMode, userColumns);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams)
    {
        var info = TxAttach.Info(initParams.AttachOptions);
        var tx = info.Transactional ? TxAttach.TxId(initParams.TransactionOpaqueData) : null;
        return new Processor(userColumns, WriteRequest.From(initParams.Arguments).ResultMode, info, tx);
    }

    private sealed class Processor(Schema userColumns, string mode, TxAttachInfo attach, string? tx) : IAsyncTableInOutProcessor
    {
        public Task ProcessAsync(RecordBatch input, OutputCollector output, CancellationToken cancellationToken)
        {
            var changes = WriteResults.ReadRows(input);
            var (oldRows, newRows) = TxState.Update(doc =>
            {
                var state = attach.PartitionOf(doc);
                var current = state.ItemsFor(tx).ToDictionary(row => row.Id);
                var updates = new List<(ItemRow Old, ItemRow New)>(changes.Count);
                foreach (var change in changes)
                {
                    var rowId = Convert.ToInt32(change["rowid"]);
                    var old = current.GetValueOrDefault(rowId)
                        ?? throw new InvalidOperationException($"txfix: no row with rowid {rowId}");
                    if (change.TryGetValue("id", out var id) && (id is null || Convert.ToInt32(id) != old.Id))
                    {
                        throw new InvalidOperationException("txfix: id is not updatable");
                    }

                    var updated = old with
                    {
                        Name = change.TryGetValue("name", out var name) ? (string)name! : old.Name,
                        Qty = change.TryGetValue("qty", out var qty) ? (qty is null ? null : Convert.ToInt64(qty)) : old.Qty,
                    };
                    if (tx is null && updated.Qty < 0)
                    {
                        throw new InvalidOperationException("txfix: qty must be >= 0");
                    }

                    current[rowId] = updated;
                    updates.Add((old, updated));
                }

                if (tx is null)
                {
                    state.Items = [.. state.Items.Select(row => current.GetValueOrDefault(row.Id) ?? row)];
                }
                else
                {
                    var staged = state.Pending.TryGetValue(tx, out var existing) ? existing : state.Pending[tx] = [];
                    foreach (var (_, updated) in updates)
                    {
                        staged[updated.Id] = updated;
                    }
                }

                return (updates.Select(u => Row(u.Old)).ToList(), updates.Select(u => Row(u.New)).ToList());
            });

            output.Emit(WriteResults.Batch(mode, userColumns, oldRows, newRows));
            return Task.CompletedTask;
        }

        private static IReadOnlyDictionary<string, object?>? Row(ItemRow row) =>
            new Dictionary<string, object?> { ["id"] = row.Id, ["name"] = row.Name, ["qty"] = row.Qty };
    }
}
