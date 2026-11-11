using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.WritableTxWorker;

/// <summary>What one write batch did: the before/after image of every row it wrote (the old image
/// is null for an insert, the new one for a delete).</summary>
public sealed record WriteOutcome(
    List<IReadOnlyDictionary<string, object?>?> OldRows,
    List<IReadOnlyDictionary<string, object?>?> NewRows);

/// <summary>
/// The shared shape of <c>items_insert</c>, <c>items_update</c> and <c>items_delete</c>: a table
/// in-out function over the rows DuckDB sends, binding the result schema of the requested
/// <see cref="WriteResultMode"/>, with a finalize stage (<see cref="ITableInOutFunction.HasFinalize"/>).
/// Each batch is checked in full and then applied (non-transactional attach) or staged under the
/// transaction id (transactional attach) in one <see cref="TxState.Update{T}"/>. The processor, which
/// the SDK keeps for the call's finalize stage, carries the running row count there; finalize emits
/// nothing and records the call in <c>writes()</c>.
/// </summary>
public abstract class ItemsWriteFunction(Schema userColumns) : ITableInOutFunction
{
    public abstract string Name { get; }

    /// <summary>The <c>writes()</c> operation: <c>insert</c>, <c>update</c> or <c>delete</c>.</summary>
    protected abstract string Op { get; }

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Table("data"), TableArgFields.Named("write_options", BinaryType.Default)], metadata: null);

    public Schema OutputSchema => userColumns;

    public FunctionStability? Stability => FunctionStability.Volatile;

    public bool HasFinalize => true;

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) =>
        WriteResults.Schema(WriteRequest.From(bindParams.Arguments).ResultMode, userColumns);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams)
    {
        var info = TxAttach.Info(initParams.AttachOptions);
        var tx = info.Transactional ? TxAttach.TxId(initParams.TransactionOpaqueData) : null;
        return new Processor(this, WriteRequest.From(initParams.Arguments).ResultMode, info, tx);
    }

    /// <summary>Checks every row of <paramref name="rows"/>, then applies or stages them all, on
    /// <paramref name="state"/> as transaction <paramref name="tx"/> (null: non-transactional) sees it.
    /// Throws, changing nothing, on the first refused row.</summary>
    public abstract WriteOutcome Apply(Partition state, string? tx, IReadOnlyList<Dictionary<string, object?>> rows);

    protected static IReadOnlyDictionary<string, object?> Row(ItemRow row) =>
        new Dictionary<string, object?> { ["id"] = row.Id, ["name"] = row.Name, ["qty"] = row.Qty };

    /// <summary>Writes <paramref name="rows"/> as the attach requires: into the committed rows on
    /// the non-transactional attach, into the transaction's staged images otherwise.</summary>
    protected static void Write(Partition state, string? tx, IEnumerable<(int Id, ItemRow? Image)> rows)
    {
        var images = rows.ToDictionary(row => row.Id, row => row.Image);
        if (tx is null)
        {
            state.Apply(images);
            return;
        }

        var staged = state.Staged(tx);
        foreach (var (id, image) in images)
        {
            staged[id] = image;
        }
    }

    private sealed class Processor(ItemsWriteFunction function, string mode, TxAttachInfo attach, string? tx) : IAsyncTableInOutProcessor
    {
        private long _rows;

        public Task ProcessAsync(RecordBatch input, OutputCollector output, CancellationToken cancellationToken)
        {
            var rows = WriteResults.ReadRows(input);
            var outcome = TxState.Update(doc => function.Apply(attach.PartitionOf(doc), tx, rows));
            _rows += outcome.NewRows.Count;
            output.Emit(WriteResults.Batch(mode, function.OutputSchema, outcome.OldRows, outcome.NewRows));
            return Task.CompletedTask;
        }

        public Task FinalizeAsync(OutputCollector output, CancellationToken cancellationToken)
        {
            TxState.Update(doc =>
            {
                attach.PartitionOf(doc).AddWrite(function.Op, tx, _rows);
                return 0;
            });
            output.Finish();
            return Task.CompletedTask;
        }
    }
}

/// <summary>
/// <c>items_insert</c>: DuckDB sends every user column (<c>id</c>, <c>name</c>, <c>qty</c>), with
/// defaults filled in. A NULL id or name, an id the call can already see (or repeated within the
/// batch), and on the non-transactional attach a negative qty, refuse the batch.
/// </summary>
public sealed class ItemsInsertFunction(Schema userColumns) : ItemsWriteFunction(userColumns)
{
    public override string Name => "items_insert";

    protected override string Op => "insert";

    public override WriteOutcome Apply(Partition state, string? tx, IReadOnlyList<Dictionary<string, object?>> rows)
    {
        var visible = state.ItemsFor(tx).Select(row => row.Id).ToHashSet();
        var inserted = new List<ItemRow>(rows.Count);
        foreach (var row in rows)
        {
            if (row.GetValueOrDefault("id") is not { } rawId)
            {
                throw new InvalidOperationException("txfix: id required");
            }

            if (row.GetValueOrDefault("name") is not string name)
            {
                throw new InvalidOperationException("txfix: name required");
            }

            var id = Convert.ToInt32(rawId);
            if (!visible.Add(id))
            {
                throw new InvalidOperationException($"txfix: duplicate id {id}");
            }

            var qty = row.GetValueOrDefault("qty") is { } rawQty ? Convert.ToInt64(rawQty) : (long?)null;
            if (tx is null && qty < 0)
            {
                throw new InvalidOperationException("txfix: qty must be >= 0");
            }

            inserted.Add(new ItemRow(id, name, qty));
        }

        Write(state, tx, inserted.Select(row => (row.Id, (ItemRow?)row)));
        return new WriteOutcome([.. inserted.Select(_ => (IReadOnlyDictionary<string, object?>?)null)], [.. inserted.Select(Row)]);
    }
}

/// <summary>
/// <c>items_update</c>: DuckDB sends the SET columns plus <c>rowid</c> last (every column with
/// RETURNING). <c>id</c> is not updatable; a negative qty is refused at once on the
/// non-transactional attach and by the commit hook on the transactional one.
/// </summary>
public sealed class ItemsUpdateFunction(Schema userColumns) : ItemsWriteFunction(userColumns)
{
    public override string Name => "items_update";

    protected override string Op => "update";

    public override WriteOutcome Apply(Partition state, string? tx, IReadOnlyList<Dictionary<string, object?>> rows)
    {
        var current = state.ItemsFor(tx).ToDictionary(row => row.Id);
        var updates = new List<(ItemRow Old, ItemRow New)>(rows.Count);
        foreach (var change in rows)
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
                Name = change.TryGetValue("name", out var name)
                    ? name as string ?? throw new InvalidOperationException("txfix: name required")
                    : old.Name,
                Qty = change.TryGetValue("qty", out var qty) ? (qty is null ? null : Convert.ToInt64(qty)) : old.Qty,
            };
            if (tx is null && updated.Qty < 0)
            {
                throw new InvalidOperationException("txfix: qty must be >= 0");
            }

            current[rowId] = updated;
            updates.Add((old, updated));
        }

        Write(state, tx, updates.Select(u => (u.New.Id, (ItemRow?)u.New)));
        return new WriteOutcome([.. updates.Select(u => (IReadOnlyDictionary<string, object?>?)Row(u.Old))], [.. updates.Select(u => (IReadOnlyDictionary<string, object?>?)Row(u.New))]);
    }
}

/// <summary>
/// <c>items_delete</c>: DuckDB sends <c>rowid</c> only (rowid = id). A rowid with no visible row
/// is a no-op, and one repeated within the call is deleted and counted once.
/// </summary>
public sealed class ItemsDeleteFunction(Schema userColumns) : ItemsWriteFunction(userColumns)
{
    public override string Name => "items_delete";

    protected override string Op => "delete";

    public override WriteOutcome Apply(Partition state, string? tx, IReadOnlyList<Dictionary<string, object?>> rows)
    {
        var visible = state.ItemsFor(tx).ToDictionary(row => row.Id);
        var deleted = new List<ItemRow>(rows.Count);
        foreach (var row in rows)
        {
            if (row.GetValueOrDefault("rowid") is { } rowId && visible.Remove(Convert.ToInt32(rowId), out var old))
            {
                deleted.Add(old);
            }
        }

        Write(state, tx, deleted.Select(row => (row.Id, (ItemRow?)null)));
        return new WriteOutcome([.. deleted.Select(Row)], [.. deleted.Select(_ => (IReadOnlyDictionary<string, object?>?)null)]);
    }
}

/// <summary><c>writes()</c>: one row per finished write call of the partition, in order.</summary>
public sealed class WritesFunction : OneShotFunction
{
    public override string Name => "writes";

    public override Schema OutputSchema { get; } = new(
        [
            new Field("seq", Int64Type.Default, true),
            new Field("op", StringType.Default, true),
            new Field("tx_id", StringType.Default, true),
            new Field("rows", Int64Type.Default, true),
            new Field("finalized", BooleanType.Default, true),
        ],
        metadata: null);

    protected override IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows(TableInitParams initParams) =>
        TxState.Read(state => TxAttach.Info(initParams.AttachOptions).PartitionOf(state).Writes
            .Select(w => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?>
            {
                ["seq"] = w.Seq,
                ["op"] = w.Op,
                ["tx_id"] = w.TxId,
                ["rows"] = w.Rows,
                ["finalized"] = w.Finalized,
            })
            .ToList());
}
