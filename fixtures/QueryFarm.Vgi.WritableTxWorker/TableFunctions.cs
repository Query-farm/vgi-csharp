using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.WritableTxWorker;

/// <summary>A no-argument VOLATILE table function that emits one batch, built at produce time
/// (so it reads the transaction id the produce call runs under, not only bind's).</summary>
public abstract class OneShotFunction : ITableFunction
{
    public abstract string Name { get; }

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public abstract Schema OutputSchema { get; }

    public FunctionStability? Stability => FunctionStability.Volatile;

    /// <summary>The rows to emit, keyed by column name.</summary>
    protected abstract IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows(TableInitParams initParams);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(this, initParams);

    private sealed class Producer(OneShotFunction function, TableInitParams initParams) : ITableFunctionProducer
    {
        private bool _done;

        public void Produce(OutputCollector output)
        {
            if (!_done)
            {
                _done = true;
                output.Emit(WriteResults.BuildBatch(function.OutputSchema, function.Rows(initParams)));
            }

            output.Finish();
        }
    }
}

/// <summary><c>reset()</c>: initial rows back, pending changes and events cleared.</summary>
public sealed class ResetFunction : OneShotFunction
{
    public override string Name => "reset";

    public override Schema OutputSchema { get; } = new([new Field("ok", BooleanType.Default, true)], metadata: null);

    protected override IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows(TableInitParams initParams)
    {
        var attach = TxAttach.Info(initParams.AttachOptions);
        TxState.Update(state =>
        {
            var partition = attach.PartitionOf(state);
            var fresh = Partition.Initial();
            partition.Items = fresh.Items;
            partition.Labels = fresh.Labels;
            partition.Pending.Clear();
            partition.Events.Clear();
            partition.Seq = 0;
            return 0;
        });
        return [new Dictionary<string, object?> { ["ok"] = true }];
    }
}

/// <summary><c>whoami()</c>: the attach's options as a function sees them, and whether the token's
/// bytes appear in the attach data the SDK handed over (they must not: the SDK encrypts it).</summary>
public sealed class WhoAmIFunction : OneShotFunction
{
    public override string Name => "whoami";

    public override Schema OutputSchema { get; } = new(
        [
            new Field("token", StringType.Default, true),
            new Field("transactional", BooleanType.Default, true),
            new Field("token_in_attach_data", BooleanType.Default, true),
        ],
        metadata: null);

    protected override IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows(TableInitParams initParams)
    {
        var info = TxAttach.Info(initParams.AttachOptions);
        var inAttachData = info.Token is { Length: > 0 } token
            && initParams.AttachOpaqueData.AsSpan().IndexOf(System.Text.Encoding.UTF8.GetBytes(token)) >= 0;
        return [new Dictionary<string, object?> { ["token"] = info.Token, ["transactional"] = info.Transactional, ["token_in_attach_data"] = inAttachData }];
    }
}

/// <summary><c>current_tx()</c>: the plaintext transaction id this produce call received.</summary>
public sealed class CurrentTxFunction : OneShotFunction
{
    public override string Name => "current_tx";

    public override Schema OutputSchema { get; } = new(
        [new Field("in_tx", BooleanType.Default, true), new Field("tx_id", StringType.Default, true)], metadata: null);

    protected override IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows(TableInitParams initParams)
    {
        var tx = TxAttach.TxId(initParams.TransactionOpaqueData);
        return [new Dictionary<string, object?> { ["in_tx"] = tx is not null, ["tx_id"] = tx }];
    }
}

/// <summary><c>events()</c>: the begin/commit/rollback log, in order.</summary>
public sealed class EventsFunction : OneShotFunction
{
    public override string Name => "events";

    public override Schema OutputSchema { get; } = new(
        [
            new Field("seq", Int64Type.Default, true),
            new Field("kind", StringType.Default, true),
            new Field("tx_id", StringType.Default, true),
            new Field("ok", BooleanType.Default, true),
        ],
        metadata: null);

    protected override IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows(TableInitParams initParams) =>
        TxState.Read(state => TxAttach.Info(initParams.AttachOptions).PartitionOf(state).Events
            .Select(e => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?>
            {
                ["seq"] = e.Seq,
                ["kind"] = e.Kind,
                ["tx_id"] = e.TxId,
                ["ok"] = e.Ok,
            })
            .ToList());
}

/// <summary>
/// The scan of <c>items</c> (with its <c>rowid</c> virtual column, whose value is <c>id</c>) or of
/// <c>labels</c>. Projection pushdown, which a row-id column requires. On a transactional attach a
/// scan inside a transaction sees that transaction's staged changes.
/// </summary>
public sealed class TxScanFunction(string name, Schema columns, Func<Partition, string?, IEnumerable<IReadOnlyDictionary<string, object?>>> rows)
    : ITableFunction
{
    private readonly Func<Partition, string?, IEnumerable<IReadOnlyDictionary<string, object?>>> _rows = rows;

    public string Name => name;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema => columns;

    public bool? ProjectionPushdown => true;

    public FunctionStability? Stability => FunctionStability.Volatile;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(this, initParams);

    private sealed class Producer(TxScanFunction scan, TableInitParams initParams) : ITableFunctionProducer
    {
        private bool _done;

        public void Produce(OutputCollector output)
        {
            if (!_done)
            {
                _done = true;
                var attach = TxAttach.Info(initParams.AttachOptions);
                var tx = attach.Transactional ? TxAttach.TxId(initParams.TransactionOpaqueData) : null;
                var data = TxState.Read(state => scan._rows(attach.PartitionOf(state), tx).ToList());
                output.Emit(WriteResults.BuildBatch(initParams.ProjectedSchema, data));
            }

            output.Finish();
        }
    }
}
