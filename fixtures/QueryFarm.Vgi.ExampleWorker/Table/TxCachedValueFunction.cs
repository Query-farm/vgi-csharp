using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>tx_cached_value(key, seed)</c> — backs <c>table/transaction_storage.test</c>. A single-row
/// table function whose value is cached per <c>(SQL transaction, key)</c> using
/// <see cref="Table.TableBindParams.TransactionOpaqueData"/>/<see cref="Table.TableInitParams.TransactionOpaqueData"/>
/// (populated only because <c>VgiServiceImpl.CatalogAttachAsync</c> now advertises
/// <c>SupportsTransactions</c> for the <c>"example"</c> catalog identity) as a
/// <see cref="FunctionStorage"/> key: the FIRST call for a given key within one transaction stores
/// its <c>seed</c> argument; every subsequent call for the SAME key in the SAME transaction ignores
/// its own <c>seed</c> and returns the stored value instead. <c>VgiServiceImpl</c>'s
/// <c>CatalogTransactionCommit/RollbackAsync</c> overrides clear that transaction's storage, so a
/// NEW transaction (or an autocommit statement, which is its own one-shot transaction with a fresh,
/// immediately-discarded id) always starts empty — the seed wins again.
///
/// <para>Resolution happens TWICE — once in <see cref="Bind"/> (so the value is durably stored the
/// first time a key is touched, even if nothing ever reads the producer's output) and once again in
/// <see cref="CreateProducer"/> (so whichever pooled worker process ends up serving <c>init</c> —
/// not necessarily the same process that served <c>bind</c> — reads the correct already-resolved
/// value). This works with no bind→init opaque-data handoff because <see cref="FunctionStorage"/>
/// is already a cross-PROCESS durable store (file-backed under the OS temp directory): by the time
/// <c>init</c> runs, <see cref="Bind"/> has already resolved and persisted the value under this
/// transaction's key, so re-reading it is enough — no need to thread anything through
/// <c>BindResponse.opaque_data</c>.</para>
/// </summary>
public sealed class TxCachedValueFunction : ITableFunction
{
    private const string Namespace = "tx_cached_value";

    public string Name => "tx_cached_value";

    public string Description => "Single-row value cached per (SQL transaction, key)";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("key", StringType.Default), TableArgFields.Positional("seed", Int64Type.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("v", Int64Type.Default, nullable: false)], metadata: null);

    public void Bind(TableBindParams bindParams)
    {
        if (bindParams.TransactionOpaqueData.Length == 0)
        {
            // Outside a transaction (autocommit) — nothing to cache; every call is independent.
            return;
        }

        var key = bindParams.Arguments.StringPositional(0);
        var seed = bindParams.Arguments.Int64(1);
        var storage = new FunctionStorage(bindParams.TransactionOpaqueData);
        if (storage.ReadSingle(Namespace, key) is null)
        {
            storage.WriteSingle(Namespace, key, BitConverter.GetBytes(seed));
        }
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var key = initParams.Arguments.StringPositional(0);
        var value = initParams.Arguments.Int64(1);

        if (initParams.TransactionOpaqueData.Length > 0)
        {
            var stored = new FunctionStorage(initParams.TransactionOpaqueData).ReadSingle(Namespace, key);
            if (stored is not null)
            {
                value = BitConverter.ToInt64(stored);
            }
        }

        return new Producer(value, initParams.OutputSchema);
    }

    private sealed class Producer(long value, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _done;

        public void Produce(OutputCollector output)
        {
            if (_done)
            {
                output.Finish();
                return;
            }

            _done = true;
            var builder = new Int64Array.Builder();
            builder.Append(value);
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], 1));
            output.Finish();
        }
    }
}
