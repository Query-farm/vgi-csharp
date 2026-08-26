using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.SimpleWritableWorker;

/// <summary>
/// A writable table's UPDATE delegate. The C++ extension's input batch carries only the SET-clause's
/// changed columns (a call-site-dependent subset, e.g. just <c>qty</c> for
/// <c>UPDATE items SET qty = qty + 1</c>) PLUS the hidden row-id column APPENDED LAST — see
/// <c>vgi_physical_write.cpp</c>'s <c>VgiPhysicalUpdate::Sink</c>. RETURNING always surfaces the
/// table's FULL visible-column set (current, post-update values), regardless of which columns were
/// actually changed.
/// </summary>
public sealed class WritableUpdateFunction(string name, Schema visibleSchema, Schema fullSchema, string rowIdColumn, RowStore store) : ITableInOutFunction
{
    public string Name => name;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Table("data"), TableArgFields.Named("write_options", BinaryType.Default)],
        metadata: null);

    public Schema OutputSchema => visibleSchema;

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) =>
        WriteOptions.Decode(bindParams.Arguments).ReturnChunks ? visibleSchema : WriteCount.Schema;

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams)
    {
        var returnChunks = WriteOptions.Decode(initParams.Arguments).ReturnChunks;
        return new Processor(visibleSchema, fullSchema, rowIdColumn, store, returnChunks, initParams.AttachOpaqueData);
    }

    private sealed class Processor(
        Schema visibleSchema, Schema fullSchema, string rowIdColumn, RowStore store, bool returnChunks, byte[] attachOpaqueData)
        : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var returnedRows = new List<IReadOnlyDictionary<string, object?>>(returnChunks ? input.Length : 0);
            for (var i = 0; i < input.Length; i++)
            {
                var changed = RowCodec.ReadRow(input, i);
                if (changed.GetValueOrDefault(rowIdColumn) is not byte[] rowId)
                {
                    throw new InvalidOperationException($"'{rowIdColumn}' update input row is missing its row-id value.");
                }

                var existing = store.Get(attachOpaqueData, rowId)
                    ?? throw new InvalidOperationException($"UPDATE targeted a row-id that no longer exists in the store.");
                var merged = RowCodec.ReadRow(existing, 0);
                foreach (var (column, value) in changed)
                {
                    if (!string.Equals(column, rowIdColumn, StringComparison.Ordinal))
                    {
                        merged[column] = value;
                    }
                }

                store.Put(attachOpaqueData, rowId, RowCodec.BuildRow(fullSchema, merged));
                if (returnChunks)
                {
                    returnedRows.Add(merged);
                }
            }

            output.Emit(returnChunks ? RowCodec.BuildBatch(visibleSchema, returnedRows) : WriteCount.Batch(input.Length));
        }
    }
}
