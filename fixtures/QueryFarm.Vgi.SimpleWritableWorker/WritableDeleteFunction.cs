using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.SimpleWritableWorker;

/// <summary>
/// A writable table's DELETE delegate. The C++ extension's input batch is a SINGLE column — just
/// the hidden row-id — one value per row to delete (<c>vgi_physical_write.cpp</c>'s
/// <c>VgiPhysicalDelete::Sink</c>). RETURNING surfaces the FULL visible-column set of each deleted
/// row, read back from the store before removing it.
/// </summary>
public sealed class WritableDeleteFunction(string name, Schema visibleSchema, RowStore store) : ITableInOutFunction
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
        return new Processor(visibleSchema, store, returnChunks, initParams.AttachOpaqueData);
    }

    private sealed class Processor(Schema visibleSchema, RowStore store, bool returnChunks, byte[] attachOpaqueData) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var returnedRows = new List<IReadOnlyDictionary<string, object?>>(returnChunks ? input.Length : 0);
            long deleted = 0;
            for (var i = 0; i < input.Length; i++)
            {
                if (ScalarArgCodec.ReadScalar(input.Column(0), i) is not byte[] rowId)
                {
                    continue;
                }

                var existing = store.Get(attachOpaqueData, rowId);
                if (existing is null)
                {
                    continue;
                }

                store.Delete(attachOpaqueData, rowId);
                deleted++;
                if (returnChunks)
                {
                    returnedRows.Add(RowCodec.ReadRow(existing, 0));
                }
            }

            output.Emit(returnChunks ? RowCodec.BuildBatch(visibleSchema, returnedRows) : WriteCount.Batch(deleted));
        }
    }
}
