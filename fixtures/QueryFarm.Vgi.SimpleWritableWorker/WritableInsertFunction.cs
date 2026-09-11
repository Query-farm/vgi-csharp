using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.SimpleWritableWorker;

/// <summary>
/// A writable table's INSERT delegate — an ordinary table-in-out function the C++ extension
/// resolves by name (<see cref="Protocol.TableInfo.InsertFunction"/>) and calls with one input
/// batch per <c>Sink</c> turn, shaped to the table's VISIBLE columns only (the hidden row-id column
/// is excluded — see <see cref="WritableTableFixture"/>'s doc comment). Mints a fresh row-id per
/// inserted row.
/// </summary>
public sealed class WritableInsertFunction(string name, Schema visibleSchema, Schema fullSchema, RowStore store, bool brokenReturning) : ITableInOutFunction
{
    public string Name => name;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Table("data"), TableArgFields.Named("write_options", BinaryType.Default)],
        metadata: null);

    public Schema OutputSchema => visibleSchema;

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams)
    {
        var mode = brokenReturning ? "count" : WriteOptions.Decode(bindParams.Arguments).ResultMode;
        return WriteResults.Schema(mode, visibleSchema);
    }

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams)
    {
        var mode = brokenReturning ? "count" : WriteOptions.Decode(initParams.Arguments).ResultMode;
        return new Processor(visibleSchema, fullSchema, store, mode, brokenReturning, initParams.AttachOpaqueData);
    }

    private sealed class Processor(
        Schema visibleSchema, Schema fullSchema, RowStore store, string mode, bool brokenReturning, byte[] attachOpaqueData)
        : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var returnedRows = new List<IReadOnlyDictionary<string, object?>>(input.Length);
            for (var i = 0; i < input.Length; i++)
            {
                var values = RowCodec.ReadRow(input, i);
                var rowId = store.NextRowId(attachOpaqueData);
                var full = new Dictionary<string, object?>(values, StringComparer.Ordinal) { [WritableTableFixture.RowIdColumn] = rowId };
                store.Put(attachOpaqueData, rowId, RowCodec.BuildRow(fullSchema, full));
                returnedRows.Add(values);
            }

            // items_broken_returning advertises rows but always
            // emits the count shape regardless — the C++ side must catch the mismatch when RETURNING
            // was actually requested (test/sql/integration/simple_writable/returning_validation.test).
            if (brokenReturning)
            {
                output.Emit(WriteCount.Batch(input.Length));
                return;
            }
            output.Emit(WriteResults.Batch(
                mode,
                visibleSchema,
                Enumerable.Repeat<IReadOnlyDictionary<string, object?>?>(null, returnedRows.Count).ToList(),
                returnedRows.Cast<IReadOnlyDictionary<string, object?>?>().ToList()));
        }
    }
}
