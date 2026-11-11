// The cross-SDK `txfix` fixture worker — drives vgi's test/sql/integration/writable_tx/**
// (vgi docs/writable_tx_fixtures.md). Built only with QueryFarm.Vgi's public API: attach options
// (AttachOptions: declared, and read back in every function and hook, the secret `token`
// included), per-attach transactions (AttachContext.SupportsTransactions), transaction hooks
// (TransactionHooks) and the writable-table helpers (WriteResults / WriteRequest).
//
// IMPORTANT: stdout is the wire channel — never Console.WriteLine here; use Console.Error only.
//
//     ATTACH 'txfix' AS t   (TYPE vgi, LOCATION '<this executable>', token 'tok-123');
//     ATTACH 'txfix' AS ttx (TYPE vgi, LOCATION '<this executable>', token 'tok-123', transactional true);

using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.WritableTxWorker;

var itemColumns = new Schema(
    [
        new Field("id", Int32Type.Default, nullable: false),
        new Field("name", StringType.Default, nullable: false),
        new Field("qty", Int64Type.Default, nullable: true),
        WriteResults.RowIdField(),
    ],
    metadata: null);
var itemUserColumns = WriteResults.UserColumns(itemColumns);

var labelColumns = new Schema(
    [new Field("id", Int32Type.Default, nullable: false), new Field("label", StringType.Default, nullable: true)],
    metadata: null);

var items = new CatalogTable
{
    Name = "items",
    Columns = itemColumns,
    RowIdColumn = "rowid",
    Constraints = ConstraintRecord.For(["main"], "items").NotNull("id").NotNull("name").PrimaryKey("id").Build(),
    ScanFunction = new TxScanFunction("items_scan", itemColumns, (state, tx) => state.ItemsFor(tx).Select(row =>
        (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?>
        {
            ["id"] = row.Id,
            ["name"] = row.Name,
            ["qty"] = row.Qty,
            ["rowid"] = (long)row.Id,
        })),
    UpdateFunction = new ItemsUpdateFunction(itemUserColumns),
    WriteResultModes = new Dictionary<string, string> { [WriteOperation.Update] = WriteResultMode.Changes },
};

var labels = new CatalogTable
{
    Name = "labels",
    Columns = labelColumns,
    Constraints = ConstraintRecord.For(["main"], "labels").NotNull("id").PrimaryKey("id").Build(),
    ScanFunction = new TxScanFunction("labels_scan", labelColumns, (state, _) => state.Labels.OrderBy(row => row.Id).Select(row =>
        (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?> { ["id"] = row.Id, ["label"] = row.Label })),
    // Fixture contract (protocol 3.3): labels_scan is public and callable; items_scan and
    // items_update stay table_only (the default).
    ExposeScan = true,
};

var worker = new Worker()
    .CatalogName(TxAttach.CatalogName)
    .DefaultSchema("main")
    .RegisterCatalog(TxAttach.Catalog)
    .OnAttach(TxAttach.OnAttach)
    .TransactionHooks(new TxHooks())
    .RegisterCatalogTable(items)
    .RegisterCatalogTable(labels)
    .RegisterTable(new ResetFunction())
    .RegisterTable(new WhoAmIFunction())
    .RegisterTable(new CurrentTxFunction())
    .RegisterTable(new EventsFunction())
    .RegisterTableInOut(new TagTxFunction());

await worker.RunFromArgsAsync(args);
