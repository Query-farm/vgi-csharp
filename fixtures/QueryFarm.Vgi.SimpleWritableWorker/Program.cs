// M6's simple in-memory writable-catalog fixture worker — drives
// ~/Development/vgi's test/sql/integration/simple_writable/**.
//
// IMPORTANT: stdout is the wire channel — never Console.WriteLine here; use Console.Error only.
//
// Built and pointed at by DuckDB via:
//
//     ATTACH 'simple_writable' AS w (TYPE vgi, LOCATION '<path to this executable>');

using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi;
using QueryFarm.Vgi.SimpleWritableWorker;

var itemsSchema = new Schema(
    [
        new Field("id", Int64Type.Default, nullable: true),
        new Field("name", StringType.Default, nullable: true),
        new Field("qty", Int64Type.Default, nullable: true),
    ],
    metadata: null);

var brokenSchema = new Schema(
    [
        new Field("id", Int64Type.Default, nullable: true),
        new Field("name", StringType.Default, nullable: true),
    ],
    metadata: null);

var items = WritableTableFixture.Build(
    "items", itemsSchema, notNullColumns: [],
    supportsInsert: true, supportsUpdate: true, supportsDelete: true, supportsReturning: true);

var itemsNoReturning = WritableTableFixture.Build(
    "items_no_returning", itemsSchema, notNullColumns: [],
    supportsInsert: true, supportsUpdate: true, supportsDelete: true, supportsReturning: false);

var itemsInsertOnly = WritableTableFixture.Build(
    "items_insert_only", itemsSchema, notNullColumns: [],
    supportsInsert: true, supportsUpdate: false, supportsDelete: false, supportsReturning: false);

var itemsBrokenReturning = WritableTableFixture.Build(
    "items_broken_returning", brokenSchema, notNullColumns: [],
    supportsInsert: true, supportsUpdate: false, supportsDelete: false, supportsReturning: true,
    brokenReturning: true);

var worker = new Worker()
    .CatalogName("simple_writable")
    .DefaultSchema("main")
    .RegisterCatalogTable(items)
    .RegisterCatalogTable(itemsNoReturning)
    .RegisterCatalogTable(itemsInsertOnly)
    .RegisterCatalogTable(itemsBrokenReturning);

await worker.RunFromArgsAsync(args);
