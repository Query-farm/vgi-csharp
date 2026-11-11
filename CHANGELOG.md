# Changelog

## Unreleased

- **Protocol 3.3.0** (`Worker.DefaultProtocolVersion`). Requires the matching VGI DuckDB
  extension (wire 3.3.x); 3.2 clients and workers are incompatible. Regenerated protocol types:
  `FunctionInfo.Visibility` (nullable utf8, `"public"`/null or `"table_only"`).
- **A table's scan and write functions disappear from `duckdb_functions()`.** Every function a
  `CatalogTable` names is listed so DuckDB keeps its capabilities, but write functions
  (insert/update/delete) are always listed `table_only` (not callable from SQL), and so is the
  scan unless the table sets the new `CatalogTable.ExposeScan = true` or the same instance is also
  registered with `RegisterTable`. Code that called a table's scan function directly
  (`SELECT * FROM cat.main.my_scan()`) must set `ExposeScan`.
- `QueryFarm.Vgi.Catalog.FunctionVisibility` (`Public`, `TableOnly`) constants.
- Registering the same function instance twice (`RegisterTable`/`RegisterTableInOut`, or a write
  function shared by several tables) now lists it once.
- **Protocol 3.3 write path** (still 3.3.0; regenerated from vgi-python 2bce3e0):
  - `catalog_table_scan_function_get` is gone (`IVgiService.CatalogTableScanFunctionGetAsync`
    removed). `catalog_table_scan_branches_get` is the only scan-discovery RPC; this SDK always
    answered it, so table scans are unchanged.
  - `catalog_table_create` replies `created: bool` (`TableCreateResponse`;
    `IVgiService.CatalogTableCreateAsync` now returns `Task<TableCreateResponse>`): true when the
    call created the table, false when `IF NOT EXISTS` found an existing one, so
    `CREATE TABLE IF NOT EXISTS ... AS SELECT` no longer appends into it. In-memory catalogs
    report it.
  - The write-options batch carries only `result_mode`. `WriteRequest.OnConflict` /
    `OnConflictColumns` are deprecated and always `"throw"` / empty: DuckDB rewrites every
    `INSERT ... ON CONFLICT` / `INSERT OR REPLACE` into a MERGE, which reaches the worker as
    ordinary insert/update/delete write calls (one per MERGE action).
- New `Worker.AttachTables(AttachTables)` (`QueryFarm.Vgi.Catalog.AttachTables`,
  `AttachTablesContext`, `TableCreateContext`, `TableDropContext`): serve tables an attach creates
  at run time (`CREATE TABLE`, CTAS, `DROP TABLE`) next to declared `CatalogTable`s. The SDK asks
  `Tables(...)` whenever it lists or looks up tables, routes `catalog_table_create` (returning
  `created`) and `catalog_table_drop` for undeclared names to it, and lists the functions it
  declares (`ScanFunctions`, `WriteFunctions`) `table_only`.
- New `CatalogTable.WriteArguments`: fixed positional arguments baked into the insert/update/delete
  descriptors (the write-side `ScanArguments`), so one write function can serve several tables.
- Fixtures: the `txfix` worker implements the new contract (vgi `docs/writable_tx_fixtures.md`):
  `items_insert` / `items_delete` next to `items_update`, all with a finalize stage that records
  the call in the new `writes()` function; `items.qty BIGINT DEFAULT 0`; staged inserts and
  deletes with read-your-writes on the transactional attach; snapshot scans. The
  `simple_writable` worker serves created tables (CTAS) through `AttachTables`.
