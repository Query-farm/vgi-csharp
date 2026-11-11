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
