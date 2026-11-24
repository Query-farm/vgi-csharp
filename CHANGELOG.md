# Changelog

## Unreleased

- **Direct storage (contract v1).** `VGI_STORAGE_BACKEND` = `vendor` | `s3` | `gcs` | `none`
  (inferred from `VGI_BLOB_VENDOR_URL` / `VGI_STORAGE_S3_BUCKET` / `VGI_STORAGE_GCS_BUCKET`; more
  than one set refuses to start). With `s3` or `gcs` an HTTP worker externalizes results, vends
  upload grants, advertises `VGI-Arguments-Ref-Bytes` and keeps function-storage blobs
  (`VGI_STORAGE_EXTERNALIZE=1`, s3 only, under `<prefix>fs/`) in the operator's own bucket
  (vgi-rpc-csharp `S3StorageEnvironment` / `GcsStorageEnvironment`). `VGI_STORAGE_S3_CHECKSUM=none`
  issues no upload grants and advertises no upload-URL support or argument references; results are
  still externalized. Hosted mode with s3/gcs is allowed and logged. QueryFarm.Vgi now references
  QueryFarm.VgiRpc.S3 and QueryFarm.VgiRpc.Gcs.
- **CI `s3` integration lane**: the http suite with results externalized and large requests
  uploaded to a RustFS bucket; fails unless results landed in the bucket.

## 0.27.0 (2026-11-23)

- **Protocol 3.4 large values** (vgi `docs/large-values-3.4.md`):
  - Regenerated protocol types. The six requests that embed `bind_call` (init, cardinality,
    statistics, dynamic-to-string, plan, projection) carry it as `Nested<BindRequest>`, which holds
    the received bytes and re-encodes them verbatim; `BindRequest.Arguments` is nullable and
    `BindRequest.ArgumentsLocation` is new (the last field). Code that built `BindCall =
    EmbeddedIpc.Encode(bind)` now writes `Nested<BindRequest>.Of(bind)`.
  - Argument references: a bind that does not set exactly one of `arguments` and
    `arguments_location` is refused (`invalid_bind_request`); a location is resolved at the
    turn's entry through vgi-rpc's object cache, so functions keep seeing their arguments. A gone
    object fails with "this prepared statement's large argument has expired; prepare it again".
    Split tokens fingerprint a location by its size and SHA-256.
  - The HTTP worker reads the blob-vendor environment (`VGI_BLOB_VENDOR_URL`,
    `VGI_BLOB_VENDOR_TOKEN`, `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES`, `VGI_RPC_MAX_REQUEST_BYTES`,
    `VGI_RPC_MAX_UPLOAD_BYTES`, `VGI_RPC_MAX_FETCH_BYTES`): large results are vendor pointers,
    client uploads go through sized vendor grants, and `VGI-Arguments-Ref-Bytes` is advertised
    (4 MiB, `VGI_ARGUMENTS_REF_BYTES`), in Orchard hosted mode too, where grants and argument
    references bind to the Pass's principal and `VGI_RPC_MAX_REQUEST_BYTES` defaults to 32 MiB
    (as in vgi-python). Streams expire after 6 h (`token_ttl`). With
    `VGI_STORAGE_EXTERNALIZE=1`, function-storage values of at least
    `VGI_STORAGE_EXTERNALIZE_BYTES` live in the vendor (storage envelope v1).
  - The sealing boundary no longer rewrites `bind_call`: the opened attach and transaction live
    beside it, so the bytes re-encoded on the wire or into storage are the ones received.
  - The table-buffering bind context persisted to worker storage is the received `bind_call`
    (the reference form), sealed, instead of a re-encoded request with `secrets` in the clear.
  - Streams stay in process memory (single-replica in 3.4): there is no stored `init` state.
  - The example worker serves the `blob_test` catalog (vgi `docs/testing/blob-test-catalog.md`),
    including `blob_param_echo`, `blob_param_batches`, `blob_param_streams`, `blob_param_scalar`
    and the `blob_param_table` table.
- **Orchard hosted mode** (`VGI_ORCHARD_HOSTED=1`), the port of vgi-python's: the HTTP worker
  accepts only Orchard Passes (RFC 9068 `at+jwt`, ES256, verified against `VGI_JWT_JWKS_URI` with a
  JWKS cache that fails closed) on every request but `HEAD /health` and CORS preflights, and refuses
  to start with any other credential, a non-HTTP transport, or a missing or short
  `VGI_SIGNING_KEY`. See the README's "Orchard hosted mode". Scalar functions now see the caller's
  `AuthContext` as `ScalarProcessParams.Auth`. The example worker serves `pass_claims(x)` in hosted
  mode only.
- **Protocol 3.4.0** (`Worker.DefaultProtocolVersion`). Requires the matching VGI DuckDB
  extension (wire 3.4.x); 3.3 clients and workers are incompatible. No vgi.v3 schema changes: 3.4
  moves client uploads to vgi-rpc's upload-URL protocol v2 (`VGI-Upload-URL-Support: 2`) and
  makes `vgi_rpc.location.sha256` mandatory on pointers. Requires QueryFarm.VgiRpc 0.19.0.
- Filter decoding no longer rejects comparisons between parameterised Arrow types as "not
  bind-compatible". Apache.Arrow's `TimestampType` (and time, duration, interval, fixed-size binary,
  nested types) compare by reference, so the column's and the literal's separately built
  `timestamp[us, tz=UTC]` never matched and every `ts >= TIMESTAMPTZ '...'` pushed to a worker
  failed. Types now compare structurally; a timestamp matches on unit and time-zone awareness.
- The example worker serves `data.planned_events` (vgi `scan_planning_predicates*.test`,
  `table/comments.test`): a filtered-planning table with DATE, TIMESTAMPTZ, VARCHAR, BIGINT, DOUBLE
  and BOOLEAN columns that prunes days on `event_date`/`ts` statistics and records each planning
  call's days and described filters in `planned_events_last_plan.json`, as vgi-python's fixture
  does.
- `ci/run-integration.sh` and `scripts/run_tests.sh` set `VGI_TEST_BRANCH_DIR`, so the
  `catalog/scan_planning_*.test` files run instead of skipping. The shared suite pin (vgi
  protocol-3.4 `007edd8`) includes those tests.

## 0.26.1 (2026-11-16)

- The example worker serves the `contract` schema for the vgi extension's cross-SDK conformance
  files (a table_only scan with a row id, a table_only scan named `range`, a finalize-only INSERT
  count with an enforced NOT NULL, and decimal32/64/128 columns).

## 0.26.0 (2026-11-11)

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
