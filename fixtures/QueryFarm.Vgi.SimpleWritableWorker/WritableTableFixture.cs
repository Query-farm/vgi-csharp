using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.SimpleWritableWorker;

/// <summary>
/// Builds one writable catalog table backed by an in-memory (well — cross-process file-backed, see
/// <see cref="RowStore"/>) row store, wiring up INSERT/UPDATE/DELETE as ordinary
/// <see cref="TableInOut.ITableInOutFunction"/>s the way the C++ extension actually expects (see
/// <c>vgi_physical_write.cpp</c>): each is resolved by name (inlined on <see cref="Protocol.TableInfo"/>'s
/// <c>insert_function</c>/<c>update_function</c>/<c>delete_function</c>) and invoked exactly like any
/// other table-in-out function — there is NO separate "write function" RPC kind.
///
/// CRITICAL design point discovered empirically against the real C++ extension: the row-identity
/// column DuckDB uses for UPDATE/DELETE (<c>is_row_id</c> field metadata) must be a column of ITS
/// OWN, NOT a reused business column like <c>id</c>. Marking a normal user-supplied column (e.g. the
/// test fixtures' natural-key <c>id</c>) as row-id makes DuckDB's INSERT binder exclude it from the
/// INSERT column list entirely ("table items has 2 columns but 3 values were supplied") — DuckDB
/// treats an <c>is_row_id</c> column as a hidden, worker-managed identity, symmetric with how
/// <c>vgi_physical_write.cpp</c> unconditionally erases it from the wire schema on every
/// INSERT/UPDATE/DELETE RETURNING path. So every table here gets an extra hidden
/// <c>__rowid</c> column (a fresh <see cref="Guid"/> minted per inserted row) — invisible to
/// <c>SELECT *</c>/<c>RETURNING *</c>, used purely to correlate a scanned row back to a specific
/// stored row for UPDATE/DELETE.
/// </summary>
public static class WritableTableFixture
{
    public const string RowIdColumn = "__rowid";

    public static CatalogTable Build(
        string name,
        Schema visibleSchema,
        IReadOnlyList<string> notNullColumns,
        bool supportsInsert,
        bool supportsUpdate,
        bool supportsDelete,
        bool supportsReturning,
        bool brokenReturning = false)
    {
        var fullFields = visibleSchema.FieldsList
            .Append(new Field(RowIdColumn, BinaryType.Default, nullable: false))
            .ToList();
        var fullSchema = new Schema(fullFields, metadata: null);

        var store = new RowStore(name);

        var insert = supportsInsert
            ? new WritableInsertFunction($"{name}_insert", visibleSchema, fullSchema, store, brokenReturning)
            : null;
        var update = supportsUpdate
            ? new WritableUpdateFunction($"{name}_update", visibleSchema, fullSchema, RowIdColumn, store)
            : null;
        var delete = supportsDelete
            ? new WritableDeleteFunction($"{name}_delete", visibleSchema, store)
            : null;

        return new CatalogTable
        {
            Name = name,
            SchemaName = "main",
            Columns = fullSchema,
            NotNullColumns = notNullColumns,
            RowIdColumn = RowIdColumn,
            ScanFunction = new RowStoreScanFunction(name, fullSchema, store),
            SupportsInsert = supportsInsert,
            SupportsUpdate = supportsUpdate,
            SupportsDelete = supportsDelete,
            SupportsReturning = supportsReturning,
            InsertFunction = insert,
            UpdateFunction = update,
            DeleteFunction = delete,
        };
    }
}
