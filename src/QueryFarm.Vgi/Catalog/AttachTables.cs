using Apache.Arrow;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi.Catalog;

/// <summary>
/// Tables an attach creates at run time (<c>CREATE TABLE</c>, <c>CREATE TABLE ... AS SELECT</c>),
/// served next to the worker's declared <see cref="CatalogTable"/>s. Register one with
/// <see cref="Worker.AttachTables"/>.
/// </summary>
/// <remarks>
/// <para>The SDK asks <see cref="Tables"/> for the attach's current tables whenever it lists a
/// schema's tables, looks one table up (<c>catalog_table_get</c>, scans) or counts them, so the
/// tables live wherever the worker keeps them (a file, a database, per attach) and every worker
/// process sees the same set. <see cref="CreateTable"/> answers <c>catalog_table_create</c> and
/// <see cref="DropTable"/> answers <c>catalog_table_drop</c> for names that are not declared
/// tables.</para>
/// <para>A created table names its functions like any <see cref="CatalogTable"/>, but they must be
/// instances this object declares in <see cref="ScanFunctions"/> and <see cref="WriteFunctions"/>:
/// the SDK registers those once, lists them <see cref="FunctionVisibility.TableOnly"/> (unless the
/// same instance is also registered as an ordinary function), and resolves calls to them by name.
/// One function usually serves every created table, told which one by the table's
/// <see cref="CatalogTable.ScanArguments"/> and <see cref="CatalogTable.WriteArguments"/>.
/// Created tables cannot declare constraints.</para>
/// <para>Protocol 3.3: <see cref="CreateTable"/> returns whether this call created the table, which
/// the reply's <c>created</c> carries. <c>CREATE TABLE IF NOT EXISTS ... AS SELECT</c> inserts
/// nothing when it is <see langword="false"/>.</para>
/// <para>The default <c>catalog_contents</c> snapshot of an identity with attach tables is composed
/// on every call instead of cached, since its tables change without a registration.</para>
/// </remarks>
public abstract class AttachTables
{
    /// <summary>The scan functions created tables may name (<see cref="CatalogTable.ScanFunction"/>).</summary>
    public virtual IReadOnlyList<ITableFunction> ScanFunctions => [];

    /// <summary>The write functions created tables may name (<see cref="CatalogTable.InsertFunction"/>,
    /// <see cref="CatalogTable.UpdateFunction"/>, <see cref="CatalogTable.DeleteFunction"/>).</summary>
    public virtual IReadOnlyList<ITableInOutFunction> WriteFunctions => [];

    /// <summary>The attach's created tables in <see cref="AttachTablesContext.SchemaPath"/>, or in
    /// every schema when it is <see langword="null"/>.</summary>
    public abstract IEnumerable<CatalogTable> Tables(AttachTablesContext context);

    /// <summary>Creates a table (<c>catalog_table_create</c>). Returns <see langword="true"/> when
    /// this call created it (including <see cref="OnConflict.Replace"/>, which drops the old table
    /// first) and <see langword="false"/> when <see cref="OnConflict.Ignore"/> (<c>IF NOT EXISTS</c>)
    /// found an existing table, which must stay exactly as it was. Throw for
    /// <see cref="OnConflict.Error"/> on an existing name (a message containing <c>already exists</c>).
    /// The default refuses: the catalog does not create tables.</summary>
    public virtual bool CreateTable(TableCreateContext context) =>
        throw new Internal.CatalogReadOnlyException("catalog_table_create");

    /// <summary>Drops a created table (<c>catalog_table_drop</c>); a missing name is an error unless
    /// <see cref="TableDropContext.IgnoreNotFound"/>. The default refuses.</summary>
    public virtual void DropTable(TableDropContext context) =>
        throw new Internal.CatalogReadOnlyException("catalog_table_drop");
}

/// <summary>Which attach (and schema) an <see cref="AttachTables.Tables"/> call is about.</summary>
public sealed class AttachTablesContext
{
    private AttachData? _attach;

    /// <summary>The attach's plaintext <c>attach_opaque_data</c>, the bytes its functions see as
    /// <c>AttachOpaqueData</c>.</summary>
    public required byte[] AttachOpaqueData { get; init; }

    /// <summary>The schema asked about, or <see langword="null"/> for every schema.</summary>
    public IReadOnlyList<string>? SchemaPath { get; init; }

    /// <summary>The caller's transaction id (plaintext), empty outside a transaction.</summary>
    public byte[] TransactionOpaqueData { get; init; } = [];

    /// <summary>The decoded attach envelope.</summary>
    public AttachData Attach => _attach ??= AttachData.Parse(AttachOpaqueData);

    /// <summary>The attach's ATTACH options, <c>secret</c> ones included.</summary>
    public AttachOptions AttachOptions => Attach.Options;

    /// <summary>The RPC call context, when the transport supplies one.</summary>
    public ICallContext? CallContext { get; init; }
}

/// <summary>A <c>catalog_table_create</c> call (<see cref="AttachTables.CreateTable"/>).</summary>
public sealed class TableCreateContext
{
    private AttachData? _attach;
    private Schema? _columns;

    /// <summary>The request as it came off the wire.</summary>
    public required TableCreateRequest Request { get; init; }

    /// <summary>The attach's plaintext <c>attach_opaque_data</c>.</summary>
    public byte[] AttachOpaqueData => Request.AttachOpaqueData;

    /// <summary>The schema the table is created in.</summary>
    public IReadOnlyList<string> SchemaPath => Request.SchemaPath;

    /// <summary>The table's name.</summary>
    public string Name => Request.Name;

    /// <summary>What to do when the name exists: <see cref="OnConflict.Error"/>,
    /// <see cref="OnConflict.Ignore"/> (<c>IF NOT EXISTS</c>) or <see cref="OnConflict.Replace"/>
    /// (<c>OR REPLACE</c>).</summary>
    public OnConflict OnConflict => Request.OnConflict;

    /// <summary>The requested columns (decoded <see cref="TableCreateRequest.Columns"/>), as DuckDB
    /// typed them; field metadata carries defaults and comments.</summary>
    public Schema Columns => _columns ??= Internal.SchemaIpc.ReadSchemaOnly(Request.Columns);

    /// <summary>The caller's transaction id (plaintext), empty outside a transaction.</summary>
    public byte[] TransactionOpaqueData => Request.TransactionOpaqueData ?? [];

    /// <summary>The decoded attach envelope.</summary>
    public AttachData Attach => _attach ??= AttachData.Parse(AttachOpaqueData);

    /// <summary>The attach's ATTACH options, <c>secret</c> ones included.</summary>
    public AttachOptions AttachOptions => Attach.Options;

    /// <summary>The RPC call context, when the transport supplies one.</summary>
    public ICallContext? CallContext { get; init; }
}

/// <summary>A <c>catalog_table_drop</c> call (<see cref="AttachTables.DropTable"/>).</summary>
public sealed class TableDropContext
{
    private AttachData? _attach;

    /// <summary>The attach's plaintext <c>attach_opaque_data</c>.</summary>
    public required byte[] AttachOpaqueData { get; init; }

    /// <summary>The table's schema.</summary>
    public required IReadOnlyList<string> SchemaPath { get; init; }

    /// <summary>The table's name.</summary>
    public required string Name { get; init; }

    /// <summary><c>DROP TABLE IF EXISTS</c>: a missing table is not an error.</summary>
    public bool IgnoreNotFound { get; init; }

    /// <summary><c>DROP TABLE ... CASCADE</c>.</summary>
    public bool Cascade { get; init; }

    /// <summary>The caller's transaction id (plaintext), empty outside a transaction.</summary>
    public byte[] TransactionOpaqueData { get; init; } = [];

    /// <summary>The decoded attach envelope.</summary>
    public AttachData Attach => _attach ??= AttachData.Parse(AttachOpaqueData);

    /// <summary>The attach's ATTACH options, <c>secret</c> ones included.</summary>
    public AttachOptions AttachOptions => Attach.Options;

    /// <summary>The RPC call context, when the transport supplies one.</summary>
    public ICallContext? CallContext { get; init; }
}
