namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>catalog_table_create</c> RPC's packed request (wire field name <c>request</c>, matching
/// <c>init</c>/<c>table_buffering_*</c>'s packed-single-parameter convention). Property order
/// matches the C++ extension's <c>BuildTableCreateRequest</c> field order exactly: attach_opaque_data,
/// schema_name, name, columns, on_conflict, not_null_constraints, unique_constraints,
/// check_constraints, primary_key_constraints, foreign_key_constraints, transaction_opaque_data.
/// </summary>
public sealed class TableCreateRequest
{
    public byte[] AttachOpaqueData { get; set; } = [];

    public string SchemaName { get; set; } = "";

    public string Name { get; set; } = "";

    /// <summary>Serialized (schema-only) Arrow schema — see <see cref="Internal.SchemaIpc"/>.</summary>
    public byte[] Columns { get; set; } = [];

    public OnConflict OnConflict { get; set; }

    // See TableInfo's matching properties for why these are nullable-element lists.
    public List<int?> NotNullConstraints { get; set; } = [];

    public List<List<int?>> UniqueConstraints { get; set; } = [];

    public List<string> CheckConstraints { get; set; } = [];

    public List<List<int?>> PrimaryKeyConstraints { get; set; } = [];

    /// <summary>Each element an <see cref="Internal.EmbeddedIpc"/>-encoded <see cref="ForeignKeyInfo"/>.</summary>
    public List<byte[]> ForeignKeyConstraints { get; set; } = [];

    public byte[]? TransactionOpaqueData { get; set; }
}
