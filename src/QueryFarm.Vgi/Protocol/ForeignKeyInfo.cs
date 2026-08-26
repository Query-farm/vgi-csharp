namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One element of <see cref="TableInfo.ForeignKeyConstraints"/> — each list entry is itself an
/// independently embedded-IPC-encoded record of this shape (mirrors <see cref="Internal.EmbeddedIpc"/>'s
/// "list of independently-encoded items" convention), parsed by <c>vgi_catalog_api.cpp</c>'s
/// <c>ParseTableInfo</c> off a single-row batch with these four field names: fk_columns, pk_columns,
/// referenced_table, referenced_schema.
/// </summary>
public sealed class ForeignKeyInfo
{
    public List<string> FkColumns { get; set; } = [];

    public List<string> PkColumns { get; set; } = [];

    public string ReferencedTable { get; set; } = "";

    public string ReferencedSchema { get; set; } = "";
}
