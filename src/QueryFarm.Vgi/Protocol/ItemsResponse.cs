namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The common unary result shape for every catalog-discovery RPC (<c>catalog_catalogs</c>,
/// <c>catalog_schemas</c>, <c>catalog_schema_contents_functions</c>, etc.): a list of opaque
/// binary blobs, each one an independently embedded-IPC-encoded item record (e.g. one
/// <see cref="SchemaInfo"/> or <see cref="FunctionInfo"/> — see <see cref="Internal.EmbeddedIpc"/>).
/// Wire field name "items", type <c>list(binary)</c>, not nullable.
/// </summary>
public sealed class ItemsResponse
{
    public List<byte[]> Items { get; set; } = [];
}
