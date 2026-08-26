namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The Arrow field-metadata key <c>vgi_catalog_api.cpp</c>'s <c>ParseTableInfo</c> looks for on a
/// <see cref="Protocol.TableInfo.Columns"/> field to mark it as the table's row identity column for
/// UPDATE/DELETE (<c>VGI_ROW_ID_METADATA_KEY</c> in <c>vgi_protocol_constants.hpp</c>, value
/// <c>"is_row_id"</c>). At most one column may carry it; a table declaring
/// <see cref="Protocol.TableInfo.SupportsUpdate"/>/<see cref="Protocol.TableInfo.SupportsDelete"/>
/// with no such column fails to load catalog-side.
/// </summary>
public static class VgiRowIdMetadata
{
    public const string Key = "is_row_id";
    public const string Value = "true";
}
