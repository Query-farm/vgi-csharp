namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One item of a <c>catalog_view_get</c>/<c>catalog_schema_contents_views</c> <see cref="ItemsResponse"/>.
/// Property declaration order matches the generated <c>ViewInfoSchema()</c> exactly: comment, tags,
/// name, schema_name, definition, column_comments.
/// </summary>
public sealed class ViewInfo
{
    public string? Comment { get; set; }

    public Dictionary<string, string> Tags { get; set; } = [];

    public string Name { get; set; } = "";

    public string SchemaName { get; set; } = "";

    /// <summary>The view's SQL <c>SELECT</c> statement.</summary>
    public string Definition { get; set; } = "";

    public Dictionary<string, string> ColumnComments { get; set; } = [];
}
