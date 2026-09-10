namespace QueryFarm.Vgi.Catalog;

/// <summary>A declaratively-registered catalog view — see <see cref="Protocol.ViewInfo"/> for the
/// wire shape this becomes.</summary>
public sealed class CatalogView
{
    public required string Name { get; init; }

    public string SchemaName { get; init; } = "main";

    public IReadOnlyList<string>? SchemaPath { get; init; }

    internal IReadOnlyList<string> EffectiveSchemaPath => SchemaPath ?? [SchemaName];

    public required string Definition { get; init; }

    public string? Comment { get; init; }

    public Dictionary<string, string> Tags { get; init; } = [];

    public Dictionary<string, string> ColumnComments { get; init; } = [];
}
