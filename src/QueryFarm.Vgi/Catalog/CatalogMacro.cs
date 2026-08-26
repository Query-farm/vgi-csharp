using Apache.Arrow;

namespace QueryFarm.Vgi.Catalog;

/// <summary>A declaratively-registered catalog macro (scalar or table) — see
/// <see cref="Protocol.MacroInfo"/> for the wire shape this becomes.</summary>
public sealed class CatalogMacro
{
    public required string Name { get; init; }

    public string SchemaName { get; init; } = "main";

    public required Protocol.MacroType MacroType { get; init; }

    /// <summary>The macro body: a scalar expression for <see cref="Protocol.MacroType.Scalar"/>,
    /// or a <c>SELECT</c> query for <see cref="Protocol.MacroType.Table"/> — references
    /// <see cref="Parameters"/> by name.</summary>
    public required string Definition { get; init; }

    /// <summary>Every parameter's name, in positional-binding order.</summary>
    public List<string> Parameters { get; init; } = [];

    /// <summary>A one-row <c>RecordBatch</c> whose field names are the (a subset of
    /// <see cref="Parameters"/>) defaulted parameters and whose single row holds each one's
    /// default value — <see langword="null"/> when no parameter has a default.</summary>
    public RecordBatch? ParameterDefaults { get; init; }

    /// <summary>Per-parameter descriptions, keyed by parameter NAME (a subset of
    /// <see cref="Parameters"/>) — surfaced via <c>vgi_function_arguments()</c>'s <c>arg_description</c>
    /// column (<c>vgi_doc</c> field metadata on <see cref="Protocol.MacroInfo.ArgumentsSchema"/>).
    /// Empty (the default) means no parameter is documented.</summary>
    public IReadOnlyDictionary<string, string> ParameterDocs { get; init; } = new Dictionary<string, string>();

    public string? Comment { get; init; }

    public Dictionary<string, string> Tags { get; init; } = [];
}
