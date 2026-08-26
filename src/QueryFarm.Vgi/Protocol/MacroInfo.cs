namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// Whether a <see cref="MacroInfo"/>/<see cref="Catalog.CatalogMacro"/> is a scalar macro (usable
/// in an expression position) or a table macro (usable as <c>FROM schema.name(...)</c>). Wire-
/// encoded as <c>dictionary(int16, utf8)</c> by member name (same convention as
/// <see cref="FunctionType"/>) — the C++ extension's macro_type parser accepts this value
/// case-insensitively, but "SCALAR"/"TABLE" (this enum's default wire naming) is the canonical
/// form other language ports emit.
/// </summary>
public enum MacroType
{
    Scalar,
    Table,
}

/// <summary>
/// One item of a <c>catalog_schema_contents_macros</c>/<c>catalog_macro_get</c>
/// <see cref="ItemsResponse"/>. The C++ extension validates this type's embedded-IPC schema with
/// STRICT <c>arrow::Schema::Equals</c> against its generated <c>MacroInfoSchema()</c> — property
/// declaration order matters and must match that schema exactly: comment, tags, name, schema_name,
/// macro_type, parameters, parameter_default_values, definition, arguments_schema.
/// </summary>
public sealed class MacroInfo
{
    public string? Comment { get; set; }

    public Dictionary<string, string> Tags { get; set; } = [];

    public string Name { get; set; } = "";

    public string SchemaName { get; set; } = "";

    public MacroType MacroType { get; set; }

    /// <summary>Every macro parameter's name, in positional-binding order.</summary>
    public List<string> Parameters { get; set; } = [];

    /// <summary>A one-row embedded-IPC <c>RecordBatch</c> whose field NAMES are the (necessarily a
    /// subset of <see cref="Parameters"/>) defaulted parameters and whose single row holds each
    /// one's default value — <see langword="null"/> when no parameter has a default.</summary>
    public byte[]? ParameterDefaultValues { get; set; }

    /// <summary>The macro body — a scalar expression (<see cref="MacroType.Scalar"/>) or a
    /// <c>SELECT</c> query (<see cref="MacroType.Table"/>), referencing <see cref="Parameters"/>
    /// by name.</summary>
    public string Definition { get; set; } = "";

    /// <summary>Reserved for future per-parameter documentation (additive, back-compat-guarded on
    /// the C++ side) — always <see langword="null"/> from this port today.</summary>
    public byte[]? ArgumentsSchema { get; set; }
}
