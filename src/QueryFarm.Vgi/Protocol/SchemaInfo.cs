namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One item of a <c>catalog_schemas</c>/<c>catalog_schema_get</c> <see cref="ItemsResponse"/>.
/// The C++ extension validates each item's embedded-IPC schema with STRICT
/// <c>arrow::Schema::Equals</c> against its generated <c>SchemaInfoSchema()</c> — property
/// declaration order matters and must match that 5-field schema exactly: comment, tags,
/// attach_opaque_data, name, estimated_object_count.
/// </summary>
public sealed class SchemaInfo
{
    public string? Comment { get; set; }

    public Dictionary<string, string> Tags { get; set; } = [];

    public byte[] AttachOpaqueData { get; set; } = [];

    public string Name { get; set; } = "";

    /// <summary>
    /// Declared with a nullable <c>long?</c> value type deliberately: Arrow's own
    /// <c>map(utf8, int64)</c> factory defaults the value field to NULLABLE, but
    /// <c>SchemaDerivation</c>'s map-value-field rule only infers "nullable" for a reference-typed
    /// value (a plain non-nullable <c>long</c> value type would derive a non-nullable value field,
    /// which fails the C++ side's strict schema-equality check against its generated
    /// <c>SchemaInfoSchema()</c>).
    /// </summary>
    public Dictionary<string, long?>? EstimatedObjectCount { get; set; }
}
