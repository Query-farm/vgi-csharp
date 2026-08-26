namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One item of a <c>catalog_table_get</c>/<c>catalog_schema_contents_tables</c> <see cref="ItemsResponse"/>.
/// The C++ extension validates each item's embedded-IPC schema with STRICT <c>arrow::Schema::Equals</c>
/// against its generated <c>TableInfoSchema()</c> — property declaration order matters and must match
/// that 24-field schema exactly: comment, tags, name, schema_name, columns, not_null_constraints,
/// unique_constraints, check_constraints, primary_key_constraints, foreign_key_constraints,
/// supports_insert, supports_update, supports_delete, supports_returning, supports_column_statistics,
/// scan_function, insert_function, update_function, delete_function, cardinality_estimate,
/// cardinality_max, column_statistics, bind_result, required_filters.
/// </summary>
public sealed class TableInfo
{
    public string? Comment { get; set; }

    public Dictionary<string, string> Tags { get; set; } = [];

    public string Name { get; set; } = "";

    public string SchemaName { get; set; } = "";

    /// <summary>Serialized (schema-only, <see cref="Internal.SchemaIpc"/>) Arrow schema describing
    /// this table's columns — a column marked with <see cref="Internal.VgiRowIdMetadata.Key"/> field
    /// metadata is the row identity UPDATE/DELETE key.</summary>
    public byte[] Columns { get; set; } = [];

    // Declared with NULLABLE int?/List<int?> element types deliberately (same reasoning as
    // SchemaInfo.EstimatedObjectCount): Arrow's own list(int32()) factory defaults the item field
    // to NULLABLE, and SchemaDerivation's list-item-field rule only infers "nullable" for a
    // reference-typed (or Nullable<T>) element — a plain `List<int>` derives a NON-nullable item
    // field, which fails the C++ side's strict schema-equality check against its generated
    // TableInfoSchema() (confirmed against a real mismatch: "expected list<item: int32 not null>"
    // — er, the other way: C++ expects the NULLABLE item, ours came out non-nullable).
    public List<int?> NotNullConstraints { get; set; } = [];

    public List<List<int?>> UniqueConstraints { get; set; } = [];

    public List<string> CheckConstraints { get; set; } = [];

    public List<List<int?>> PrimaryKeyConstraints { get; set; } = [];

    /// <summary>Each element an <see cref="Internal.EmbeddedIpc"/>-encoded <see cref="ForeignKeyInfo"/>.</summary>
    public List<byte[]> ForeignKeyConstraints { get; set; } = [];

    public bool SupportsInsert { get; set; }

    public bool SupportsUpdate { get; set; }

    public bool SupportsDelete { get; set; }

    public bool SupportsReturning { get; set; }

    public bool SupportsColumnStatistics { get; set; }

    /// <summary>Inline <see cref="Internal.EmbeddedIpc"/>-encoded <see cref="ScanFunctionResult"/> —
    /// when present, the C++ extension uses it directly and never fires the (unimplemented, in this
    /// worker) <c>catalog_table_scan_function_get</c> RPC.</summary>
    public byte[]? ScanFunction { get; set; }

    public byte[]? InsertFunction { get; set; }

    public byte[]? UpdateFunction { get; set; }

    public byte[]? DeleteFunction { get; set; }

    public long? CardinalityEstimate { get; set; }

    public long? CardinalityMax { get; set; }

    public byte[]? ColumnStatistics { get; set; }

    public byte[]? BindResult { get; set; }

    public List<List<string>> RequiredFilters { get; set; } = [];
}
