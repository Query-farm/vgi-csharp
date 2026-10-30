using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Catalog;

/// <summary>
/// Builds one table's canonical constraint records with the owner (schema path + table name)
/// filled in, so a record can never name another table. Every record is
/// <see cref="ConstraintEnforcement.Enforced"/> + <see cref="ConstraintValidation.AllRows"/> — the
/// only records DuckDB reads — so add one only when every row the table can ever return satisfies
/// it. Ids default to stable names derived from the columns. Duplicate ids, duplicate declared names
/// and a second PRIMARY KEY fail here; column resolution and CHECK grammar are checked when the
/// table is served (<see cref="ConstraintRecord.ValidateTable(IReadOnlyList{string}, string, Apache.Arrow.Schema, IReadOnlyList{byte[]}, Func{IReadOnlyList{string}, string, IReadOnlyList{byte[]}?}?)"/>).
/// <code>
/// Constraints = ConstraintRecord.For(["data"], "employees")
///     .PrimaryKey("id")
///     .NotNull("name")
///     .ForeignKey(["department_id"], "departments", ["id"])
///     .Build(),
/// </code>
/// </summary>
public sealed class TableConstraints
{
    private readonly List<byte[]> _records = [];
    private readonly HashSet<string> _ids = new(StringComparer.Ordinal);
    private readonly HashSet<string> _labels = new(StringComparer.OrdinalIgnoreCase);
    private bool _primaryKey;

    internal TableConstraints(IReadOnlyList<string> schemaPath, string table)
    {
        ArgumentNullException.ThrowIfNull(schemaPath);
        ArgumentException.ThrowIfNullOrEmpty(table);
        if (schemaPath.Count != 1)
            throw new ArgumentException("Constraint records require a one-component schema path.", nameof(schemaPath));
        SchemaPath = [.. schemaPath];
        Table = table;
    }

    /// <summary>The owning table's schema path.</summary>
    public IReadOnlyList<string> SchemaPath { get; }

    /// <summary>The owning table's name.</summary>
    public string Table { get; }

    /// <summary>NOT NULL on a top-level column. Default id <c>not_null:&lt;column&gt;</c>.</summary>
    public TableConstraints NotNull(string column, string? id = null, string? declaredName = null) =>
        NotNull([column], id, declaredName);

    /// <summary>NOT NULL on a field, possibly nested in ordinary STRUCTs, e.g. <c>["bbox", "xmin"]</c>.
    /// Default id <c>not_null:&lt;dotted path&gt;</c>.</summary>
    public TableConstraints NotNull(IReadOnlyList<string> path, string? id = null, string? declaredName = null) =>
        Add(id ?? $"not_null:{string.Join('.', path)}", ConstraintKind.NotNull, [path], declaredName);

    /// <summary>The table's PRIMARY KEY over top-level columns. Default id <c>primary_key</c>.</summary>
    public TableConstraints PrimaryKey(params string[] columns) => PrimaryKey(columns, null);

    /// <summary>The table's PRIMARY KEY over top-level columns.</summary>
    public TableConstraints PrimaryKey(IReadOnlyList<string> columns, string? id, string? declaredName = null)
    {
        if (_primaryKey)
            throw new ArgumentException($"Table '{Table}' already has a PRIMARY KEY; a table may carry only one.");
        Add(id ?? "primary_key", ConstraintKind.PrimaryKey, Paths(columns), declaredName);
        _primaryKey = true;
        return this;
    }

    /// <summary>UNIQUE over top-level columns. Default id <c>unique:&lt;col,col&gt;</c>.</summary>
    public TableConstraints Unique(params string[] columns) => Unique(columns, null);

    /// <summary>UNIQUE over top-level columns.</summary>
    public TableConstraints Unique(IReadOnlyList<string> columns, string? id, string? declaredName = null) =>
        Add(id ?? $"unique:{string.Join(',', columns)}", ConstraintKind.Unique, Paths(columns), declaredName);

    /// <summary>CHECK: one numeric comparison between columns and numbers (the grammar DuckDB
    /// reads), e.g. <c>"budget &gt;= 0"</c>.</summary>
    public TableConstraints Check(string id, string sql, string? declaredName = null) =>
        Add(id, ConstraintKind.Check, [], declaredName, checkSql: sql);

    /// <summary>FOREIGN KEY to another table in the same schema (or this one). The referenced
    /// table must carry a PRIMARY KEY or UNIQUE record over <paramref name="referencedColumns"/>
    /// with id <paramref name="referencedConstraintId"/> (default <c>primary_key</c>, the default
    /// id of <see cref="PrimaryKey(string[])"/>). Default id <c>foreign_key:&lt;col,col&gt;</c>.</summary>
    public TableConstraints ForeignKey(
        IReadOnlyList<string> columns, string referencedTable, IReadOnlyList<string> referencedColumns,
        string referencedConstraintId = "primary_key", string? id = null, string? declaredName = null) =>
        Add(id ?? $"foreign_key:{string.Join(',', columns)}", ConstraintKind.ForeignKey, Paths(columns), declaredName,
            references: new ConstraintReference
            {
                SchemaPath = SchemaPath,
                Table = referencedTable,
                Columns = Paths(referencedColumns),
                ConstraintId = referencedConstraintId,
            });

    /// <summary>The records, in the order they were added, for <see cref="CatalogTable.Constraints"/>.</summary>
    public IReadOnlyList<byte[]> Build() => [.. _records];

    private TableConstraints Add(
        string id, ConstraintKind kind, IReadOnlyList<IReadOnlyList<string>> columns, string? declaredName,
        string? checkSql = null, ConstraintReference? references = null)
    {
        if (!_ids.Add(id)) throw new ArgumentException($"Table '{Table}': duplicate constraint id '{id}'.");
        if (declaredName is not null && !_labels.Add(declaredName))
            throw new ArgumentException($"Table '{Table}': duplicate constraint name '{declaredName}' (names compare case-insensitively).");
        _records.Add(ConstraintRecord.Build(new ConstraintRecordSpec
        {
            SchemaPath = SchemaPath,
            Table = Table,
            Id = id,
            Kind = kind,
            Columns = columns,
            DeclaredName = declaredName,
            CheckSql = checkSql,
            References = references,
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.AllRows,
        }));
        return this;
    }

    private static List<IReadOnlyList<string>> Paths(IReadOnlyList<string> columns)
    {
        if (columns.Count == 0) throw new ArgumentException("A key needs at least one column.");
        return columns.Select(column => (IReadOnlyList<string>)[column]).ToList();
    }
}
