using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Catalog;

/// <summary>The catalog read profiles that gate canonical constraint records (VGI protocol v3).</summary>
public static class ConstraintProfiles
{
    /// <summary>Read profile: the catalog serves canonical <see cref="ConstraintInfo"/> records.</summary>
    public const string RecordsRead = "vgi.constraint.records.read.v1";

    /// <summary>Read profile: CHECK constraints carry DuckDB scalar SQL expressions. Selectable only
    /// alongside <see cref="RecordsRead"/>.</summary>
    public const string ScalarExpression = "vgi.expression.duckdb.scalar.v1";
}

/// <summary>The key a FOREIGN_KEY record references.</summary>
public sealed class ConstraintReference
{
    /// <summary>Schema path of the referenced table (the DuckDB extension requires the owning
    /// table's schema).</summary>
    public required IReadOnlyList<string> SchemaPath { get; init; }

    /// <summary>Referenced table name.</summary>
    public required string Table { get; init; }

    /// <summary>Referenced column paths; same arity as the local columns.</summary>
    public required IReadOnlyList<IReadOnlyList<string>> Columns { get; init; }

    /// <summary><see cref="ConstraintRecordSpec.Id"/> of the referenced table's PRIMARY_KEY or
    /// UNIQUE record. <see langword="null"/> only on a decoded record that carries none.</summary>
    public required string? ConstraintId { get; init; }
}

/// <summary>
/// One canonical constraint fact, in the worker's own words. The worker attests it: the SDK never
/// derives identities or translates column flags, and it never upgrades
/// <see cref="Enforcement"/>/<see cref="Validation"/>. The DuckDB extension reads only
/// <see cref="ConstraintEnforcement.Enforced"/> + <see cref="ConstraintValidation.AllRows"/>
/// records, so state that only when every row the table can ever return satisfies it.
/// </summary>
public sealed class ConstraintRecordSpec
{
    /// <summary>Schema path of the owning table.</summary>
    public required IReadOnlyList<string> SchemaPath { get; init; }

    /// <summary>Owning table name.</summary>
    public required string Table { get; init; }

    /// <summary>Stable provider-owned identity, unique within the table.</summary>
    public required string Id { get; init; }

    /// <summary>The constraint kind.</summary>
    public required ConstraintKind Kind { get; init; }

    /// <summary>Raw component paths, e.g. <c>[["id"]]</c> or <c>[["bbox", "xmin"]]</c>; empty for
    /// CHECK, exactly one for NOT_NULL.</summary>
    public IReadOnlyList<IReadOnlyList<string>> Columns { get; init; } = [];

    /// <summary>Whether the authority enforces the constraint on every write path. Required: the
    /// worker states it explicitly.</summary>
    public required ConstraintEnforcement Enforcement { get; init; }

    /// <summary>Coverage of existing rows; only <see cref="ConstraintValidation.AllRows"/>
    /// justifies engine facts. Required: the worker states it explicitly.</summary>
    public required ConstraintValidation Validation { get; init; }

    /// <summary>Optional SQL label (<c>CONSTRAINT &lt;name&gt;</c>).</summary>
    public string? DeclaredName { get; init; }

    /// <summary>CHECK only: a DuckDB scalar boolean expression over the table's columns.</summary>
    public string? CheckSql { get; init; }

    /// <summary>FOREIGN_KEY only: the referenced key.</summary>
    public ConstraintReference? References { get; init; }
}

/// <summary>
/// Builds, decodes and checks canonical <see cref="ConstraintInfo"/> records — the
/// <see cref="CatalogTable.Constraints"/> entries a v3 table carries. A catalog whose tables carry
/// records negotiates <see cref="ConstraintProfiles.RecordsRead"/> automatically in
/// <c>catalog_attach</c>; nothing else is required of the worker.
/// </summary>
public static class ConstraintRecord
{
    /// <summary>Encodes one canonical <see cref="ConstraintInfo"/> record (IPC bytes) for
    /// <see cref="CatalogTable.Constraints"/>. Throws <see cref="ArgumentException"/> for a shape the
    /// protocol rejects.</summary>
    public static byte[] Build(ConstraintRecordSpec spec)
    {
        ArgumentNullException.ThrowIfNull(spec);
        if (string.IsNullOrEmpty(spec.Id) || spec.Id.Contains('\0'))
            throw new ArgumentException("constraint id must be nonempty without NUL", nameof(spec));
        if (string.IsNullOrEmpty(spec.Table))
            throw new ArgumentException("constraint table must be nonempty", nameof(spec));
        if (spec.SchemaPath.Count == 0)
            throw new ArgumentException("constraint schema path must be nonempty", nameof(spec));
        if (!Enum.IsDefined(spec.Kind) || !Enum.IsDefined(spec.Enforcement) || !Enum.IsDefined(spec.Validation))
            throw new ArgumentException("constraint kind, enforcement and validation must be defined values", nameof(spec));
        var isCheck = spec.Kind == ConstraintKind.Check;
        if (isCheck != (spec.CheckSql is not null))
            throw new ArgumentException("CheckSql is required for CHECK and only CHECK", nameof(spec));
        if (isCheck ? spec.Columns.Count != 0 : spec.Columns.Count == 0)
            throw new ArgumentException(isCheck ? "CHECK takes no column paths" : $"{spec.Kind} requires column paths", nameof(spec));
        if (spec.Columns.Any(path => path.Count == 0))
            throw new ArgumentException("column paths must be nonempty", nameof(spec));
        if (spec.Kind == ConstraintKind.NotNull && spec.Columns.Count != 1)
            throw new ArgumentException("NOT_NULL requires exactly one column path", nameof(spec));
        if ((spec.Kind == ConstraintKind.ForeignKey) != (spec.References is not null))
            throw new ArgumentException("References is required for FOREIGN_KEY and only FOREIGN_KEY", nameof(spec));
        if (spec.References is { } reference)
        {
            if (reference.Columns.Count != spec.Columns.Count)
                throw new ArgumentException("foreign key local and referenced arity must match", nameof(spec));
            if (string.IsNullOrEmpty(reference.ConstraintId))
                throw new ArgumentException("foreign key must name the referenced PRIMARY_KEY/UNIQUE record id", nameof(spec));
        }

        var check = spec.CheckSql is null ? null : EmbeddedIpc.Encode(new SqlExpression
        {
            Sql = spec.CheckSql,
            Profile = ConstraintProfiles.ScalarExpression,
            SchemaPath = [.. spec.SchemaPath],
            EvaluationContext = new QueryEvaluationContext
            {
                Timezone = "UTC",
                Collation = "binary",
                DefaultOrder = "asc",
                DefaultNullOrder = "nulls_last",
                PreserveInsertionOrder = true,
            },
        });
        var foreign = spec.References is not { } fk ? null : EmbeddedIpc.Encode(new ForeignKeyDefinition
        {
            ReferencedTable = ObjectRef("table", fk.SchemaPath, fk.Table, null),
            ReferencedColumnPaths = Paths(fk.Columns),
            Match = ForeignKeyMatch.Simple,
            OnUpdate = ReferentialAction.NoAction,
            OnDelete = ReferentialAction.NoAction,
        });
        return EmbeddedIpc.Encode(new ConstraintInfo
        {
            Ref = ObjectRef("constraint", spec.SchemaPath, spec.Id, spec.Table),
            Definition = EmbeddedIpc.Encode(new ConstraintDefinition
            {
                Kind = spec.Kind,
                ColumnPaths = Paths(spec.Columns),
                DeclaredName = spec.DeclaredName,
                CheckExpression = check,
                ForeignKey = foreign,
            }),
            Enforcement = spec.Enforcement,
            Validation = spec.Validation,
            ReferencedConstraint = spec.References is { } target
                ? ObjectRef("constraint", target.SchemaPath, target.ConstraintId!, target.Table)
                : null,
        });
    }

    /// <summary>Decodes a <see cref="TableInfo.Constraints"/> entry — the inverse of
    /// <see cref="Build"/>.</summary>
    public static ConstraintRecordSpec Read(byte[] record)
    {
        ArgumentNullException.ThrowIfNull(record);
        var info = EmbeddedIpc.Decode<ConstraintInfo>(record);
        var reference = EmbeddedIpc.Decode<CatalogObjectRef>(info.Ref);
        var definition = EmbeddedIpc.Decode<ConstraintDefinition>(info.Definition);
        ConstraintReference? references = null;
        if (definition.ForeignKey is { } fkBytes)
        {
            var fk = EmbeddedIpc.Decode<ForeignKeyDefinition>(fkBytes);
            var table = EmbeddedIpc.Decode<CatalogObjectRef>(fk.ReferencedTable);
            references = new ConstraintReference
            {
                SchemaPath = table.SchemaPath,
                Table = table.Name,
                Columns = fk.ReferencedColumnPaths,
                ConstraintId = info.ReferencedConstraint is { } target
                    ? EmbeddedIpc.Decode<CatalogObjectRef>(target).Name : null,
            };
        }

        return new ConstraintRecordSpec
        {
            SchemaPath = reference.SchemaPath,
            Table = reference.OwnerTable ?? "",
            Id = reference.Name,
            Kind = definition.Kind,
            Columns = definition.ColumnPaths,
            Enforcement = info.Enforcement,
            Validation = info.Validation,
            DeclaredName = definition.DeclaredName,
            CheckSql = definition.CheckExpression is { } check ? EmbeddedIpc.Decode<SqlExpression>(check).Sql : null,
            References = references,
        };
    }

    /// <summary>The client-offered profiles from <paramref name="available"/>, in canonical (ordinal)
    /// order. <see cref="ConstraintProfiles.ScalarExpression"/> is selected only alongside
    /// <see cref="ConstraintProfiles.RecordsRead"/>.</summary>
    /// <param name="available">The read profiles the catalog advertises.</param>
    /// <param name="clientCapabilities">The request's encoded <see cref="ClientCapabilities"/>
    /// (<see cref="CatalogAttachRequest.ClientCapabilities"/>), or <see langword="null"/>.</param>
    public static List<string> SelectReadProfiles(IEnumerable<string> available, byte[]? clientCapabilities)
    {
        if (clientCapabilities is not { Length: > 0 }) return [];
        var client = EmbeddedIpc.Decode<ClientCapabilities>(clientCapabilities);
        var offered = client.Catalog is { Length: > 0 } catalog
            ? EmbeddedIpc.Decode<CatalogCapabilities>(catalog).Read : [];
        var selected = available.Where(offered.Contains).Distinct(StringComparer.Ordinal).ToList();
        if (!selected.Contains(ConstraintProfiles.RecordsRead))
            selected.Remove(ConstraintProfiles.ScalarExpression);
        selected.Sort(StringComparer.Ordinal);
        return selected;
    }

    /// <summary>
    /// Checks one table's records the way the DuckDB extension does at ATTACH, so the worker's own
    /// tests catch what would otherwise fail every ATTACH: every record is
    /// <see cref="ConstraintEnforcement.Enforced"/> + <see cref="ConstraintValidation.AllRows"/>, and
    /// every Arrow field — descending through ordinary STRUCTs (no <c>ARROW:extension:name</c> or
    /// <c>vgi.logical_type.*</c> metadata) — is non-nullable exactly when a NOT_NULL or PRIMARY_KEY
    /// record names its path. Applies to tables with no records too. The top-level row-id column
    /// (<c>is_row_id</c> metadata) is exempt, as in the extension.
    /// </summary>
    /// <exception cref="InvalidOperationException">Naming the table, constraint id or column path.</exception>
    public static void ValidateTable(string table, Schema columns, IReadOnlyList<byte[]> constraints)
    {
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(constraints);
        var required = new HashSet<string>(StringComparer.Ordinal);
        foreach (var bytes in constraints)
        {
            var record = Read(bytes);
            if (record.Enforcement != ConstraintEnforcement.Enforced || record.Validation != ConstraintValidation.AllRows)
            {
                throw new InvalidOperationException(
                    $"Table '{table}': constraint '{record.Id}' is {Wire(record.Enforcement)}/{Wire(record.Validation)}; " +
                    "DuckDB accepts only ENFORCED + ALL_ROWS records");
            }

            if (record.Kind is ConstraintKind.NotNull or ConstraintKind.PrimaryKey)
            {
                foreach (var path in record.Columns) required.Add(PathKey(path));
            }
        }

        Walk(table, columns.FieldsList, [], required);
    }

    private static void Walk(string table, IEnumerable<Field> fields, List<string> prefix, HashSet<string> required)
    {
        foreach (var field in fields)
        {
            if (prefix.Count == 0 && field.Metadata?.GetValueOrDefault(VgiRowIdMetadata.Key) == VgiRowIdMetadata.Value)
                continue;
            List<string> path = [.. prefix, field.Name];
            if (field.IsNullable == required.Contains(PathKey(path)))
            {
                var column = string.Join('.', path);
                throw new InvalidOperationException(field.IsNullable
                    ? $"Table '{table}': column {column} is in a NOT_NULL/PRIMARY_KEY record but its Arrow field is nullable"
                    : $"Table '{table}': column {column} is non-nullable in Arrow but no NOT_NULL/PRIMARY_KEY record names it; "
                      + "declare one with ConstraintRecord.Build() or make the field nullable");
            }

            if (OrdinaryStruct(field) is { } children) Walk(table, children, path, required);
        }
    }

    private static IReadOnlyList<Field>? OrdinaryStruct(Field field)
    {
        if (field.DataType is not StructType structType) return null;
        if (field.Metadata is { } metadata
            && metadata.Keys.Any(key => key == "ARROW:extension:name" || key.StartsWith("vgi.logical_type.", StringComparison.Ordinal)))
            return null;
        return structType.Fields;
    }

    private static string PathKey(IEnumerable<string> path) => string.Join('\0', path);

    private static string Wire(ConstraintEnforcement value) => value == ConstraintEnforcement.Enforced ? "ENFORCED" : "METADATA_ONLY";

    private static string Wire(ConstraintValidation value) => value switch
    {
        ConstraintValidation.AllRows => "ALL_ROWS",
        ConstraintValidation.FutureWrites => "FUTURE_WRITES",
        _ => "UNVALIDATED",
    };

    private static List<List<string>> Paths(IEnumerable<IReadOnlyList<string>> paths) =>
        paths.Select(path => path.ToList()).ToList();

    private static byte[] ObjectRef(string kind, IEnumerable<string> schemaPath, string name, string? ownerTable) =>
        EmbeddedIpc.Encode(new CatalogObjectRef
        {
            Kind = kind,
            SchemaPath = [.. schemaPath],
            Name = name,
            OwnerTable = ownerTable,
        });
}
