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

    /// <summary>DDL profile: the catalog accepts <c>CREATE TABLE</c> carrying canonical constraint
    /// definitions. Requires <see cref="RecordsRead"/> and <see cref="Enforce"/>.</summary>
    public const string Create = "vgi.constraint.create.v1";

    /// <summary>DDL profile: the catalog accepts constraint mutations (<c>ALTER ... SET/DROP NOT
    /// NULL</c>, <c>catalog_constraints_apply</c>).</summary>
    public const string Mutate = "vgi.constraint.mutate.v1";

    /// <summary>Write profile: the catalog enforces its records on every write path.</summary>
    public const string Enforce = "vgi.constraint.enforce.v1";
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
/// <see cref="CatalogTable.Constraints"/> entries a v3 table carries. Prefer the table-aware
/// <see cref="For(CatalogTable)"/> builder, which fills each record's owner. A catalog whose
/// registered tables carry records negotiates <see cref="ConstraintProfiles.RecordsRead"/>
/// automatically in <c>catalog_attach</c>; records the registry cannot see at attach time (raw
/// <c>catalog_contents</c> tables, <see cref="CatalogTable.ResolveAtClause"/> versions) need
/// <see cref="Worker.AdvertiseConstraintRecords"/> or <see cref="AttachContext.ServesConstraintRecords"/>.
/// </summary>
public static class ConstraintRecord
{
    /// <summary>A builder for <paramref name="table"/>'s records with its owner filled in — see
    /// <see cref="TableConstraints"/>.</summary>
    public static TableConstraints For(IReadOnlyList<string> schemaPath, string table) => new(schemaPath, table);

    /// <summary>A builder for <paramref name="table"/>'s records with its owner
    /// (<see cref="CatalogTable.SchemaPath"/>/<see cref="CatalogTable.SchemaName"/> and
    /// <see cref="CatalogTable.Name"/>) filled in — see <see cref="TableConstraints"/>.</summary>
    public static TableConstraints For(CatalogTable table)
    {
        ArgumentNullException.ThrowIfNull(table);
        return new(table.EffectiveSchemaPath, table.Name);
    }

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

    /// <summary>Encodes a <see cref="CatalogCapabilities"/> advertisement or selection (for
    /// <see cref="AttachContext.CatalogCapabilities"/>, or a test's client offer).</summary>
    public static byte[] EncodeCapabilities(
        IEnumerable<string>? read = null, IEnumerable<string>? ddl = null,
        IEnumerable<string>? write = null, IEnumerable<string>? transaction = null) =>
        EmbeddedIpc.Encode(new CatalogCapabilities
        {
            Read = Canonical(read),
            Ddl = Canonical(ddl),
            Write = Canonical(write),
            Transaction = Canonical(transaction),
        });

    /// <summary>Decodes an encoded <see cref="CatalogCapabilities"/>; an empty array decodes to an
    /// empty set.</summary>
    public static CatalogCapabilities DecodeCapabilities(byte[]? capabilities) =>
        capabilities is { Length: > 0 } ? EmbeddedIpc.Decode<CatalogCapabilities>(capabilities) : new CatalogCapabilities();

    /// <summary>
    /// Selects, per category, the profiles in <paramref name="advertised"/> that the client offers
    /// in its <c>catalog</c> capabilities (<see cref="CatalogAttachRequest.ClientCapabilities"/>),
    /// in canonical (ordinal) order. Dependent profiles are dropped when their prerequisite is
    /// not selected, exactly as the DuckDB extension requires: the scalar-expression read profile
    /// and the constraint create/mutate/enforce profiles need
    /// <see cref="ConstraintProfiles.RecordsRead"/>.
    /// </summary>
    public static CatalogCapabilities SelectProfiles(CatalogCapabilities advertised, byte[]? clientCapabilities)
    {
        ArgumentNullException.ThrowIfNull(advertised);
        var offered = new CatalogCapabilities();
        if (clientCapabilities is { Length: > 0 }
            && EmbeddedIpc.Decode<ClientCapabilities>(clientCapabilities).Catalog is { Length: > 0 } catalog)
            offered = EmbeddedIpc.Decode<CatalogCapabilities>(catalog);
        var selected = new CatalogCapabilities
        {
            Read = Canonical(advertised.Read.Where(offered.Read.Contains)),
            Ddl = Canonical(advertised.Ddl.Where(offered.Ddl.Contains)),
            Write = Canonical(advertised.Write.Where(offered.Write.Contains)),
            Transaction = Canonical(advertised.Transaction.Where(offered.Transaction.Contains)),
        };
        if (!selected.Read.Contains(ConstraintProfiles.RecordsRead))
        {
            selected.Read.Remove(ConstraintProfiles.ScalarExpression);
            selected.Ddl.RemoveAll(profile => profile is ConstraintProfiles.Create or ConstraintProfiles.Mutate);
            selected.Write.Remove(ConstraintProfiles.Enforce);
        }

        return selected;
    }

    private static List<string> Canonical(IEnumerable<string>? profiles) =>
        (profiles ?? []).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();

    /// <summary>
    /// Checks one table's records exactly as the DuckDB extension does at ATTACH
    /// (<c>DecodeNativeConstraints</c>) and returns the Arrow schema to serve.
    /// <para>Rejected, naming the table, constraint id and column path: a record whose owner or
    /// schema path is not this table's; duplicate constraint ids; duplicate declared names
    /// (case-insensitive); anything but <see cref="ConstraintEnforcement.Enforced"/> +
    /// <see cref="ConstraintValidation.AllRows"/>; more than one PRIMARY KEY; a column path that does
    /// not resolve (NOT_NULL may name a field nested in ordinary STRUCTs; other kinds take top-level
    /// columns only; the row-id column never); a FOREIGN KEY outside this schema, with an action
    /// other than NO ACTION/RESTRICT, MATCH other than SIMPLE, or — when
    /// <paramref name="referencedConstraints"/> is supplied — naming a referenced PRIMARY KEY/UNIQUE
    /// record that does not exist over those columns; a CHECK outside the extension's numeric
    /// comparison grammar (<c>&lt;column|number&gt; &lt;op&gt; &lt;column|number&gt;</c> over signed
    /// integer/float columns); a NOT_NULL/PRIMARY KEY on a VIRTUAL generated column.</para>
    /// <para>Nullability is then derived, never claimed: every field a NOT_NULL or PRIMARY KEY
    /// record names is served non-nullable (the worker attested it, ENFORCED + ALL_ROWS) and every
    /// other field is served nullable, descending ordinary STRUCTs (not ones carrying
    /// <c>ARROW:extension:name</c> or <c>vgi.logical_type.*</c> metadata). The top-level row-id
    /// column (<c>is_row_id</c> metadata) is served unchanged. Arrow non-nullability on its own is
    /// therefore never an error — it is just not a SQL fact.</para>
    /// </summary>
    /// <param name="schemaPath">The owning table's schema path.</param>
    /// <param name="table">The owning table's name.</param>
    /// <param name="columns">The table's declared Arrow columns.</param>
    /// <param name="constraints">The table's canonical records (<see cref="Build"/>).</param>
    /// <param name="referencedConstraints">Optional lookup of another table's records in the same
    /// catalog, by schema path and name (<see langword="null"/> when that table is not served),
    /// used to check FOREIGN KEY targets. Omit it to skip the cross-table check.</param>
    /// <returns>The schema to serve.</returns>
    /// <exception cref="InvalidOperationException">Naming the table, constraint id or column path.</exception>
    public static Schema ValidateTable(
        IReadOnlyList<string> schemaPath, string table, Schema columns, IReadOnlyList<byte[]> constraints,
        Func<IReadOnlyList<string>, string, IReadOnlyList<byte[]>?>? referencedConstraints = null)
    {
        ArgumentNullException.ThrowIfNull(schemaPath);
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(constraints);
        var where = $"Table '{string.Join('.', schemaPath)}.{table}'";
        var required = RequiredPaths(where, schemaPath, table, columns, constraints, referencedConstraints);
        return new Schema(Serve(columns.FieldsList, [], required), columns.Metadata);
    }

    private const int MaxRecordBytes = 8 * 1024 * 1024;

    private static HashSet<string> RequiredPaths(
        string where, IReadOnlyList<string> schemaPath, string table, Schema columns, IReadOnlyList<byte[]> constraints,
        Func<IReadOnlyList<string>, string, IReadOnlyList<byte[]>?>? referencedConstraints)
    {
        InvalidOperationException Fail(string message) => new($"{where}: {message}");
        var rowId = -1;
        for (var i = 0; i < columns.FieldsList.Count; i++)
        {
            if (columns.FieldsList[i].Metadata?.ContainsKey(VgiRowIdMetadata.Key) != true) continue;
            if (rowId >= 0) throw Fail("has multiple is_row_id columns; at most one is allowed");
            rowId = i;
        }

        var required = new HashSet<string>(StringComparer.Ordinal);
        if (constraints.Count == 0) return required;
        if (constraints.Count > 1024) throw Fail($"carries {constraints.Count} constraints; at most 1024 are allowed");
        if (constraints.Sum(record => (long)record.Length) > MaxRecordBytes)
            throw Fail("constraint records exceed the 8 MiB collection limit");
        if (schemaPath.Count != 1)
            throw Fail("constraint records require a one-component schema path (the DuckDB extension's native adapter)");

        var ids = new HashSet<string>(StringComparer.Ordinal);
        var labels = new HashSet<string>(StringComparer.Ordinal);
        var primaryKeys = 0;
        foreach (var bytes in constraints)
        {
            if (bytes is not { Length: > 0 } || bytes.Length > MaxRecordBytes) throw Fail("has an empty or oversized constraint record");
            var info = EmbeddedIpc.Decode<ConstraintInfo>(bytes);
            var reference = EmbeddedIpc.Decode<CatalogObjectRef>(info.Ref);
            var id = reference.Name;
            InvalidOperationException FailRecord(string message) => Fail($"constraint '{id}': {message}");
            if (reference.Kind != "constraint") throw FailRecord($"ref kind is '{reference.Kind}', not 'constraint'");
            if (!Identifier(id)) throw FailRecord("id must be a nonempty identifier without NUL (at most 4096 bytes)");
            if (!reference.SchemaPath.SequenceEqual(schemaPath, StringComparer.Ordinal) || reference.OwnerTable != table)
            {
                throw FailRecord(
                    $"belongs to {string.Join('.', reference.SchemaPath)}.{reference.OwnerTable ?? "<no owner>"}, not this table; " +
                    "records must name their own table and schema (use ConstraintRecord.For(table) to fill them)");
            }

            if (!ids.Add(id)) throw FailRecord("duplicate constraint id");
            if (info.Enforcement != ConstraintEnforcement.Enforced || info.Validation != ConstraintValidation.AllRows)
                throw FailRecord($"is {Wire(info.Enforcement)}/{Wire(info.Validation)}; DuckDB accepts only ENFORCED + ALL_ROWS records");
            var definition = EmbeddedIpc.Decode<ConstraintDefinition>(info.Definition);
            if (definition.DeclaredName is { } label)
            {
                if (!Identifier(label)) throw FailRecord("declared name must be a nonempty identifier without NUL");
                if (!labels.Add(AsciiLower(label))) throw FailRecord($"duplicate declared name '{label}' (names compare case-insensitively)");
            }

            var paths = definition.ColumnPaths;
            if (paths.Count > 1024) throw FailRecord("has more than 1024 column paths");
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var path in paths)
            {
                if (path.Count is 0 or > 64 || !path.All(Identifier))
                    throw FailRecord("column paths must have 1-64 nonempty identifier components");
                if (!seen.Add(PathKey(path))) throw FailRecord($"duplicate column path {Dotted(path)}");
            }

            var isCheck = definition.Kind == ConstraintKind.Check;
            if (isCheck != (definition.CheckExpression is not null)) throw FailRecord("CHECK requires a check expression and only CHECK carries one");
            if (isCheck == (paths.Count > 0)) throw FailRecord(isCheck ? "CHECK takes no column paths" : $"{definition.Kind} requires column paths");
            var isForeign = definition.Kind == ConstraintKind.ForeignKey;
            if (isForeign != (definition.ForeignKey is not null) || isForeign != (info.ReferencedConstraint is not null))
                throw FailRecord("FOREIGN_KEY requires a foreign key definition and referenced constraint, and only FOREIGN_KEY carries them");
            switch (definition.Kind)
            {
                case ConstraintKind.Check:
                    CheckExpression(EmbeddedIpc.Decode<SqlExpression>(definition.CheckExpression!), schemaPath, columns, rowId, FailRecord);
                    break;
                case ConstraintKind.NotNull:
                    if (paths.Count != 1) throw FailRecord("NOT_NULL requires exactly one column path");
                    ResolvePath(columns, rowId, paths[0], FailRecord);
                    required.Add(PathKey(paths[0]));
                    break;
                case ConstraintKind.PrimaryKey:
                case ConstraintKind.Unique:
                case ConstraintKind.ForeignKey:
                    foreach (var path in paths)
                    {
                        if (path.Count != 1)
                            throw FailRecord($"column path {Dotted(path)} is nested; only NOT_NULL may name nested fields");
                        ResolvePath(columns, rowId, path, FailRecord);
                    }

                    if (definition.Kind == ConstraintKind.PrimaryKey)
                    {
                        if (++primaryKeys > 1) throw FailRecord("a table may carry only one PRIMARY_KEY");
                        foreach (var path in paths) required.Add(PathKey(path));
                    }
                    else if (isForeign)
                    {
                        ForeignKey(info, definition, schemaPath, table, constraints, referencedConstraints, FailRecord);
                    }

                    break;
                default:
                    throw FailRecord($"unknown constraint kind {definition.Kind}");
            }
        }

        foreach (var field in columns.FieldsList)
        {
            if (required.Contains(PathKey([field.Name])) && field.Metadata is { } metadata
                && (metadata.ContainsKey(ColumnExpressions.GeneratedKey) || metadata.ContainsKey(ColumnExpressions.LegacyGeneratedKey)))
                throw Fail($"column {field.Name} is a VIRTUAL generated column; generated columns must be nullable, so no NOT_NULL/PRIMARY_KEY record may name it");
        }

        return required;
    }

    private static void ForeignKey(
        ConstraintInfo info, ConstraintDefinition definition, IReadOnlyList<string> schemaPath, string table,
        IReadOnlyList<byte[]> own, Func<IReadOnlyList<string>, string, IReadOnlyList<byte[]>?>? lookup,
        Func<string, InvalidOperationException> fail)
    {
        var fk = EmbeddedIpc.Decode<ForeignKeyDefinition>(definition.ForeignKey!);
        var target = EmbeddedIpc.Decode<CatalogObjectRef>(fk.ReferencedTable);
        var key = EmbeddedIpc.Decode<CatalogObjectRef>(info.ReferencedConstraint!);
        if (target.Kind != "table" || target.OwnerTable is not null || key.Kind != "constraint")
            throw fail("foreign key must reference a table ref and a constraint ref");
        if (!target.SchemaPath.SequenceEqual(schemaPath, StringComparer.Ordinal)
            || !key.SchemaPath.SequenceEqual(schemaPath, StringComparer.Ordinal) || key.OwnerTable != target.Name)
        {
            throw fail($"foreign key must reference an identified key of a table in schema {string.Join('.', schemaPath)} " +
                       $"(got table {string.Join('.', target.SchemaPath)}.{target.Name}, key owner {key.OwnerTable})");
        }

        if (!Identifier(target.Name) || !Identifier(key.Name)) throw fail("foreign key target names must be identifiers");
        var referenced = fk.ReferencedColumnPaths;
        if (referenced.Any(path => path.Count != 1 || !Identifier(path[0])))
            throw fail("foreign key referenced columns must be top-level column names");
        if (referenced.Select(path => path[0]).Distinct(StringComparer.Ordinal).Count() != referenced.Count)
            throw fail("foreign key referenced columns repeat");
        if (referenced.Count != definition.ColumnPaths.Count) throw fail("foreign key local and referenced arity differ");
        if (fk.Match != ForeignKeyMatch.Simple) throw fail("foreign key requires MATCH SIMPLE");
        if (fk.OnUpdate is not (ReferentialAction.NoAction or ReferentialAction.Restrict)
            || fk.OnDelete is not (ReferentialAction.NoAction or ReferentialAction.Restrict))
            throw fail($"foreign key actions ON UPDATE {fk.OnUpdate} / ON DELETE {fk.OnDelete} are unsupported; DuckDB accepts only NO ACTION or RESTRICT");

        var records = target.Name == table ? own : lookup?.Invoke(schemaPath, target.Name);
        if (lookup is null && target.Name != table) return;
        if (records is null)
            throw fail($"foreign key references table {string.Join('.', schemaPath)}.{target.Name}, which this catalog does not serve");
        var wanted = referenced.Select(path => path[0]).ToHashSet(StringComparer.Ordinal);
        foreach (var bytes in records)
        {
            var candidate = EmbeddedIpc.Decode<ConstraintInfo>(bytes);
            if (EmbeddedIpc.Decode<CatalogObjectRef>(candidate.Ref).Name != key.Name) continue;
            var candidateDefinition = EmbeddedIpc.Decode<ConstraintDefinition>(candidate.Definition);
            if (candidateDefinition.Kind is not (ConstraintKind.PrimaryKey or ConstraintKind.Unique))
                throw fail($"foreign key references constraint '{key.Name}' of {target.Name}, which is {candidateDefinition.Kind}, not PRIMARY_KEY/UNIQUE");
            if (!candidateDefinition.ColumnPaths.Select(path => string.Join('.', path)).ToHashSet(StringComparer.Ordinal).SetEquals(wanted))
                throw fail($"foreign key referenced columns ({string.Join(", ", wanted)}) differ from {target.Name}'s key '{key.Name}'");
            return;
        }

        throw fail($"foreign key references constraint '{key.Name}', which table {target.Name} does not carry");
    }

    private static void ResolvePath(Schema columns, int rowId, IReadOnlyList<string> path, Func<string, InvalidOperationException> fail)
    {
        var matches = columns.FieldsList.Select((field, index) => (field, index)).Where(pair => pair.field.Name == path[0]).ToList();
        if (matches.Count != 1 || matches[0].index == rowId)
            throw fail($"column path {Dotted(path)} names a missing, ambiguous or row-id column");
        var field = matches[0].field;
        for (var depth = 1; depth < path.Count; depth++)
        {
            var children = OrdinaryStruct(field)
                ?? throw fail($"column path {Dotted(path)} must traverse ordinary STRUCT fields, not '{field.Name}'");
            var next = children.Where(child => child.Name == path[depth]).ToList();
            if (next.Count != 1) throw fail($"column path {Dotted(path)} names a missing or ambiguous nested field");
            field = next[0];
        }
    }

    private static void CheckExpression(
        SqlExpression expression, IReadOnlyList<string> schemaPath, Schema columns, int rowId, Func<string, InvalidOperationException> fail)
    {
        if (expression.Profile != ConstraintProfiles.ScalarExpression)
            throw fail($"CHECK profile '{expression.Profile}' is unsupported; use {ConstraintProfiles.ScalarExpression}");
        if (!expression.SchemaPath.SequenceEqual(schemaPath, StringComparer.Ordinal))
            throw fail("CHECK expression belongs to another schema");
        var context = expression.EvaluationContext;
        if (context is not { Timezone: "UTC", Collation: "binary", DefaultOrder: "asc", DefaultNullOrder: "nulls_last", PreserveInsertionOrder: true })
            throw fail("CHECK evaluation context must be UTC/binary/asc/nulls_last with insertion order preserved");
        var sql = expression.Sql;
        if (string.IsNullOrEmpty(sql) || sql.Contains('\0') || System.Text.Encoding.UTF8.GetByteCount(sql) > 1024 * 1024)
            throw fail("CHECK SQL must be nonempty text without NUL (at most 1 MiB)");
        ScalarSqlGrammar.Node node;
        try
        {
            node = ScalarSqlGrammar.Parse(sql);
        }
        catch (FormatException error)
        {
            throw fail($"CHECK ({sql}) is outside the supported grammar: {error.Message}");
        }

        if (node is not ScalarSqlGrammar.Comparison comparison)
            throw fail($"CHECK ({sql}) must be one numeric comparison (=, <>, <, >, <=, >=) between columns and numbers");
        foreach (var operand in new[] { comparison.Left, comparison.Right })
        {
            switch (operand)
            {
                case ScalarSqlGrammar.Constant { Type: not (ScalarSqlGrammar.ConstantType.Null or ScalarSqlGrammar.ConstantType.Boolean or ScalarSqlGrammar.ConstantType.Varchar) }:
                    break;
                case ScalarSqlGrammar.ColumnRef { Names.Count: 1 } column:
                    {
                        var matches = columns.FieldsList.Select((field, index) => (field, index))
                            .Where(pair => string.Equals(pair.field.Name, column.Names[0], StringComparison.OrdinalIgnoreCase)).ToList();
                        if (matches.Count > 1) throw fail($"CHECK ({sql}) column reference {column.Names[0]} is ambiguous");
                        if (matches.Count == 0 || matches[0].index == rowId)
                            throw fail($"CHECK ({sql}) references missing or row-id column {column.Names[0]}");
                        var field = matches[0].field;
                        if (field.Metadata?.ContainsKey("ARROW:extension:name") == true)
                            throw fail($"CHECK ({sql}) operand {field.Name} is extension-typed");
                        if (field.DataType.TypeId is not (ArrowTypeId.Int8 or ArrowTypeId.Int16 or ArrowTypeId.Int32 or ArrowTypeId.Int64
                            or ArrowTypeId.Float or ArrowTypeId.Double))
                            throw fail($"CHECK ({sql}) operand {field.Name} must be a signed integer or floating-point column");
                        break;
                    }

                default:
                    throw fail($"CHECK ({sql}) operands must be unqualified numeric columns or numeric literals");
            }
        }
    }

    /// <summary>Rebuilds <paramref name="fields"/> with the served nullability (see
    /// <see cref="ValidateTable(IReadOnlyList{string}, string, Schema, IReadOnlyList{byte[]}, Func{IReadOnlyList{string}, string, IReadOnlyList{byte[]}?}?)"/>).</summary>
    private static List<Field> Serve(IEnumerable<Field> fields, List<string> prefix, HashSet<string> required)
    {
        var served = new List<Field>();
        foreach (var field in fields)
        {
            if (prefix.Count == 0 && field.Metadata?.GetValueOrDefault(VgiRowIdMetadata.Key) == VgiRowIdMetadata.Value)
            {
                served.Add(field);
                continue;
            }

            List<string> path = [.. prefix, field.Name];
            var type = OrdinaryStruct(field) is { } children ? new StructType(Serve(children, path, required)) : field.DataType;
            served.Add(new Field(field.Name, type, !required.Contains(PathKey(path)), field.Metadata));
        }

        return served;
    }

    private static IReadOnlyList<Field>? OrdinaryStruct(Field field)
    {
        if (field.DataType is not StructType structType) return null;
        if (field.Metadata is { } metadata
            && metadata.Keys.Any(key => key == "ARROW:extension:name" || key.StartsWith("vgi.logical_type.", StringComparison.Ordinal)))
            return null;
        return structType.Fields;
    }

    private static bool Identifier(string? value) =>
        !string.IsNullOrEmpty(value) && !value.Contains('\0') && System.Text.Encoding.UTF8.GetByteCount(value) <= 4096;

    private static string AsciiLower(string value) =>
        string.Create(value.Length, value, (span, text) =>
        {
            for (var i = 0; i < text.Length; i++) span[i] = text[i] is >= 'A' and <= 'Z' ? (char)(text[i] + 32) : text[i];
        });

    private static string Dotted(IEnumerable<string> path) => string.Join('.', path);

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
