using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>Explicit read-only fixture facts checked against their complete fixed rows.</summary>
internal static class FixtureConstraints
{
    internal static AttachContext? Attach(CatalogAttachRequest request)
    {
        if (request.Name != "example") return null;
        List<string> read = ["vgi.constraint.records.read.v1", "vgi.expression.duckdb.scalar.v1"];
        var client = request.ClientCapabilities is { } encoded ? EmbeddedIpc.Decode<ClientCapabilities>(encoded) : null;
        var offered = client?.Catalog is { } bytes
            ? EmbeddedIpc.Decode<CatalogCapabilities>(bytes) : new CatalogCapabilities();
        return new AttachContext
        {
            CatalogCapabilities = EmbeddedIpc.Encode(new CatalogCapabilities { Read = read }),
            SelectedCapabilities = EmbeddedIpc.Encode(new CatalogCapabilities
            {
                Read = read.Where(offered.Read.Contains)
                    .Where(profile => profile == read[0] || offered.Read.Contains(read[0])).Order(StringComparer.Ordinal).ToList(),
            }),
        };
    }

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, List<Dictionary<string, object?>>> Rows = new();
    internal static readonly object VersionLock = new();

    internal static void Register(string table, RecordBatch batch)
    {
        Rows[table] = Enumerable.Range(0, batch.Length).Select(index =>
            batch.Schema.FieldsList.Select((field, column) => (field.Name,
                Value: batch.Column(column).IsNull(index) ? null : batch.Column(column) is StructArray ? (object)true : ScalarArgCodec.ReadScalar(batch.Column(column), index)))
                .ToDictionary(pair => pair.Name, pair => pair.Value)).ToList();
    }

    internal static byte[] SequenceNotNull(string table, string key, string column, int count)
    {
        // Both fixed sequence fixtures emit a non-null Int64 for every ordinal.
        Rows[table] = Enumerable.Range(0, count).Select(value =>
            new Dictionary<string, object?> { [column] = (long)value }).ToList();
        return Of(table, key, ConstraintKind.NotNull, column);
    }

    internal static byte[] Revision => "fixture.constraints.v1"u8.ToArray();

    private static byte[] Ref(string kind, string key, string? table) => EmbeddedIpc.Encode(new CatalogObjectRef
    {
        Kind = kind,
        SchemaPath = ["data"],
        Name = key,
        OwnerTable = table,
    });

    internal static byte[] Of(string table, string key, ConstraintKind kind, params string[] columns) =>
        Record(table, key, new ConstraintDefinition
        {
            Kind = kind,
            ColumnPaths = columns.Select(column => new List<string> { column }).ToList(),
        });

    internal static byte[] Check(string table, string key, string sql) => Record(table, key, new ConstraintDefinition
    {
        Kind = ConstraintKind.Check,
        CheckExpression = EmbeddedIpc.Encode(new SqlExpression
        {
            Sql = sql,
            Profile = "vgi.expression.duckdb.scalar.v1",
            SchemaPath = ["data"],
            EvaluationContext = new QueryEvaluationContext
            {
                Timezone = "UTC",
                Collation = "binary",
                DefaultOrder = "asc",
                DefaultNullOrder = "nulls_last",
                PreserveInsertionOrder = true,
            },
        }),
    });

    internal static byte[] DepartmentForeignKey(string table) => Record(table, "department_fk", new ConstraintDefinition
    {
        Kind = ConstraintKind.ForeignKey,
        ColumnPaths = [["department_id"]],
        ForeignKey = EmbeddedIpc.Encode(new ForeignKeyDefinition
        {
            ReferencedTable = Ref("table", "departments", null),
            ReferencedColumnPaths = [["id"]],
            Match = ForeignKeyMatch.Simple,
            OnUpdate = ReferentialAction.NoAction,
            OnDelete = ReferentialAction.NoAction,
        }),
    }, Ref("constraint", "primary", "departments"));

    private static byte[] Record(string table, string key, ConstraintDefinition definition, byte[]? target = null)
    {
        if (!Rows.TryGetValue(table, out var rows))
            throw new InvalidOperationException($"No immutable row proof for {table}");
        Validate(rows, definition, Rows.GetValueOrDefault("departments") ?? []);
        return EmbeddedIpc.Encode(new ConstraintInfo
        {
            Ref = Ref("constraint", key, table),
            Definition = EmbeddedIpc.Encode(definition),
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.AllRows,
            ReferencedConstraint = target,
        });
    }

    internal static void Validate(IReadOnlyList<Dictionary<string, object?>> rows,
        ConstraintDefinition definition, IReadOnlyList<Dictionary<string, object?>> departments)
    {
        if (definition.Kind is not (ConstraintKind.NotNull or ConstraintKind.PrimaryKey or ConstraintKind.Unique
            or ConstraintKind.Check or ConstraintKind.ForeignKey))
            throw new InvalidOperationException("Unsupported fixture constraint kind");
        var seen = new List<object?[]>();
        foreach (var row in rows)
        {
            var key = definition.ColumnPaths.Select(path => path.Count == 1 && row.ContainsKey(path[0])
                ? row[path[0]] : throw new InvalidOperationException("Unknown fixture constraint path")).ToArray();
            var hasNull = key.Any(value => value is null);
            if (definition.Kind is ConstraintKind.NotNull or ConstraintKind.PrimaryKey && hasNull)
                throw new InvalidOperationException("Fixed fixture violates NOT_NULL");
            if (definition.Kind is ConstraintKind.Unique or ConstraintKind.PrimaryKey && !hasNull)
            {
                if (seen.Any(prior => prior.SequenceEqual(key)))
                    throw new InvalidOperationException("Fixed fixture violates uniqueness");
                seen.Add(key);
            }
            if (definition.Kind == ConstraintKind.Check)
            {
                var expression = EmbeddedIpc.Decode<SqlExpression>(definition.CheckExpression!);
                if (expression.Sql is not "(budget >= 0)" and not "budget >= 0")
                    throw new InvalidOperationException("Unsupported fixture CHECK");
                if (row["budget"] is { } budget && !(Convert.ToDouble(budget) >= 0))
                    throw new InvalidOperationException("Fixed fixture violates budget CHECK");
            }
            if (definition.Kind == ConstraintKind.ForeignKey && !hasNull
                && !departments.Any(parent => Equals(parent["id"], key[0])))
                throw new InvalidOperationException("Fixed fixture violates department reference");
        }
    }
}
