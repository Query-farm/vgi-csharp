using Apache.Arrow;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>Explicit read-only fixture facts checked against their complete fixed rows.</summary>
internal static class FixtureConstraints
{
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

    private static readonly string[] Schema = ["data"];

    internal static byte[] Of(string table, string key, ConstraintKind kind, params string[] columns) =>
        Record(new ConstraintRecordSpec
        {
            SchemaPath = Schema,
            Table = table,
            Id = key,
            Kind = kind,
            Columns = columns.Select(column => (IReadOnlyList<string>)[column]).ToList(),
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.AllRows,
        });

    internal static byte[] Check(string table, string key, string sql) => Record(new ConstraintRecordSpec
    {
        SchemaPath = Schema,
        Table = table,
        Id = key,
        Kind = ConstraintKind.Check,
        CheckSql = sql,
        Enforcement = ConstraintEnforcement.Enforced,
        Validation = ConstraintValidation.AllRows,
    });

    internal static byte[] DepartmentForeignKey(string table) => Record(new ConstraintRecordSpec
    {
        SchemaPath = Schema,
        Table = table,
        Id = "department_fk",
        Kind = ConstraintKind.ForeignKey,
        Columns = [["department_id"]],
        References = new ConstraintReference
        {
            SchemaPath = Schema,
            Table = "departments",
            Columns = [["id"]],
            ConstraintId = "primary",
        },
        Enforcement = ConstraintEnforcement.Enforced,
        Validation = ConstraintValidation.AllRows,
    });

    /// <summary>Encodes the record only after proving it against the table's complete fixed rows:
    /// the fixture attests ENFORCED + ALL_ROWS, so the attestation has to be true.</summary>
    private static byte[] Record(ConstraintRecordSpec spec)
    {
        if (!Rows.TryGetValue(spec.Table, out var rows))
            throw new InvalidOperationException($"No immutable row proof for {spec.Table}");
        var record = ConstraintRecord.Build(spec);
        var definition = EmbeddedIpc.Decode<ConstraintDefinition>(EmbeddedIpc.Decode<ConstraintInfo>(record).Definition);
        Validate(rows, definition, Rows.GetValueOrDefault("departments") ?? []);
        return record;
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
