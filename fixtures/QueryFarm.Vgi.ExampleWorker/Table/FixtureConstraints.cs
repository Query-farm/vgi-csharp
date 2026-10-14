using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>Explicit fixture identities, with no enforcement or row-validation claim.</summary>
internal static class FixtureConstraints
{
    internal static byte[] Revision => "fixture.constraints.v1"u8.ToArray();

    private static byte[] Ref(string kind, string key, string? table) => EmbeddedIpc.Encode(new CatalogObjectRef
    {
        Kind = kind, SchemaPath = ["data"], Name = key, OwnerTable = table,
    });

    internal static byte[] Of(string table, string key, ConstraintKind kind, params string[] columns) =>
        Record(table, key, new ConstraintDefinition
        {
            Kind = kind, ColumnPaths = columns.Select(column => new List<string> { column }).ToList(),
        });

    internal static byte[] Check(string table, string key, string sql) => Record(table, key, new ConstraintDefinition
    {
        Kind = ConstraintKind.Check,
        CheckExpression = EmbeddedIpc.Encode(new SqlExpression
        {
            Sql = sql, Profile = "vgi.expression.duckdb.scalar.v1", SchemaPath = ["data"],
            EvaluationContext = new QueryEvaluationContext
            {
                Timezone = "UTC", Collation = "binary", DefaultOrder = "asc",
                DefaultNullOrder = "nulls_last", PreserveInsertionOrder = true,
            },
        }),
    });

    internal static byte[] DepartmentForeignKey(string table) => Record(table, "department_fk", new ConstraintDefinition
    {
        Kind = ConstraintKind.ForeignKey, ColumnPaths = [["department_id"]],
        ForeignKey = EmbeddedIpc.Encode(new ForeignKeyDefinition
        {
            ReferencedTable = Ref("table", "departments", null), ReferencedColumnPaths = [["id"]],
            Match = ForeignKeyMatch.Simple, OnUpdate = ReferentialAction.NoAction, OnDelete = ReferentialAction.NoAction,
        }),
    }, Ref("constraint", "primary", "departments"));

    private static byte[] Record(string table, string key, ConstraintDefinition definition, byte[]? target = null) =>
        EmbeddedIpc.Encode(new ConstraintInfo
        {
            Ref = Ref("constraint", key, table), Definition = EmbeddedIpc.Encode(definition),
            Enforcement = ConstraintEnforcement.MetadataOnly, Validation = ConstraintValidation.Unvalidated,
            ReferencedConstraint = target,
        });
}
