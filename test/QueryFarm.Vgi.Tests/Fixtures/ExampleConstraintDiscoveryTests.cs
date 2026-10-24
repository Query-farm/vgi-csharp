using QueryFarm.Vgi.ExampleWorker.Table;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Fixtures;

public class ExampleConstraintDiscoveryTests
{
    [Fact]
    public void ReadNegotiationRequiresTheOfferedReadPrerequisite()
    {
        const string read = "vgi.constraint.records.read.v1";
        const string scalar = "vgi.expression.duckdb.scalar.v1";
        foreach (var profiles in new List<string>[] { [], [scalar], [read], [read, scalar] })
        {
            var request = new CatalogAttachRequest
            {
                Name = "example", ClientCapabilities = EmbeddedIpc.Encode(new ClientCapabilities
                {
                    Catalog = EmbeddedIpc.Encode(new CatalogCapabilities { Read = profiles, Write = ["vgi.constraint.enforce.v1"] }),
                }),
            };
            var selected = EmbeddedIpc.Decode<CatalogCapabilities>(FixtureConstraints.Attach(request)!.SelectedCapabilities!);
            Assert.Equal(profiles.Contains(read) ? profiles : [], selected.Read);
            Assert.Empty(selected.Write);
            Assert.Empty(selected.Ddl);
            Assert.Empty(selected.Transaction);
        }
        Assert.Null(FixtureConstraints.Attach(new CatalogAttachRequest { Name = "other" }));
    }

    [Fact]
    public void FixedRowProofRejectsNullDuplicateNegativeAndMissingParent()
    {
        var row = new Dictionary<string, object?> { ["id"] = 1L, ["budget"] = -1.0, ["department_id"] = 7L };
        var notNull = new ConstraintDefinition { Kind = ConstraintKind.NotNull, ColumnPaths = [["id"]] };
        Assert.Throws<InvalidOperationException>(() => FixtureConstraints.Validate(
            [new() { ["id"] = null }], notNull, []));
        Assert.Throws<InvalidOperationException>(() => FixtureConstraints.Validate([row, row],
            new ConstraintDefinition { Kind = ConstraintKind.PrimaryKey, ColumnPaths = [["id"]] }, []));
        Assert.Throws<InvalidOperationException>(() => FixtureConstraints.Validate([row],
            new ConstraintDefinition { Kind = ConstraintKind.ForeignKey, ColumnPaths = [["department_id"]] }, []));
        var check = EmbeddedIpc.Decode<ConstraintInfo>(DataSchemaTables.Departments.Constraints.Last());
        Assert.Throws<InvalidOperationException>(() => FixtureConstraints.Validate([row],
            EmbeddedIpc.Decode<ConstraintDefinition>(check.Definition), []));
    }

    [Fact]
    public async Task ReadonlyFixturesPublishExplicitValidatedConstraints()
    {
        var registry = new CatalogRegistry();
        var fixtures = new[]
        {
            DataSchemaTables.Departments, DataSchemaTables.Employees, DataSchemaTables.Projects,
            DataSchemaTables.Products, DataSchemaTables.Colors, GeoPointsTable.Table,
            VersionedTimeTravelTables.VersionedData, VersionedTimeTravelTables.VersionedConstraints,
        };
        foreach (var fixture in fixtures)
        {
            registry.RegisterCatalogTable(fixture);
            var required = fixture.Constraints.Select(EmbeddedIpc.Decode<ConstraintInfo>)
                .Select(info => EmbeddedIpc.Decode<ConstraintDefinition>(info.Definition))
                .Where(definition => definition.Kind is ConstraintKind.NotNull or ConstraintKind.PrimaryKey)
                .SelectMany(definition => definition.ColumnPaths).Select(path => path[0]).ToHashSet();
            Assert.All(fixture.ResolveColumns().FieldsList,
                field => Assert.Equal(!required.Contains(field.Name), field.IsNullable));
        }

        var service = new VgiServiceImpl(registry);
        var response = await service.CatalogSchemaContentsTablesAsync([], ["data"], null);
        var tables = response.Items.Select(EmbeddedIpc.Decode<TableInfo>).ToList();
        Assert.Equal(fixtures.Length, tables.Count);
        foreach (var table in tables)
        {
            Assert.NotEmpty(table.DefinitionRevision!);
            Assert.NotEmpty(table.Constraints);
            foreach (var bytes in table.Constraints)
            {
                var constraint = EmbeddedIpc.Decode<ConstraintInfo>(bytes);
                Assert.Equal(ConstraintEnforcement.Enforced, constraint.Enforcement);
                Assert.Equal(ConstraintValidation.AllRows, constraint.Validation);
                var identity = EmbeddedIpc.Decode<CatalogObjectRef>(constraint.Ref);
                Assert.Equal(table.Name, identity.OwnerTable);
                Assert.Equal(new[] { "data" }, identity.SchemaPath);
                Assert.NotEmpty(identity.Name);
                var definition = EmbeddedIpc.Decode<ConstraintDefinition>(constraint.Definition);
                if (definition.Kind == ConstraintKind.ForeignKey)
                {
                    var target = EmbeddedIpc.Decode<CatalogObjectRef>(constraint.ReferencedConstraint!);
                    Assert.Equal("departments", target.OwnerTable);
                    Assert.Equal("primary", target.Name);
                }
                if (definition.Kind == ConstraintKind.Check)
                {
                    var expression = EmbeddedIpc.Decode<SqlExpression>(definition.CheckExpression!);
                    Assert.Equal("vgi.expression.duckdb.scalar.v1", expression.Profile);
                    Assert.Equal("UTC", expression.EvaluationContext.Timezone);
                }
            }
        }
    }
}
