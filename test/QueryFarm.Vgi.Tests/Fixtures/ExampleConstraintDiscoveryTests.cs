using QueryFarm.Vgi.ExampleWorker.Table;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Fixtures;

public class ExampleConstraintDiscoveryTests
{
    [Fact]
    public async Task ReadonlyFixturesPublishExplicitUntrustedConstraints()
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
            Assert.All(fixture.ResolveColumns().FieldsList, field => Assert.True(field.IsNullable));
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
                Assert.Equal(ConstraintEnforcement.MetadataOnly, constraint.Enforcement);
                Assert.Equal(ConstraintValidation.Unvalidated, constraint.Validation);
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
