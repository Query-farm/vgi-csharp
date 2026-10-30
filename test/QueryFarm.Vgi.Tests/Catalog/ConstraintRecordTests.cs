using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Catalog;

public class ConstraintRecordTests
{
    private static readonly string[] Main = ["main"];

    private static ConstraintRecordSpec Spec(string table, string id, ConstraintKind kind, params string[][] columns) => new()
    {
        SchemaPath = Main,
        Table = table,
        Id = id,
        Kind = kind,
        Columns = columns,
        Enforcement = ConstraintEnforcement.Enforced,
        Validation = ConstraintValidation.AllRows,
    };

    private static byte[] NotNull(string table, params string[] path) =>
        ConstraintRecord.Build(Spec(table, $"{table}.{string.Join('.', path)}", ConstraintKind.NotNull, path));

    private static byte[] Offer(params string[] read) => EmbeddedIpc.Encode(new ClientCapabilities
    {
        Engine = "test",
        Catalog = EmbeddedIpc.Encode(new CatalogCapabilities { Read = [.. read] }),
    });

    private static CatalogTable Table(string name, Schema columns, params byte[][] constraints) => new()
    {
        Name = name,
        SchemaName = "main",
        Columns = columns,
        Constraints = constraints,
    };

    private static Schema Columns(params Field[] fields) => new(fields, null);

    private static async Task<List<TableInfo>> Serve(params CatalogTable[] tables)
    {
        var registry = new CatalogRegistry();
        foreach (var table in tables) registry.RegisterCatalogTable(table);
        var response = await new VgiServiceImpl(registry).CatalogSchemaContentsTablesAsync([], ["main"], null);
        return response.Items.Select(EmbeddedIpc.Decode<TableInfo>).ToList();
    }

    [Fact]
    public void BuildRoundTripsThroughRead()
    {
        var foreign = new ConstraintRecordSpec
        {
            SchemaPath = Main,
            Table = "child",
            Id = "child.parent",
            Kind = ConstraintKind.ForeignKey,
            Columns = [["parent_id"]],
            DeclaredName = "parent_fk",
            References = new ConstraintReference
            {
                SchemaPath = Main,
                Table = "parent",
                Columns = [["id"]],
                ConstraintId = "parent.pk",
            },
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.AllRows,
        };
        var bytes = ConstraintRecord.Build(foreign);
        var info = EmbeddedIpc.Decode<ConstraintInfo>(bytes);
        var reference = EmbeddedIpc.Decode<CatalogObjectRef>(info.Ref);
        Assert.Equal(("constraint", "child.parent", "child"), (reference.Kind, reference.Name, reference.OwnerTable));
        var target = EmbeddedIpc.Decode<CatalogObjectRef>(info.ReferencedConstraint!);
        Assert.Equal(("constraint", "parent.pk", "parent"), (target.Kind, target.Name, target.OwnerTable));

        var read = ConstraintRecord.Read(bytes);
        Assert.Equal(foreign.SchemaPath, read.SchemaPath);
        Assert.Equal(("child", "child.parent", ConstraintKind.ForeignKey, "parent_fk"), (read.Table, read.Id, read.Kind, read.DeclaredName));
        Assert.Equal([["parent_id"]], read.Columns.Select(path => path.ToArray()).ToArray());
        Assert.Equal((ConstraintEnforcement.Enforced, ConstraintValidation.AllRows), (read.Enforcement, read.Validation));
        Assert.Null(read.CheckSql);
        Assert.Equal(("parent", "parent.pk"), (read.References!.Table, read.References.ConstraintId));
        Assert.Equal([["id"]], read.References.Columns.Select(path => path.ToArray()).ToArray());

        var check = ConstraintRecord.Read(ConstraintRecord.Build(new ConstraintRecordSpec
        {
            SchemaPath = Main,
            Table = "t",
            Id = "t.positive",
            Kind = ConstraintKind.Check,
            CheckSql = "a > 0",
            Enforcement = ConstraintEnforcement.MetadataOnly,
            Validation = ConstraintValidation.Unvalidated,
        }));
        Assert.Equal("a > 0", check.CheckSql);
        Assert.Empty(check.Columns);
        Assert.Null(check.References);
        Assert.Equal((ConstraintEnforcement.MetadataOnly, ConstraintValidation.Unvalidated), (check.Enforcement, check.Validation));
        var checkBytes = ConstraintRecord.Build(new ConstraintRecordSpec
        {
            SchemaPath = Main,
            Table = "t",
            Id = "c",
            Kind = ConstraintKind.Check,
            CheckSql = "a > 0",
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.AllRows,
        });
        var expression = EmbeddedIpc.Decode<SqlExpression>(EmbeddedIpc.Decode<ConstraintDefinition>(
            EmbeddedIpc.Decode<ConstraintInfo>(checkBytes).Definition).CheckExpression!);
        Assert.Equal(ConstraintProfiles.ScalarExpression, expression.Profile);
    }

    [Fact]
    public void BuildRejectsShapesTheProtocolRejects()
    {
        Assert.Throws<ArgumentException>(() => ConstraintRecord.Build(Spec("t", "x", ConstraintKind.NotNull, ["a"], ["b"])));
        Assert.Throws<ArgumentException>(() => ConstraintRecord.Build(Spec("t", "x", ConstraintKind.PrimaryKey)));
        Assert.Throws<ArgumentException>(() => ConstraintRecord.Build(Spec("t", "x", ConstraintKind.ForeignKey, ["a"])));
        Assert.Throws<ArgumentException>(() => ConstraintRecord.Build(Spec("t", "", ConstraintKind.Unique, ["a"])));
        Assert.Throws<ArgumentException>(() => ConstraintRecord.Build(Spec("t", "x", ConstraintKind.Unique, [])));
        var check = Spec("t", "x", ConstraintKind.Check, ["a"]);
        Assert.Throws<ArgumentException>(() => ConstraintRecord.Build(new ConstraintRecordSpec
        {
            SchemaPath = Main,
            Table = "t",
            Id = "x",
            Kind = ConstraintKind.Check,
            Columns = check.Columns,
            CheckSql = "a > 0",
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.AllRows,
        }));
        Assert.Throws<ArgumentException>(() => ConstraintRecord.Build(new ConstraintRecordSpec
        {
            SchemaPath = Main,
            Table = "t",
            Id = "x",
            Kind = ConstraintKind.Unique,
            Columns = [["a"]],
            CheckSql = "a > 0",
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.AllRows,
        }));
    }

    [Fact]
    public void SelectionRequiresRecordsForTheScalarProfile()
    {
        string[] both = [ConstraintProfiles.RecordsRead, ConstraintProfiles.ScalarExpression];
        Assert.Equal(both, ConstraintRecord.SelectReadProfiles(both, Offer(ConstraintProfiles.ScalarExpression, ConstraintProfiles.RecordsRead)));
        Assert.Equal([ConstraintProfiles.RecordsRead], ConstraintRecord.SelectReadProfiles(both, Offer(ConstraintProfiles.RecordsRead)));
        Assert.Empty(ConstraintRecord.SelectReadProfiles(both, Offer(ConstraintProfiles.ScalarExpression)));
        Assert.Empty(ConstraintRecord.SelectReadProfiles(both, Offer()));
        Assert.Empty(ConstraintRecord.SelectReadProfiles(both, null));
        Assert.Empty(ConstraintRecord.SelectReadProfiles([], Offer(both)));
    }

    private static CatalogRegistry RegistryWithRecords()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(Table("t", Columns(new Field("id", Int64Type.Default, false)),
            ConstraintRecord.Build(Spec("t", "t.pk", ConstraintKind.PrimaryKey, ["id"]))));
        return registry;
    }

    [Fact]
    public async Task DeclarativeCatalogNegotiatesRecordsWithoutWorkerCode()
    {
        var service = new VgiServiceImpl(RegistryWithRecords());
        var result = await service.CatalogAttachAsync(new CatalogAttachRequest
        {
            Name = "c",
            ClientCapabilities = Offer(ConstraintProfiles.RecordsRead, ConstraintProfiles.ScalarExpression),
        });
        string[] both = [ConstraintProfiles.RecordsRead, ConstraintProfiles.ScalarExpression];
        Assert.Equal(both, EmbeddedIpc.Decode<CatalogCapabilities>(result.CatalogCapabilities!).Read);
        Assert.Equal(both, EmbeddedIpc.Decode<CatalogCapabilities>(result.SelectedCapabilities!).Read);

        var unoffered = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c" });
        Assert.Equal(both, EmbeddedIpc.Decode<CatalogCapabilities>(unoffered.CatalogCapabilities!).Read);
        Assert.Empty(EmbeddedIpc.Decode<CatalogCapabilities>(unoffered.SelectedCapabilities!).Read);
    }

    [Fact]
    public async Task CatalogWithoutRecordsAdvertisesNothing()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(Table("t", Columns(new Field("id", Int64Type.Default, true))));
        var result = await new VgiServiceImpl(registry).CatalogAttachAsync(new CatalogAttachRequest
        {
            Name = "c",
            ClientCapabilities = Offer(ConstraintProfiles.RecordsRead),
        });
        Assert.Null(result.CatalogCapabilities);
        Assert.Null(result.SelectedCapabilities);
    }

    [Fact]
    public async Task OnAttachHandlerWithoutCapabilitiesDoesNotDropNegotiation()
    {
        var registry = RegistryWithRecords();
        registry.OnAttach = _ => new AttachContext { ResolvedDataVersion = "1" };
        var result = await new VgiServiceImpl(registry).CatalogAttachAsync(new CatalogAttachRequest
        {
            Name = "c",
            ClientCapabilities = Offer(ConstraintProfiles.RecordsRead),
        });
        Assert.Equal("1", result.ResolvedDataVersion);
        Assert.Equal([ConstraintProfiles.RecordsRead], EmbeddedIpc.Decode<CatalogCapabilities>(result.SelectedCapabilities!).Read);
    }

    [Fact]
    public async Task ExplicitHandlerCapabilitiesMergeWithRecords()
    {
        var registry = RegistryWithRecords();
        registry.OnAttach = _ => new AttachContext
        {
            CatalogCapabilities = EmbeddedIpc.Encode(new CatalogCapabilities { Read = ["x.custom.v1"], Transaction = ["vgi.transaction.ddl.v1"] }),
            SelectedCapabilities = EmbeddedIpc.Encode(new CatalogCapabilities { Read = ["ignored"], Transaction = ["vgi.transaction.ddl.v1"] }),
        };
        var result = await new VgiServiceImpl(registry).CatalogAttachAsync(new CatalogAttachRequest
        {
            Name = "c",
            ClientCapabilities = Offer("x.custom.v1", ConstraintProfiles.RecordsRead),
        });
        var advertised = EmbeddedIpc.Decode<CatalogCapabilities>(result.CatalogCapabilities!);
        Assert.Equal([ConstraintProfiles.RecordsRead, ConstraintProfiles.ScalarExpression, "x.custom.v1"], advertised.Read);
        Assert.Equal(["vgi.transaction.ddl.v1"], advertised.Transaction);
        var selected = EmbeddedIpc.Decode<CatalogCapabilities>(result.SelectedCapabilities!);
        Assert.Equal([ConstraintProfiles.RecordsRead, "x.custom.v1"], selected.Read);
        Assert.Equal(["vgi.transaction.ddl.v1"], selected.Transaction);
    }

    [Fact]
    public async Task OnlyEnforcedAllRowsRecordsAreServed()
    {
        var record = ConstraintRecord.Build(new ConstraintRecordSpec
        {
            SchemaPath = Main,
            Table = "t",
            Id = "t.a.unique",
            Kind = ConstraintKind.Unique,
            Columns = [["a"]],
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.FutureWrites,
        });
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => Serve(Table("t", Columns(new Field("a", StringType.Default, true)), record)));
        Assert.Contains("Table 't': constraint 't.a.unique' is ENFORCED/FUTURE_WRITES", error.Message);
        Assert.Contains("only ENFORCED + ALL_ROWS", error.Message);
    }

    [Fact]
    public async Task ArrowNullabilityMustAgreeWithNotNullAndPrimaryKey()
    {
        var required = Columns(new Field("a", StringType.Default, false));
        var optional = Columns(new Field("a", StringType.Default, true));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Serve(Table("t", required)));
        Assert.Contains("Table 't': column a is non-nullable in Arrow but no NOT_NULL/PRIMARY_KEY record", error.Message);
        error = await Assert.ThrowsAsync<InvalidOperationException>(() => Serve(Table("t", optional, NotNull("t", "a"))));
        Assert.Contains("Table 't': column a is in a NOT_NULL/PRIMARY_KEY record but its Arrow field is nullable", error.Message);

        Assert.Single(await Serve(Table("t", required, NotNull("t", "a"))));
        Assert.Single(await Serve(Table("t", required, ConstraintRecord.Build(Spec("t", "t.pk", ConstraintKind.PrimaryKey, ["a"])))));
        Assert.Single(await Serve(Table("t", optional)));
    }

    [Fact]
    public async Task NullabilityRuleDescendsOrdinaryStructsOnly()
    {
        var nested = Columns(new Field("s", new StructType([new Field("x", StringType.Default, false)]), true));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Serve(Table("t", nested)));
        Assert.Contains("column s.x is non-nullable", error.Message);
        Assert.Single(await Serve(Table("t", nested, NotNull("t", "s", "x"))));

        // A struct carrying extension metadata is a storage type, not an ordinary STRUCT: its
        // members are not column paths, so the rule does not descend into it.
        var extension = Columns(new Field("s", new StructType([new Field("x", StringType.Default, false)]), true,
            new Dictionary<string, string> { ["ARROW:extension:name"] = "example.ext" }));
        Assert.Single(await Serve(Table("t", extension)));
    }

    [Fact]
    public void RowIdColumnIsExempt()
    {
        var columns = Columns(new Field("rowid", Int64Type.Default, false,
            new Dictionary<string, string> { [VgiRowIdMetadata.Key] = VgiRowIdMetadata.Value }));
        ConstraintRecord.ValidateTable("t", columns, []);
    }

    [Fact]
    public async Task LegacyFieldsNameTheReplacement()
    {
        var table = new CatalogTable
        {
            Name = "t",
            SchemaName = "main",
            Columns = Columns(new Field("id", Int64Type.Default, true)),
            PrimaryKeyColumns = ["id"],
        };
        var error = await Assert.ThrowsAnyAsync<Exception>(() => Serve(table));
        Assert.Contains("ConstraintRecord.Build()", error.Message);
    }
}
