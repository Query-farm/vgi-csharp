using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using Xunit;

namespace QueryFarm.Vgi.Tests.Catalog;

/// <summary>What every serve path hands the DuckDB extension: ATTACH-equivalent record checks,
/// nullability derived from the records, generated-column rules, and capability negotiation.</summary>
public class ServedTableTests
{
    private static readonly string[] Main = ["main"];

    private static Schema Columns(params Field[] fields) => new(fields, null);

    private static Field Int(string name, bool nullable = true) => new(name, Int64Type.Default, nullable);

    private static byte[] Offer(IEnumerable<string>? read = null, IEnumerable<string>? ddl = null, IEnumerable<string>? write = null) =>
        EmbeddedIpc.Encode(new ClientCapabilities
        {
            Engine = "test",
            Catalog = ConstraintRecord.EncodeCapabilities(read, ddl, write),
        });

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

    private static async Task<Schema> ServeOne(CatalogTable table, params CatalogTable[] others)
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(table);
        foreach (var other in others) registry.RegisterCatalogTable(other);
        var response = await new VgiServiceImpl(registry).CatalogTableGetAsync([], [.. table.EffectiveSchemaPath], table.Name, null, null, null);
        return SchemaIpc.ReadSchemaOnly(EmbeddedIpc.Decode<TableInfo>(Assert.Single(response.Items)).Columns);
    }

    private static Exception Rejects(Schema columns, params byte[][] constraints) =>
        Assert.Throws<InvalidOperationException>(() => ConstraintRecord.ValidateTable(Main, "t", columns, constraints));

    private sealed class Scan(Schema output) : ITableFunction
    {
        public string Name => "scan";

        public Schema ArgumentsSchema { get; } = new([], null);

        public Schema OutputSchema => output;

        public ITableFunctionProducer CreateProducer(TableInitParams initParams) => throw new NotSupportedException();
    }

    // ------------------------------------------------------------- nullability

    [Fact]
    public async Task FunctionBackedTableServesItsOutputSchemaNullable()
    {
        var served = await ServeOne(new CatalogTable
        {
            Name = "t",
            ScanFunction = new Scan(Columns(Int("n", nullable: false),
                new Field("s", new StructType([Int("x", nullable: false)]), nullable: false))),
        });
        Assert.True(served.GetFieldByName("n").IsNullable);
        Assert.True(served.GetFieldByName("s").IsNullable);
        Assert.True(((StructType)served.GetFieldByName("s").DataType).Fields[0].IsNullable);
    }

    [Fact]
    public async Task BuilderRecordsServeNamedFieldsNonNullable()
    {
        var table = new CatalogTable
        {
            Name = "t",
            Columns = Columns(Int("id"), Int("v"), new Field("s", new StructType([Int("x")]), true)),
            Constraints = ConstraintRecord.For(Main, "t").PrimaryKey("id").NotNull(["s", "x"]).Build(),
        };
        var served = await ServeOne(table);
        Assert.False(served.GetFieldByName("id").IsNullable);
        Assert.True(served.GetFieldByName("v").IsNullable);
        Assert.True(served.GetFieldByName("s").IsNullable);
        Assert.False(((StructType)served.GetFieldByName("s").DataType).Fields[0].IsNullable);
        var record = ConstraintRecord.Read(table.Constraints[0]);
        Assert.Equal(("t", "primary_key", ConstraintKind.PrimaryKey), (record.Table, record.Id, record.Kind));
        Assert.Equal(Main, record.SchemaPath);
    }

    [Fact]
    public async Task TimeTravelVersionsAreServedTheSameWay()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "t",
            Columns = Columns(Int("id")),
            SupportsTimeTravel = true,
            ResolveAtClause = (_, _) => new CatalogTable
            {
                Name = "t",
                Columns = Columns(Int("id", nullable: false), Int("old", nullable: false)),
                Constraints = ConstraintRecord.For(Main, "t").NotNull("id").Build(),
            },
        });
        var response = await new VgiServiceImpl(registry).CatalogTableGetAsync([], ["main"], "t", "VERSION", "1", null);
        var served = SchemaIpc.ReadSchemaOnly(EmbeddedIpc.Decode<TableInfo>(Assert.Single(response.Items)).Columns);
        Assert.False(served.GetFieldByName("id").IsNullable);
        Assert.True(served.GetFieldByName("old").IsNullable);
    }

    // ------------------------------------------------------------- ATTACH rules

    [Fact]
    public void RecordsOwnedByAnotherTableOrSchemaAreRejected()
    {
        var columns = Columns(Int("a"));
        Assert.Contains("belongs to main.other", Rejects(columns, ConstraintRecord.Build(Spec("other", "x", ConstraintKind.NotNull, ["a"]))).Message);
        var otherSchema = ConstraintRecord.Build(new ConstraintRecordSpec
        {
            SchemaPath = ["data"],
            Table = "t",
            Id = "x",
            Kind = ConstraintKind.NotNull,
            Columns = [["a"]],
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.AllRows,
        });
        var error = Rejects(columns, otherSchema);
        Assert.Contains("Table 'main.t': constraint 'x'", error.Message);
        Assert.Contains("belongs to data.t", error.Message);
    }

    [Fact]
    public void DuplicateIdsLabelsAndPrimaryKeysAreRejected()
    {
        var columns = Columns(Int("a"), Int("b"));
        var first = ConstraintRecord.Build(Spec("t", "x", ConstraintKind.NotNull, ["a"]));
        Assert.Contains("duplicate constraint id", Rejects(columns, first, ConstraintRecord.Build(Spec("t", "x", ConstraintKind.NotNull, ["b"]))).Message);

        var labelled = ConstraintRecord.Build(new ConstraintRecordSpec
        {
            SchemaPath = Main,
            Table = "t",
            Id = "u1",
            Kind = ConstraintKind.Unique,
            Columns = [["a"]],
            DeclaredName = "Key",
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.AllRows,
        });
        var relabelled = ConstraintRecord.Build(new ConstraintRecordSpec
        {
            SchemaPath = Main,
            Table = "t",
            Id = "u2",
            Kind = ConstraintKind.Unique,
            Columns = [["b"]],
            DeclaredName = "KEY",
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.AllRows,
        });
        Assert.Contains("duplicate declared name", Rejects(columns, labelled, relabelled).Message);

        Assert.Contains("only one PRIMARY_KEY", Rejects(columns,
            ConstraintRecord.Build(Spec("t", "p1", ConstraintKind.PrimaryKey, ["a"])),
            ConstraintRecord.Build(Spec("t", "p2", ConstraintKind.PrimaryKey, ["b"]))).Message);

        // The table-aware builder catches the same mistakes up front.
        Assert.Throws<ArgumentException>(() => ConstraintRecord.For(Main, "t").NotNull("a", id: "x").NotNull("b", id: "x"));
        Assert.Throws<ArgumentException>(() => ConstraintRecord.For(Main, "t").PrimaryKey("a").PrimaryKey(["b"], "other"));
        Assert.Throws<ArgumentException>(() => ConstraintRecord.For(Main, "t").Unique(["a"], "u1", "k").Unique(["b"], "u2", "K"));
    }

    [Fact]
    public void ColumnPathsMustResolve()
    {
        var columns = Columns(Int("a"), new Field("s", new StructType([Int("x")]), true));
        Assert.Contains("column path b", Rejects(columns, ConstraintRecord.Build(Spec("t", "x", ConstraintKind.NotNull, ["b"]))).Message);
        Assert.Contains("missing or ambiguous nested field", Rejects(columns, ConstraintRecord.Build(Spec("t", "x", ConstraintKind.NotNull, ["s", "y"]))).Message);
        Assert.Contains("only NOT_NULL may name nested fields", Rejects(columns, ConstraintRecord.Build(Spec("t", "x", ConstraintKind.Unique, ["s", "x"]))).Message);
        Assert.Contains("missing, ambiguous or row-id", Rejects(columns, ConstraintRecord.Build(Spec("t", "x", ConstraintKind.PrimaryKey, ["A"]))).Message);
    }

    private static byte[] Check(string sql) => ConstraintRecord.For(Main, "t").Check("c", sql).Build()[0];

    [Theory]
    [InlineData("budget >= 0")]
    [InlineData("(budget >= 0)")]
    [InlineData("BUDGET <> -1.5")]
    [InlineData("\"budget\" < ratio")]
    [InlineData("0 = small")]
    [InlineData("budget != 1e3")]
    public void ChecksInsideTheNumericGrammarAreServed(string sql) =>
        ConstraintRecord.ValidateTable(Main, "t", Columns(Int("budget"), new Field("ratio", DoubleType.Default, true),
            new Field("small", Int16Type.Default, true), new Field("label", StringType.Default, true)), [Check(sql)]);

    [Theory]
    [InlineData("budget >= 0 AND budget < 10", "must be one numeric comparison")]
    [InlineData("budget", "must be one numeric comparison")]
    [InlineData("label = 'x'", "signed integer or floating-point column")]
    [InlineData("budget = 'x'", "operands must be")]
    [InlineData("abs(budget) > 0", "operands must be")]
    [InlineData("budget + 1 > 0", "operands must be")]
    [InlineData("t.budget > 0", "operands must be")]
    [InlineData("missing > 0", "missing or row-id column")]
    [InlineData("budget IN (1, 2)", "outside the supported grammar")]
    [InlineData("budget > 0 -- comment", "outside the supported grammar")]
    [InlineData("budget IS NOT NULL", "must be one numeric comparison")]
    public void ChecksOutsideTheNumericGrammarAreRejected(string sql, string message)
    {
        var error = Rejects(Columns(Int("budget"), new Field("label", StringType.Default, true)), Check(sql));
        Assert.Contains("constraint 'c'", error.Message);
        Assert.Contains(message, error.Message);
    }

    private static CatalogTable Parent(params byte[][] constraints) => new()
    {
        Name = "parent",
        Columns = Columns(Int("id"), Int("code")),
        Constraints = constraints.Length > 0 ? constraints : ConstraintRecord.For(Main, "parent").PrimaryKey("id").Build(),
    };

    private static CatalogTable Child(TableConstraints records) => new()
    {
        Name = "child",
        Columns = Columns(Int("parent_id")),
        Constraints = records.Build(),
    };

    [Fact]
    public async Task ForeignKeysAreCheckedAgainstTheCatalog()
    {
        var good = Child(ConstraintRecord.For(Main, "child").ForeignKey(["parent_id"], "parent", ["id"]));
        Assert.True((await ServeOne(good, Parent())).GetFieldByName("parent_id").IsNullable);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => ServeOne(good));
        Assert.Contains("which this catalog does not serve", error.Message);
        error = await Assert.ThrowsAsync<InvalidOperationException>(() => ServeOne(
            Child(ConstraintRecord.For(Main, "child").ForeignKey(["parent_id"], "parent", ["id"], "missing")), Parent()));
        Assert.Contains("constraint 'missing', which table parent does not carry", error.Message);
        error = await Assert.ThrowsAsync<InvalidOperationException>(() => ServeOne(
            Child(ConstraintRecord.For(Main, "child").ForeignKey(["parent_id"], "parent", ["code"])), Parent()));
        Assert.Contains("differ from parent's key 'primary_key'", error.Message);
        error = await Assert.ThrowsAsync<InvalidOperationException>(() => ServeOne(
            Child(ConstraintRecord.For(Main, "child").ForeignKey(["parent_id"], "parent", ["id"], "nn")),
            Parent(ConstraintRecord.For(Main, "parent").NotNull("id", id: "nn").Build()[0])));
        Assert.Contains("not PRIMARY_KEY/UNIQUE", error.Message);
    }

    [Fact]
    public void ForeignKeysTheExtensionRefusesAreRejected()
    {
        var columns = Columns(Int("parent_id"));
        var crossSchema = ConstraintRecord.Build(new ConstraintRecordSpec
        {
            SchemaPath = Main,
            Table = "t",
            Id = "fk",
            Kind = ConstraintKind.ForeignKey,
            Columns = [["parent_id"]],
            Enforcement = ConstraintEnforcement.Enforced,
            Validation = ConstraintValidation.AllRows,
            References = new ConstraintReference { SchemaPath = ["other"], Table = "parent", Columns = [["id"]], ConstraintId = "pk" },
        });
        Assert.Contains("in schema main", Rejects(columns, crossSchema).Message);

        // A CASCADE action is representable on the wire but refused by the extension.
        var record = ConstraintRecord.For(Main, "t").ForeignKey(["parent_id"], "t", ["parent_id"], "fk").Build()[0];
        var info = EmbeddedIpc.Decode<ConstraintInfo>(record);
        var definition = EmbeddedIpc.Decode<ConstraintDefinition>(info.Definition);
        var fk = EmbeddedIpc.Decode<ForeignKeyDefinition>(definition.ForeignKey!);
        fk.OnDelete = ReferentialAction.Cascade;
        definition.ForeignKey = EmbeddedIpc.Encode(fk);
        info.Definition = EmbeddedIpc.Encode(definition);
        Assert.Contains("only NO ACTION or RESTRICT", Rejects(columns, EmbeddedIpc.Encode(info)).Message);
    }

    // ------------------------------------------------------------- generated columns

    private static CatalogTable Generated(Schema columns, IReadOnlyDictionary<string, string> generated, ITableFunction? scan = null) => new()
    {
        Name = "t",
        Columns = columns,
        GeneratedColumns = generated,
        ScanFunction = scan,
    };

    [Theory]
    [InlineData("n * 2")]
    [InlineData("'item_' || CAST(n AS VARCHAR)")]
    [InlineData("coalesce(n, 0) > 1 AND NOT (n IS NULL)")]
    [InlineData("abs(-n) % 3 = 0 OR upper(label) <> 'X'")]
    [InlineData("n::VARCHAR")]
    public async Task GeneratedExpressionsInsideTheGrammarAreServed(string sql) =>
        await ServeOne(Generated(Columns(Int("n"), new Field("label", StringType.Default, true), Int("g")),
            new Dictionary<string, string> { ["g"] = sql }));

    [Theory]
    [InlineData("n / 2", "function / is unsupported")]
    [InlineData("sqrt(n)", "function sqrt is unsupported")]
    [InlineData("TRY_CAST(n AS BIGINT)", "TRY_CAST is unsupported")]
    [InlineData("n + 1.5", "literals must be")]
    [InlineData("CAST(n AS DOUBLE)", "CAST to DOUBLE is unsupported")]
    [InlineData("ratio + 1", "operand ratio must be")]
    [InlineData("missing + 1", "unknown column missing")]
    [InlineData("CASE WHEN n > 0 THEN 1 ELSE 0 END", "outside the supported grammar")]
    public async Task GeneratedExpressionsOutsideTheGrammarAreRejected(string sql, string message)
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => ServeOne(Generated(
            Columns(Int("n"), new Field("ratio", DoubleType.Default, true), Int("g")), new Dictionary<string, string> { ["g"] = sql })));
        Assert.Contains("Generated column 'g'", error.Message);
        Assert.Contains(message, error.Message);
    }

    [Fact]
    public async Task GeneratedColumnTypesAndNullabilityFollowTheExtension()
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => ServeOne(Generated(
            Columns(Int("n"), new Field("g", DoubleType.Default, true)), new Dictionary<string, string> { ["g"] = "n" })));
        Assert.Contains("generated columns must be BOOLEAN", error.Message);

        // A non-nullable generated field without a record is simply served nullable...
        var served = await ServeOne(Generated(Columns(Int("n"), Int("g", nullable: false)), new Dictionary<string, string> { ["g"] = "n" }));
        Assert.True(served.GetFieldByName("g").IsNullable);
        // ...but a NOT NULL record on one is refused: DuckDB computes it, so it must stay nullable.
        error = await Assert.ThrowsAsync<InvalidOperationException>(() => ServeOne(new CatalogTable
        {
            Name = "t",
            Columns = Columns(Int("n"), Int("g")),
            GeneratedColumns = new Dictionary<string, string> { ["g"] = "n" },
            Constraints = ConstraintRecord.For(Main, "t").NotNull("g").Build(),
        }));
        Assert.Contains("column g is a VIRTUAL generated column", error.Message);
    }

    [Fact]
    public async Task ScanEmittingTheFullDeclaredWidthFailsClearly()
    {
        var full = Columns(Int("n"), Int("doubled"));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => ServeOne(Generated(full,
            new Dictionary<string, string> { ["doubled"] = "n * 2" }, new Scan(full))));
        Assert.Contains("generated column(s) doubled are VIRTUAL", error.Message);
        Assert.Contains("scan function 'scan' also outputs them", error.Message);
        Assert.Contains("only the physical columns (n)", error.Message);

        // Columns defaulting to the scan's own output is the same mistake.
        error = await Assert.ThrowsAsync<InvalidOperationException>(() => ServeOne(new CatalogTable
        {
            Name = "t",
            ScanFunction = new Scan(full),
            GeneratedColumns = new Dictionary<string, string> { ["doubled"] = "n * 2" },
        }));
        Assert.Contains("scan function 'scan' also outputs them", error.Message);

        await ServeOne(Generated(full, new Dictionary<string, string> { ["doubled"] = "n * 2" }, new Scan(Columns(Int("n")))));
    }

    [Fact]
    public async Task LegacyGeneratedMetadataIsTranslated()
    {
#pragma warning disable CS0618 // the legacy key under test
        var legacyKey = VgiWireMetadata.GeneratedExpressionKey;
#pragma warning restore CS0618
        Assert.Equal("generated_expression", legacyKey);
        var raw = Columns(Int("n"), new Field("g", Int64Type.Default, true, new Dictionary<string, string> { [legacyKey] = "n * 2" }));
        var served = await ServeOne(new CatalogTable { Name = "t", Columns = raw });
        var metadata = served.GetFieldByName("g").Metadata;
        Assert.False(metadata.ContainsKey(legacyKey));
        Assert.Equal("virtual", metadata[ColumnExpressions.GenerationKey]);
        Assert.Equal("n * 2", EmbeddedIpc.Decode<SqlExpression>(Convert.FromBase64String(metadata[ColumnExpressions.GeneratedKey])).Sql);

        // Canonical base64 written under the old key (0.22's value of the constant) moves as is.
        var canonical = ColumnExpressions.GeneratedMetadata("n + 1", Main);
        served = await ServeOne(new CatalogTable
        {
            Name = "t",
            Columns = Columns(Int("n"), new Field("g", Int64Type.Default, true, new Dictionary<string, string> { [legacyKey] = canonical })),
        });
        Assert.Equal(canonical, served.GetFieldByName("g").Metadata[ColumnExpressions.GeneratedKey]);

        // Ambiguous: legacy and canonical metadata together.
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => ServeOne(new CatalogTable
        {
            Name = "t",
            Columns = Columns(Int("n"), new Field("g", Int64Type.Default, true, new Dictionary<string, string>
            {
                [legacyKey] = "n * 2",
                [ColumnExpressions.GeneratedKey] = canonical,
                [ColumnExpressions.GenerationKey] = "virtual",
            })),
        }));
        Assert.Contains("keep only the canonical keys", error.Message);
    }

    // ------------------------------------------------------------- catalog_contents

    private static byte[] RawTable(string name, Schema columns, IReadOnlyList<byte[]> constraints) => EmbeddedIpc.Encode(new TableInfo
    {
        Name = name,
        SchemaPath = ["main"],
        Columns = SchemaIpc.WriteSchemaOnly(columns),
        Constraints = [.. constraints],
    });

    private static async Task<List<TableInfo>> ContentsTables(params byte[][] tables)
    {
        var registry = new CatalogRegistry();
        registry.RegisterSchema("main");
        registry.SetCatalogContentsHandler("c", _ => Task.FromResult(new CatalogContentsResult
        {
            Schemas =
            [
                new SchemaContents
                {
                    Path = ["main"],
                    Schema = EmbeddedIpc.Encode(new SchemaInfo { Path = ["main"] }),
                    Tables = [.. tables],
                },
            ],
        }));
        IVgiService service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c" })).AttachOpaqueData;
        var response = await service.CatalogContentsAsync(attach);
        return MetadataCollections.Decode<TableInfo>(response.Tables);
    }

    [Fact]
    public async Task HandlerServedRawTablesAreNormalized()
    {
        var tables = await ContentsTables(
            RawTable("t", Columns(Int("id"), Int("v", nullable: false),
                new Field("g", Int64Type.Default, true, new Dictionary<string, string> { [ColumnExpressions.LegacyGeneratedKey] = "v + 1" })),
                ConstraintRecord.For(Main, "t").PrimaryKey("id").Build()));
        var served = SchemaIpc.ReadSchemaOnly(Assert.Single(tables).Columns);
        Assert.False(served.GetFieldByName("id").IsNullable);
        Assert.True(served.GetFieldByName("v").IsNullable);
        Assert.True(served.GetFieldByName("g").Metadata.ContainsKey(ColumnExpressions.GeneratedKey));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => ContentsTables(
            RawTable("child", Columns(Int("parent_id")),
                ConstraintRecord.For(Main, "child").ForeignKey(["parent_id"], "parent", ["id"]).Build())));
        Assert.Contains("which this catalog does not serve", error.Message);
        Assert.Equal(2, (await ContentsTables(
            RawTable("child", Columns(Int("parent_id")), ConstraintRecord.For(Main, "child").ForeignKey(["parent_id"], "parent", ["id"]).Build()),
            RawTable("parent", Columns(Int("id")), ConstraintRecord.For(Main, "parent").PrimaryKey("id").Build()))).Count);
    }

    // ------------------------------------------------------------- negotiation

    private static readonly string[] BothRead = [ConstraintProfiles.RecordsRead, ConstraintProfiles.ScalarExpression];

    [Fact]
    public async Task RecordsTheRegistryCannotSeeAreNegotiatedOnRequest()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable { Name = "t", Columns = Columns(Int("id")) });
        var service = new VgiServiceImpl(registry);
        var plain = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c", ClientCapabilities = Offer(BothRead) });
        Assert.Null(plain.SelectedCapabilities);

        registry.OnAttach = _ => new AttachContext { ServesConstraintRecords = true };
        var perAttach = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c", ClientCapabilities = Offer(BothRead) });
        Assert.Equal(BothRead, ConstraintRecord.DecodeCapabilities(perAttach.SelectedCapabilities).Read);

        registry.OnAttach = null;
        registry.AdvertiseConstraintRecords();
        var perIdentity = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c", ClientCapabilities = Offer(BothRead) });
        Assert.Equal(BothRead, ConstraintRecord.DecodeCapabilities(perIdentity.SelectedCapabilities).Read);
    }

    [Fact]
    public async Task HandlerSelectedReadProfilesFailTheAttach()
    {
        var registry = new CatalogRegistry();
        registry.OnAttach = _ => new AttachContext
        {
            SelectedCapabilities = ConstraintRecord.EncodeCapabilities(read: [ConstraintProfiles.RecordsRead]),
        };
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            new VgiServiceImpl(registry).CatalogAttachAsync(new CatalogAttachRequest { Name = "c" }));
        Assert.Contains("SelectedCapabilities.Read must be empty", error.Message);
    }

    [Fact]
    public void SelectProfilesDropsProfilesWithoutTheirPrerequisite()
    {
        var advertised = new CatalogCapabilities
        {
            Read = [.. BothRead],
            Ddl = [ConstraintProfiles.Create],
            Write = [ConstraintProfiles.Enforce],
        };
        var all = ConstraintRecord.SelectProfiles(advertised, Offer(BothRead, [ConstraintProfiles.Create], [ConstraintProfiles.Enforce]));
        Assert.Equal(BothRead, all.Read);
        Assert.Equal([ConstraintProfiles.Create], all.Ddl);
        Assert.Equal([ConstraintProfiles.Enforce], all.Write);
        var noRead = ConstraintRecord.SelectProfiles(advertised, Offer([ConstraintProfiles.ScalarExpression], [ConstraintProfiles.Create], [ConstraintProfiles.Enforce]));
        Assert.Empty(noRead.Read);
        Assert.Empty(noRead.Ddl);
        Assert.Empty(noRead.Write);
    }

    // ------------------------------------------------------------- in-memory CREATE TABLE

    [Fact]
    public async Task InMemoryCreateTableAcceptsNotNull()
    {
        var registry = new CatalogRegistry();
        registry.RegisterInMemoryCatalog("mem");
        IVgiService service = new VgiServiceImpl(registry);
        var attach = await service.CatalogAttachAsync(new CatalogAttachRequest
        {
            Name = "mem",
            ClientCapabilities = Offer(BothRead, [ConstraintProfiles.Create, ConstraintProfiles.Mutate], [ConstraintProfiles.Enforce]),
        });
        var selected = ConstraintRecord.DecodeCapabilities(attach.SelectedCapabilities);
        Assert.Equal(BothRead, selected.Read);
        Assert.Equal([ConstraintProfiles.Create], selected.Ddl);
        Assert.Equal([ConstraintProfiles.Enforce], selected.Write);

        var extension = new Field("geom", new StructType([Int("x", nullable: false)]), true,
            new Dictionary<string, string> { ["ARROW:extension:name"] = "example.point" });
        await service.CatalogTableCreateAsync(new TableCreateRequest
        {
            AttachOpaqueData = attach.AttachOpaqueData,
            SchemaPath = ["main"],
            Name = "t",
            Columns = SchemaIpc.WriteSchemaOnly(Columns(Int("id", nullable: false), Int("v", nullable: false), extension)),
            ConstraintDefinitions =
            [
                EmbeddedIpc.Encode(new ConstraintDefinition { Kind = ConstraintKind.NotNull, ColumnPaths = [["id"]], DeclaredName = "id_required" }),
            ],
        });
        var info = EmbeddedIpc.Decode<TableInfo>(Assert.Single(
            (await service.CatalogTableGetAsync(attach.AttachOpaqueData, ["main"], "t", null, null, null)).Items));
        var record = ConstraintRecord.Read(Assert.Single(info.Constraints));
        Assert.Equal((ConstraintKind.NotNull, "t", "id_required"), (record.Kind, record.Table, record.DeclaredName));
        Assert.Equal((ConstraintEnforcement.Enforced, ConstraintValidation.AllRows), (record.Enforcement, record.Validation));
        var served = SchemaIpc.ReadSchemaOnly(info.Columns);
        Assert.False(served.GetFieldByName("id").IsNullable);
        Assert.True(served.GetFieldByName("v").IsNullable);
        Assert.False(((StructType)served.GetFieldByName("geom").DataType).Fields[0].IsNullable);

        var check = new TableCreateRequest
        {
            AttachOpaqueData = attach.AttachOpaqueData,
            SchemaPath = ["main"],
            Name = "u",
            Columns = SchemaIpc.WriteSchemaOnly(Columns(Int("id"))),
            ConstraintDefinitions = [EmbeddedIpc.Encode(new ConstraintDefinition { Kind = ConstraintKind.Check, CheckExpression = [1] })],
        };
        var error = await Assert.ThrowsAsync<MethodNotImplementedError>(() => service.CatalogTableCreateAsync(check));
        Assert.Contains("enforce only NOT NULL", error.Message);
    }

    // ------------------------------------------------------------- registration

    [Fact]
    public void MacrosAreValidatedAtRegistration()
    {
        var registry = new CatalogRegistry();
        var error = Assert.Throws<ArgumentException>(() => registry.RegisterMacro(new CatalogMacro { Name = "m", MacroType = MacroType.Scalar }));
        Assert.Contains("Macro 'main.m'", error.Message);
        Assert.Contains("nonempty SQL definition", error.Message);

        var defaults = new RecordBatch(new Schema([Int("a")], null), [new Int64Array.Builder().Append(1).Build()], 1);
        error = Assert.Throws<ArgumentException>(() => registry.RegisterMacro(new CatalogMacro
        {
            Name = "m",
            MacroType = MacroType.Scalar,
            Definition = "a + b",
            Parameters = ["a", "b"],
            ParameterDefaults = defaults,
        }));
        Assert.Contains("cannot follow a default", error.Message);
    }

    [Fact]
    public void MultiComponentDefaultSchemaFailsAtRegistration()
    {
#pragma warning disable CS0618 // the obsolete overload under test
        var error = Assert.Throws<ArgumentException>(() => new Worker().DefaultSchema(["a", "b"]));
        new Worker().DefaultSchema(["a"]);
#pragma warning restore CS0618
        Assert.Contains("single root schema identifier", error.Message);
        Assert.Contains("DefaultSchema(\"a\")", error.Message);
    }
}
