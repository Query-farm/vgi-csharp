// Copyright 2025, 2026 Query Farm LLC - https://query.farm

using System.Text.Json;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Reflection;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

public class CatalogContractsTests
{
    [Fact]
    public void TypedIndexesPreserveRawPathsContextAndOptions()
    {
        Assert.Equal(["a.b"], RoundTrip<IndexKey>("index_key_column").ColumnPath);
        Assert.Equal(["record", "a.b"], RoundTrip<IndexKey>("index_key_nested").ColumnPath);
        Assert.NotNull(RoundTrip<IndexKey>("index_key_expression").Expression);
        var definition = RoundTrip<IndexDefinition>("index_definition");
        RoundTrip<IndexDefinition>("index_definition_primary");
        Assert.Equal(Fixture("index_key_column"), definition.Keys[0]);
        Assert.Equal(Fixture("index_key_expression"), definition.Keys[1]);
        Assert.Equal(Fixture("values"), definition.Options);
        var info = RoundTrip<IndexInfo>("index_info");
        Assert.Equal(Fixture("index_definition"), info.Definition);
        Assert.Equal(Fixture("state"), info.Options);
        var create = RoundTrip<IndexCreateRequest>("index_create");
        Assert.Equal(info.Definition, create.Definition);
        Assert.Equal(new byte[] {116, 120, 110, 0, 255}, create.TransactionOpaqueData);
        Assert.Equal(["indexes"], RoundTrip<IndexCreateRequest>("index_create_cross_schema").SchemaPath);
    }

    [Fact]
    public void ColumnExpressionMetadataRetainsExactIpcBytes()
    {
        using var reader = new ArrowStreamReader(new MemoryStream(Fixture("column_expression_metadata")));
        using var source = reader.ReadNextRecordBatch();
        Assert.NotNull(source);
        using var encoded = new MemoryStream();
        using (var writer = new ArrowStreamWriter(encoded, source.Schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(source);
            writer.WriteEnd();
        }
        encoded.Position = 0;
        using var restoredReader = new ArrowStreamReader(encoded);
        using var restored = restoredReader.ReadNextRecordBatch();
        Assert.NotNull(restored);
        Assert.Equal(Fixture("column_default_expression"), Convert.FromBase64String(restored.Schema.GetFieldByIndex(0).Metadata["vgi.column.default.v1"]));
        Assert.Equal(Fixture("column_generated_expression"), Convert.FromBase64String(restored.Schema.GetFieldByIndex(1).Metadata["vgi.column.generated.v1"]));
        Assert.Equal("virtual", restored.Schema.GetFieldByIndex(1).Metadata["vgi.column.generation.v1"]);
    }

    [Fact]
    public void CanonicalConstraintRecordsPreserveNestedPayloadsAndRevisions()
    {
        RoundTrip<SqlStatement>("sql_statement");
        RoundTrip<CatalogObjectRef>("constraint_ref");
        RoundTrip<ForeignKeyDefinition>("foreign_key_definition");
        RoundTrip<ConstraintDefinition>("constraint_definition_not_null");
        RoundTrip<ConstraintDefinition>("constraint_definition_unique");
        RoundTrip<ConstraintDefinition>("constraint_definition_primary");
        RoundTrip<ConstraintDefinition>("constraint_definition_check");
        RoundTrip<ConstraintDefinition>("constraint_definition_foreign");
        RoundTrip<ConstraintInfo>("constraint_info");
        RoundTrip<ConstraintInfo>("constraint_info_untrusted");
        RoundTrip<ConstraintChange>("constraint_change_add");
        RoundTrip<ConstraintChange>("constraint_change_drop");
        RoundTrip<ConstraintChange>("constraint_change_rename");
        RoundTrip<ConstraintChange>("constraint_change_unname");
        RoundTrip<ConstraintChange>("constraint_change_replace");
        RoundTrip<ConstraintSnapshot>("constraint_snapshot");
        RoundTrip<ConstraintSnapshot>("constraint_snapshot_empty");
        RoundTrip<ConstraintsApplyRequest>("constraints_apply");
        RoundTrip<ConstraintsApplyRequest>("constraints_apply_autocommit");
        var statement = RoundTrip<SqlStatement>("sql_statement");
        Assert.Equal("desc", statement.EvaluationContext.DefaultOrder);
        Assert.Equal("nulls_first", statement.EvaluationContext.DefaultNullOrder);
        Assert.False(statement.EvaluationContext.PreserveInsertionOrder);
        var snapshot = RoundTrip<ConstraintSnapshot>("constraint_snapshot");
        Assert.Equal(new byte[] { 0, 114, 101, 118, 105, 115, 105, 111, 110, 255, 128 }, snapshot.DefinitionRevision);
        Assert.Single(snapshot.Constraints);
        Assert.Equal(Fixture("constraint_info"), snapshot.Constraints[0]);
        var info = RoundTrip<ConstraintInfo>("constraint_info");
        Assert.Equal(Fixture("constraint_ref"), info.Ref);
        Assert.Equal(Fixture("constraint_definition_foreign"), info.Definition);
        var request = RoundTrip<ConstraintsApplyRequest>("constraints_apply");
        Assert.Equal(snapshot.DefinitionRevision, request.ExpectedRevision);
        Assert.Equal(Fixture("constraint_change_add"), request.Changes[0]);
        Assert.Equal(Fixture("constraint_change_rename"), request.Changes[1]);
        Assert.Null(RoundTrip<ConstraintsApplyRequest>("constraints_apply_autocommit").TransactionOpaqueData);
        Assert.Null(RoundTrip<ConstraintChange>("constraint_change_unname").DeclaredName);
    }

    [Fact]
    public void MacroFamiliesPreserveDeclarationsDefaultsAndLegacyAbsence()
    {
        var source = EmbeddedIpc.Decode<MacroParameter>(Fixture("macro_parameter"));
        var parameter = RoundTrip<MacroParameter>("macro_parameter");
        Assert.Equal("fallback", parameter.Name);
        Assert.Equal(source.DeclaredType, parameter.DeclaredType);
        Assert.Equal(source.DefaultValue, parameter.DefaultValue);
        Assert.Null(parameter.DefaultExpression);
        Assert.Equal("A typed NULL default", parameter.Documentation);
        var expression = RoundTrip<MacroParameter>("macro_parameter_expression");
        Assert.Null(expression.DefaultValue);
        Assert.Equal("2 + 3", EmbeddedIpc.Decode<SqlExpression>(expression.DefaultExpression!).Sql);
        var untyped = RoundTrip<MacroParameter>("macro_parameter_untyped");
        Assert.Null(untyped.DeclaredType);
        Assert.Null(untyped.DefaultValue);
        var overload = RoundTrip<MacroOverload>("macro_overload");
        Assert.Equal(["one.two", "nested"], overload.SchemaPath);
        Assert.Equal(["x", "fallback", "scale"], overload.Parameters.Select(p => p.Name));
        var create = RoundTrip<MacroCreateRequest>("macro_create");
        Assert.Equal(2, create.Overloads!.Count);
        Assert.Equal("upper(x)", create.Overloads[1].Definition);
        Assert.Empty(create.Parameters);
        Assert.Equal("", create.Definition);
        Assert.Null(RoundTrip<MacroCreateRequest>("macro_create_legacy").Overloads);
        Assert.Equal(2, RoundTrip<MacroInfo>("macro_info").Overloads!.Count);
        Assert.Null(RoundTrip<MacroInfo>("macro_info_legacy").Overloads);
        using var reader = new ArrowStreamReader(new MemoryStream(parameter.DefaultValue!));
        using var value = reader.ReadNextRecordBatch();
        Assert.NotNull(value);
        Assert.True(value.Column(0).IsNull(0));
        Assert.Equal("retained", value.Schema.GetFieldByIndex(0).Metadata["domain"]);
    }

    [Theory]
    [InlineData("logical_tuple", "tuple")]
    [InlineData("logical_struct", null)]
    [InlineData("logical_variant_sql_null", "arrow.parquet.variant")]
    [InlineData("logical_variant_null", "arrow.parquet.variant")]
    [InlineData("logical_variant_absent", "arrow.parquet.variant")]
    [InlineData("logical_variant_null_member", "arrow.parquet.variant")]
    public void LogicalCarriersPreserveRawTypeMetadataAndDynamicPayloadBytes(string name, string? marker)
    {
        using var reader = new ArrowStreamReader(new MemoryStream(Fixture(name)));
        using var original = reader.ReadNextRecordBatch();
        Assert.NotNull(original);
        using var encoded = new MemoryStream();
        using (var writer = new ArrowStreamWriter(encoded, original.Schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(original);
            writer.WriteEnd();
        }
        encoded.Position = 0;
        using var restoredReader = new ArrowStreamReader(encoded);
        using var restored = restoredReader.ReadNextRecordBatch();
        Assert.NotNull(restored);
        Assert.Equal(1, restored.Length);
        var field = restored.Schema.GetFieldByIndex(0);
        if (marker == "tuple") Assert.Equal(marker, field.Metadata["vgi.logical_type.v1"]);
        else Assert.False(field.Metadata?.ContainsKey("vgi.logical_type.v1") ?? false);
        Assert.Equal(name == "logical_variant_sql_null", restored.Column(0).IsNull(0));
        if (name.StartsWith("logical_variant_", StringComparison.Ordinal))
        {
            Assert.Equal(marker, field.Metadata["ARROW:extension:name"]);
            Assert.Equal("", field.Metadata["ARROW:extension:metadata"]);
            var storage = Assert.IsType<StructType>(field.DataType);
            Assert.Equal(["metadata", "value"], storage.Fields.Select(f => f.Name));
            Assert.False(storage.Fields[0].IsNullable);
            Assert.True(storage.Fields[1].IsNullable);
            if (name != "logical_variant_sql_null")
            {
                var variant = Assert.IsType<StructArray>(restored.Column(0));
                byte[] metadata = name == "logical_variant_null_member"
                    ? [1, 1, 0, 7, 112, 114, 101, 115, 101, 110, 116] : [1, 0, 0];
                byte[] payload = name == "logical_variant_null" ? [0]
                    : name == "logical_variant_absent" ? [2, 0, 0] : [2, 1, 0, 0, 1, 0];
                Assert.Equal(metadata, Assert.IsType<BinaryArray>(variant.Fields[0]).GetBytes(0).ToArray());
                Assert.Equal(payload, Assert.IsType<BinaryArray>(variant.Fields[1]).GetBytes(0).ToArray());
            }
        }
        else
        {
            var tuple = Assert.IsType<StructArray>(restored.Column(0));
            Assert.Equal(ulong.MaxValue, Assert.IsType<UInt64Array>(tuple.Fields[0]).GetValue(0));
            Assert.True(tuple.Fields[1].IsNull(0));
            Assert.Equal(["element1", "element2"], Assert.IsType<StructType>(field.DataType).Fields.Select(f => f.Name));
        }
        Assert.Null(reader.ReadNextRecordBatch());
        Assert.Null(restoredReader.ReadNextRecordBatch());
    }

    [Theory]
    [InlineData("query_parameters", "named")]
    [InlineData("query_empty_parameters", "positional")]
    public void QueryParameterBatchesPreserveMappingMetadataAndEmptyRows(string name, string mode)
    {
        using var input = new MemoryStream(Fixture(name));
        using var reader = new ArrowStreamReader(input);
        using var original = reader.ReadNextRecordBatch();
        Assert.NotNull(original);
        using var encoded = new MemoryStream();
        using (var writer = new ArrowStreamWriter(encoded, original.Schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(original);
            writer.WriteEnd();
        }
        encoded.Position = 0;
        using var restoredReader = new ArrowStreamReader(encoded);
        using var restored = restoredReader.ReadNextRecordBatch();
        Assert.NotNull(restored);
        Assert.Equal(1, restored.Length);
        Assert.Equal(mode, restored.Schema.Metadata["vgi.query.parameters.v1"]);
        if (name == "query_empty_parameters") Assert.Empty(restored.Schema.FieldsList);
        else
        {
            Assert.True(restored.Column(0).IsNull(0));
            Assert.IsType<Int32Type>(restored.Schema.GetFieldByIndex(0).DataType);
            Assert.Equal("example.optional", restored.Schema.GetFieldByIndex(0).Metadata["logical"]);
            var type = Assert.IsType<StructType>(restored.Schema.GetFieldByIndex(1).DataType);
            Assert.Equal("retained", type.Fields[0].Metadata["nested"]);
            Assert.False(type.Fields[0].IsNullable);
        }
        Assert.Null(reader.ReadNextRecordBatch());
        Assert.Null(restoredReader.ReadNextRecordBatch());
    }

    [Fact]
    public void QueryRecordsPreserveNestedContracts()
    {
        RoundTrip<ClientQueryCapabilities>("query_offer");
        RoundTrip<QueryCapabilities>("query_capabilities");
        RoundTrip<QueryEvaluationContext>("query_context");
        RoundTrip<QueryObjectReference>("query_object_reference");
        RoundTrip<CatalogQueryPrepareRequest>("query_prepare");
        RoundTrip<CatalogQueryPrepareResult>("query_accepted");
        RoundTrip<CatalogQueryPrepareResult>("query_declined");
        var request = RoundTrip<CatalogQueryPrepareRequest>("query_prepare");
        Assert.Equal("UTC", request.EvaluationContext.Timezone);
        Assert.Equal(["one.two", "nested"], request.ReferencedObjects[0].Object.SchemaPath);
        Assert.Equal(0L, request.CatalogVersion);
        Assert.Null(RoundTrip<CatalogQueryPrepareResult>("query_declined").PreparedQueryToken);
    }

    [Fact]
    public void DependenciesPreserveRawIdentitiesAndNestedPayloads()
    {
        var table = RoundTrip<CatalogObjectRef>("object_ref");
        Assert.Equal(["one.two", "nested"], table.SchemaPath);
        Assert.Null(table.OwnerTable);
        var trigger = RoundTrip<CatalogObjectRef>("trigger_ref");
        Assert.Equal(table.Name, trigger.OwnerTable);
        var edge = RoundTrip<ObjectDependency>("object_dependency");
        Assert.Equal("owned", edge.Kind);
        Assert.True(edge.BlocksAlter);
        Assert.Null(edge.SubjectColumnPaths);
        Assert.Null(edge.DependentColumnPath);
        var membership = RoundTrip<ObjectDependency>("dependency_membership");
        Assert.False(membership.BlocksAlter);
        Assert.Null(membership.SubjectColumnPaths);
        Assert.Null(membership.DependentColumnPath);
        var literal = RoundTrip<ObjectDependency>("dependency_column_literal");
        Assert.True(literal.BlocksAlter);
        Assert.Equal(["a.b"], Assert.Single(literal.SubjectColumnPaths!));
        Assert.Null(literal.DependentColumnPath);
        var nested = RoundTrip<ObjectDependency>("dependency_column_nested");
        Assert.Equal(["record", "a.b"], Assert.Single(nested.SubjectColumnPaths!));
        var generated = RoundTrip<ObjectDependency>("dependency_generated");
        Assert.Equal(generated.Dependent, generated.Subject);
        Assert.Equal(["generated"], generated.DependentColumnPath);
        Assert.Equal(["a.b"], generated.SubjectColumnPaths![0]);
        Assert.Equal(["record", "a.b"], generated.SubjectColumnPaths[1]);
        var chain = RoundTrip<ObjectDependency>("dependency_generated_chain");
        Assert.Equal(["second.generated"], chain.DependentColumnPath);
        Assert.Equal(["generated"], Assert.Single(chain.SubjectColumnPaths!));
        var columns = RoundTrip<DependencySnapshot>("dependency_column_snapshot");
        Assert.Equal(3, columns.Dependencies.Count);
        Assert.Equal(Fixture("dependency_column_literal"), columns.Dependencies[0]);
        Assert.Equal(Fixture("dependency_generated"), columns.Dependencies[1]);
        Assert.Equal(Fixture("dependency_generated_chain"), columns.Dependencies[2]);

        var graph = RoundTrip<DependencySnapshot>("dependency_snapshot");
        Assert.Equal(42L, graph.CatalogVersion);
        Assert.Equal(4, graph.Objects.Count);
        Assert.Equal(3, graph.Dependencies.Count);
        var empty = RoundTrip<DependencySnapshot>("empty_dependency_snapshot");
        Assert.Empty(empty.Objects);
        Assert.Empty(empty.Dependencies);
    }

    [Fact]
    public void MetadataPatchesPreserveCommentPresenceAndLiteralTags()
    {
        var patch = RoundTrip<CatalogMetadataChanges>("metadata_patch");
        Assert.True(patch.SetComment);
        Assert.Equal("", patch.Comment);
        Assert.Equal("", patch.SetTags["a.b"]);
        Assert.Equal("literal", patch.SetTags["__proto__"]);
        Assert.Equal("empty-key", patch.SetTags[""]);
        Assert.Equal(["old", "OLD"], patch.RemoveTags);
        var clear = RoundTrip<CatalogMetadataChanges>("metadata_clear_comment");
        Assert.True(clear.SetComment);
        Assert.Null(clear.Comment);
        Assert.Empty(clear.SetTags);
        var noop = RoundTrip<CatalogMetadataChanges>("metadata_noop");
        Assert.False(noop.SetComment);
        Assert.Null(noop.Comment);
        Assert.Empty(noop.SetTags);
        Assert.Empty(noop.RemoveTags);
    }

    [Fact]
    public void ViewSecuritySurvivesCodecRoundTrip()
    {
        Assert.Equal("regular", RoundTrip<ViewInfo>("view_info").Security);
        Assert.Equal("barrier", RoundTrip<ViewInfo>("view_info_barrier").Security);
    }

    [Fact]
    public void ViewAliasesPreserveRawIdentifiers()
    {
        Assert.Equal(["a.b", "with\"quote"], RoundTrip<ViewInfo>("view_info").Aliases);
        Assert.Empty(RoundTrip<ViewInfo>("view_info_implicit").Aliases);
    }

    [Fact]
    public void ColumnRequestsUseActualServiceParameterTypes()
    {
        var operations = new[] { "ColumnAdd", "ColumnDrop", "ColumnRename", "ColumnTypeChange",
            "ColumnDefaultSet", "ColumnDefaultDrop", "ColumnCommentSet", "NotNullSet", "NotNullDrop" };
        foreach (var operation in operations)
        {
            var method = new RpcMethodInfo(typeof(IVgiService).GetMethod($"CatalogTable{operation}Async")!);
            foreach (var variant in new[] { "literal", "nested" })
            {
                var name = method.WireName["catalog_table_".Length..] + "_" + variant;
                using var stream = new MemoryStream(Fixture(name));
                using var reader = new ArrowStreamReader(stream);
                using var batch = reader.ReadNextRecordBatch();
                Assert.NotNull(batch);
                var args = ValueCodec.ExtractRow(batch, method.ParameterTypes);
                using var restoredBatch = ValueCodec.BuildRow(method.ParamsSchema, args);
                var restored = ValueCodec.ExtractRow(restoredBatch, method.ParameterTypes);
                for (var i = 0; i < args.Length; i++) Assert.Equivalent(args[i], restored[i]);
                var pathIndex = method.ParamsSchema.FieldsList.ToList().FindIndex(field => field.Name == "column_path");
                Assert.Equal(variant == "literal" ? ["a.b"] : new[] { "a", "b.\"quoted" },
                    Assert.IsType<List<string>>(args[pathIndex]));
                if (operation is "ColumnDefaultSet" or "ColumnTypeChange")
                {
                    var expressionIndex = method.ParamsSchema.FieldsList.ToList().FindIndex(field => field.Name == "expression");
                    if (operation == "ColumnTypeChange" && variant == "literal")
                    {
                        Assert.Null(args[expressionIndex]);
                    }
                    else
                    {
                        var payload = Assert.IsType<byte[]>(args[expressionIndex]);
                        Assert.Equal(payload, Assert.IsType<byte[]>(restored[expressionIndex]));
                        var expression = EmbeddedIpc.Decode<SqlExpression>(payload);
                        var schemaIndex = method.ParamsSchema.FieldsList.ToList().FindIndex(field => field.Name == "schema_path");
                        Assert.Equal(Assert.IsType<List<string>>(args[schemaIndex]), expression.SchemaPath);
                        Assert.NotEmpty(expression.Profile);
                        Assert.NotNull(expression.EvaluationContext);
                    }
                }
                Assert.Null(reader.ReadNextRecordBatch());
            }
        }
    }

    internal static byte[] Fixture(string name)
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            var path = Path.Combine(directory.FullName, "conformance", "catalog-contracts.json");
            if (File.Exists(path))
            {
                using var document = JsonDocument.Parse(File.ReadAllText(path));
                return Convert.FromBase64String(document.RootElement.GetProperty("records").GetProperty(name).GetString()!);
            }

            directory = directory.Parent;
        }

        throw new FileNotFoundException("Required catalog contract corpus is missing");
    }

    private static T RoundTrip<T>(string name) where T : class, new() =>
        EmbeddedIpc.Decode<T>(EmbeddedIpc.Encode(EmbeddedIpc.Decode<T>(Fixture(name))));

    [Fact]
    public void OptionsRetainOpaqueMetadataAndExplicitNullOverrides()
    {
        var changes = RoundTrip<OptionChanges>("changes");
        Assert.Equal(Fixture("values"), changes.SetValues);
        Assert.Equal(["reset.me"], changes.ResetNames);
        var reset = RoundTrip<OptionChanges>("reset_only");
        Assert.Null(reset.SetValues);
        Assert.Equal(["reset.me"], reset.ResetNames);
        var state = RoundTrip<OptionState>("state");
        Assert.Equal(Fixture("values"), state.Values);
        Assert.Equal(["optional"], state.ExplicitNames);
        var empty = RoundTrip<OptionState>("empty_state");
        Assert.Equal(Fixture("empty_values"), empty.Values);
        Assert.Empty(empty.ExplicitNames);
    }

    [Fact]
    public void RequiredPathsRetainComponentBoundariesAndGroups()
    {
        var paths = RoundTrip<RequiredFilterPaths>("paths");
        Assert.Equal("[[[\"a.b\"],[\"a\",\"b\"]],[[\"record\",\"literal.\\\"name\"]]]",
            JsonSerializer.Serialize(paths.Groups, new JsonSerializerOptions
            {
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            }));
        Assert.Empty(RoundTrip<RequiredFilterPaths>("empty_paths").Groups);
    }
    [Fact]
    public void CapabilityProfilesAndOptionDefinitionsRoundTrip()
    {
        var capabilities = RoundTrip<CatalogCapabilities>("capabilities");
        Assert.Equal(["vgi.schema.options.read.v1"], capabilities.Read);
        Assert.Equal(["vgi.schema.options.create.v1"], capabilities.Ddl);
        Assert.Empty(RoundTrip<CatalogCapabilities>("empty_capabilities").Read);
        var definitions = RoundTrip<OptionDefinitions>("definitions");
        Assert.Equal(Fixture("values"), definitions.Defaults);
        Assert.Equal(["optional", "record"], definitions.CreateNames);
        Assert.Equal(["optional"], definitions.AlterNames);
    }

    [Fact]
    public void LayoutsRetainOrdersAndClearVersusUnchanged()
    {
        var expression = RoundTrip<SqlExpression>("expression");
        Assert.Equal(["one.two", "nested"], expression.SchemaPath);
        var key = RoundTrip<SortKey>("sort_key");
        Assert.Equal(Fixture("expression"), key.Expression);
        Assert.Equal(SortDirection.Desc, key.Direction);
        Assert.Equal(NullOrder.First, key.NullOrder);
        var layout = RoundTrip<TableLayout>("layout");
        Assert.Equal(Fixture("expression"), Assert.Single(layout.PartitionBy));
        Assert.Equal(Fixture("sort_key"), Assert.Single(layout.SortedBy));
        Assert.Equal(LayoutCoverage.AllRows, layout.Coverage);
        Assert.Empty(RoundTrip<TableLayout>("empty_layout").SortedBy);
        var change = RoundTrip<TableLayoutChange>("layout_change");
        Assert.Equal(LayoutChangeMode.Rewrite, change.Mode);
        Assert.Empty(change.PartitionBy!);
        Assert.Equal(Fixture("sort_key"), Assert.Single(change.SortedBy!));
        var clear = RoundTrip<TableLayoutChange>("layout_clear");
        Assert.Equal(LayoutChangeMode.FutureWrites, clear.Mode);
        Assert.Null(clear.PartitionBy);
        Assert.Empty(clear.SortedBy!);
        var create = RoundTrip<TableCreateRequest>("table_create");
        Assert.Equal("kept", create.Comment);
        Assert.Equal(Fixture("values"), create.Options);
        Assert.Equal(Fixture("layout"), create.Layout);
        var old = RoundTrip<TableCreateRequest>("table_create_legacy");
        Assert.Null(old.Comment);
        Assert.Null(old.Tags);
        Assert.Null(old.Options);
        Assert.Null(old.Layout);
        var info = RoundTrip<TableInfo>("table_info");
        Assert.Equal(Fixture("state"), info.Options);
        Assert.Equal(Fixture("layout"), info.Layout);
        Assert.Single(create.ConstraintDefinitions);
        Assert.Empty(old.ConstraintDefinitions);
        Assert.Equal(new byte[] {114, 101, 118, 105, 115, 105, 111, 110, 0, 255}, info.DefinitionRevision);
        var constraint = EmbeddedIpc.Decode<ConstraintInfo>(Assert.Single(info.Constraints));
        Assert.Equal(create.ConstraintDefinitions[0], constraint.Definition);
        Assert.Equal("fixture-stable-key", EmbeddedIpc.Decode<CatalogObjectRef>(constraint.Ref).Name);
        Assert.Equal(new[] {"record", "a.b"}, Assert.Single(
            EmbeddedIpc.Decode<ConstraintDefinition>(constraint.Definition).ColumnPaths));
    }
}
