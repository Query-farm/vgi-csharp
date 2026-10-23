using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

public class MacroFamiliesTests
{
    [Fact]
    public async Task DiscoveryPreservesTypedNullMetadataAndEmptyDocumentation()
    {
        var field = new Field("x", Int64Type.Default, true, new Dictionary<string,string> { ["logical"] = "field annotation" });
        var schema = new Schema([field], new Dictionary<string,string> { ["logical"] = "schema annotation" });
        using var defaults = new RecordBatch(schema, [new Int64Array.Builder().AppendNull().Build()], 1);
        var registry = new CatalogRegistry();
        registry.RegisterMacro(new CatalogMacro { Name = "m", SchemaName = "sales.archive", MacroType = MacroType.Scalar,
            Parameters = ["x"], Definition = "x", ParameterDefaults = defaults,
            ParameterDocs = new Dictionary<string,string> { ["x"] = "" } });
        IVgiService service = new VgiServiceImpl(registry);
        var attach = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
        var listed = await service.CatalogSchemaContentsMacrosAsync(attach.AttachOpaqueData, ["sales.archive"], SchemaObjectType.ScalarMacro, null);
        var info = EmbeddedIpc.Decode<MacroInfo>(Assert.Single(listed.Items));
        var overload = Assert.Single(info.Overloads);
        Assert.Equal(["sales.archive"], overload.SchemaPath);
        var parameter = Assert.Single(overload.Parameters);
        Assert.Null(parameter.DeclaredType);
        Assert.Null(parameter.DefaultExpression);
        Assert.Equal("", parameter.Documentation);
        using var value = RecordBatchIpc.Read(parameter.DefaultValue!);
        Assert.True(value.Column(0).IsNull(0));
        Assert.Equal(Int64Type.Default, value.Schema.GetFieldByIndex(0).DataType);
        Assert.Equal("schema annotation", value.Schema.Metadata["logical"]);
        Assert.Equal("field annotation", value.Schema.GetFieldByIndex(0).Metadata["logical"]);
        var family = MacroFamilies.FromDescriptor(new CatalogMacro { Name = "m", SchemaName = "sales.archive",
            MacroType = MacroType.Scalar, Overloads = [overload, new MacroOverload { Parameters = [], Definition = "1",
                Profile = overload.Profile, SchemaPath = overload.SchemaPath, MacroType = overload.MacroType,
                EvaluationContext = overload.EvaluationContext }] });
        Assert.Equal(2, family.Count);
        Assert.Equal(parameter.DefaultValue, family[0].Parameters[0].DefaultValue);
        Assert.Empty(family[1].Parameters);
    }
}
