using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Arguments;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;
using Xunit;
using CatalogRegistry = QueryFarm.Vgi.Internal.CatalogRegistry;
using EmbeddedIpc = QueryFarm.Vgi.Internal.EmbeddedIpc;
using VgiServiceImpl = QueryFarm.Vgi.Internal.VgiServiceImpl;

namespace QueryFarm.Vgi.Tests.Scalar;

/// <summary>ScalarFn's Compute discovery (static / inherited / [Compute] / ambiguity) and the
/// catalog metadata members (Comment/Tags/Categories/Examples).</summary>
public class ScalarFnDiscoveryTests
{
    private static RecordBatch Run(IScalarFunction fn, params long[] values)
    {
        var input = new RecordBatch(new Schema([new Field("col_0", Int64Type.Default, true)], null),
            [new Int64Array.Builder().AppendRange(values).Build()], values.Length);
        return fn.Process(new ScalarProcessParams { Input = input, OutputSchema = fn.OutputSchema });
    }

    private static long[] Values(RecordBatch batch) => ((Int64Array)batch.Column(0)).Values.ToArray();

    private sealed class StaticCompute : ScalarFn
    {
        public override string Name => "static_compute";

        private static void Compute([Param] Int64Array value, Int64Array.Builder result)
        {
            for (var i = 0; i < value.Length; i++)
            {
                result.Append(value.GetValue(i)!.Value + 1);
            }
        }
    }

    private abstract class DoublingBase : ScalarFn
    {
        protected void Compute([Param] Int64Array value, Int64Array.Builder result)
        {
            for (var i = 0; i < value.Length; i++)
            {
                result.Append(value.GetValue(i)!.Value * 2);
            }
        }
    }

    private sealed class InheritedCompute : DoublingBase
    {
        public override string Name => "inherited_compute";
    }

    private sealed class Overloaded : ScalarFn
    {
        public override string Name => "overloaded";

        private static void Compute([Param] Int64Array value, Int64Array.Builder result) => throw new NotSupportedException();

        private static void Compute([Param] StringArray value, StringArray.Builder result) => throw new NotSupportedException();
    }

    private sealed class MarkedOverload : ScalarFn
    {
        public override string Name => "marked_overload";

        private static void Compute([Param] StringArray value, StringArray.Builder result) => throw new NotSupportedException();

        [Compute]
        private static void Negate([Param] Int64Array value, Int64Array.Builder result)
        {
            for (var i = 0; i < value.Length; i++)
            {
                result.Append(-value.GetValue(i)!.Value);
            }
        }
    }

    private sealed class TwoMarked : ScalarFn
    {
        public override string Name => "two_marked";

        [Compute]
        private static void A([Param] Int64Array value, Int64Array.Builder result) => throw new NotSupportedException();

        [Compute]
        private static void B([Param] Int64Array value, Int64Array.Builder result) => throw new NotSupportedException();
    }

    private sealed class Described : ScalarFn
    {
        public override string Name => "described";

        public override string? Comment => "a comment";

        public override IReadOnlyDictionary<string, string> Tags => new Dictionary<string, string> { ["owner"] = "qf" };

        public override IReadOnlyList<string> Categories => ["math"];

        public override IReadOnlyList<FunctionExample> Examples =>
            [new FunctionExample { Sql = "SELECT described(1)", Description = "adds one", ExpectedOutput = "2" }];

        private static void Compute([Param] Int64Array value, Int64Array.Builder result)
        {
        }
    }

    private sealed class ExampleTable : ITableFunction
    {
        public string Name => "example_table";

        public Schema ArgumentsSchema { get; } = new([], null);

        public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, true)], null);

        public IReadOnlyList<FunctionExample> Examples => [new FunctionExample { Sql = "SELECT * FROM example_table()", Description = "all rows" }];

        public ITableFunctionProducer CreateProducer(TableInitParams initParams) => throw new NotSupportedException();
    }

    private sealed class SecretLength : ScalarFn
    {
        public override string Name => "secret_length";

        private static void Compute([Param] Int64Array value, [Secret(SecretType = "s3")] Secret? creds, Int64Array.Builder result)
        {
            for (var i = 0; i < value.Length; i++)
            {
                result.Append(creds?.GetString("key_id")?.Length ?? -1);
            }
        }
    }

    [Fact]
    public void SecretParameter_MayBeTypedAsSecret()
    {
        var fn = new SecretLength();
        Assert.Equal("s3", Assert.Single(fn.RequiredSecrets).SecretType);
        var input = new RecordBatch(new Schema([new Field("col_0", Int64Type.Default, true)], null),
            [new Int64Array.Builder().Append(1).Build()], 1);
        var secrets = QueryFarm.Vgi.Tests.Arguments.PublicArgumentApiTests.SecretsBytes(("mine", [("type", "s3"), ("key_id", "ABCD")]));
        var output = fn.Process(new ScalarProcessParams { Input = input, OutputSchema = fn.OutputSchema, Secrets = secrets });
        Assert.Equal([4L], Values(output));
    }

    [Fact]
    public void StaticCompute_IsDispatched() => Assert.Equal([2L, 3L], Values(Run(new StaticCompute(), 1, 2)));

    [Fact]
    public void InheritedCompute_IsDispatched() => Assert.Equal([2L, 4L], Values(Run(new InheritedCompute(), 1, 2)));

    [Fact]
    public void ComputeAttribute_PicksAmongOverloads() => Assert.Equal([-1L], Values(Run(new MarkedOverload(), 1)));

    [Fact]
    public void AmbiguousOverloads_FailWithAClearMessage()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new Overloaded());
        Assert.Contains("2 'Compute' overloads", error.Message, StringComparison.Ordinal);
        Assert.Contains("[Compute]", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TwoComputeAttributes_FailWithAClearMessage()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new TwoMarked());
        Assert.Contains("more than one method with [Compute]", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CatalogMetadata_FlowsIntoFunctionInfo()
    {
        var registry = new CatalogRegistry();
        registry.RegisterScalar(new Described());
        registry.RegisterTable(new ExampleTable());
        var service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData ?? [];

        var scalar = EmbeddedIpc.Decode<FunctionInfo>(Assert.Single(
            (await service.CatalogSchemaContentsFunctionsAsync(attach, ["main"], SchemaObjectType.ScalarFunction, null)).Items));
        Assert.Equal("a comment", scalar.Comment);
        Assert.Equal("qf", scalar.Tags!["owner"]);
        Assert.Equal(["math"], scalar.Categories);
        var example = Assert.Single(scalar.Examples);
        Assert.Equal("SELECT described(1)", example.Sql);
        Assert.Equal("2", example.ExpectedOutput);

        var table = EmbeddedIpc.Decode<FunctionInfo>(Assert.Single(
            (await service.CatalogSchemaContentsFunctionsAsync(attach, ["main"], SchemaObjectType.TableFunction, null)).Items));
        Assert.Equal("all rows", Assert.Single(table.Examples).Description);
    }
}
