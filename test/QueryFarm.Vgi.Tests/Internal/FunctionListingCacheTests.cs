using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// A function listing depends only on what is registered, so <see cref="VgiServiceImpl"/> builds
/// and encodes each (attach identity, schema, listing type) once, and each function's
/// <see cref="FunctionInfo"/> once, instead of on every <c>catalog_schema_contents_functions</c>
/// request and every ATTACH (about 1 ms per function, ~170 ms for the example worker's main-schema
/// table-function listing). These pin that, and that a registration made after the first listing
/// is still advertised.
/// </summary>
public class FunctionListingCacheTests
{
    /// <summary>Counts how many times its <see cref="FunctionInfo"/> was built: the description is
    /// read exactly once per build.</summary>
    private sealed class CountingScalar(string name, string schemaName = "main") : ScalarFn
    {
        private int _builds;

        public int Builds => Volatile.Read(ref _builds);

        public override string Name => name;

        public override string SchemaName => schemaName;

        public override string Description
        {
            get
            {
                Interlocked.Increment(ref _builds);
                return $"{name} doubles its argument";
            }
        }

        private void Compute([Param] Int64Array value, Int64Array.Builder result)
        {
            for (var row = 0; row < value.Length; row++)
            {
                result.Append(value.GetValue(row) * 2);
            }
        }
    }

    private static async Task<List<string>> ListScalarsAsync(VgiServiceImpl service, byte[] attach, string schema = "main")
    {
        var response = await service.CatalogSchemaContentsFunctionsAsync(attach, [schema], SchemaObjectType.ScalarFunction, null);
        return [.. response.Items.Select(EmbeddedIpc.Decode<FunctionInfo>).Select(info => info.Name)];
    }

    [Fact]
    public async Task Listing_BuildsEachFunctionInfoOnce_AcrossRequests()
    {
        var registry = new CatalogRegistry();
        var first = new CountingScalar("first");
        var second = new CountingScalar("second");
        registry.RegisterScalar(first);
        registry.RegisterScalar(second);
        var service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData;

        for (var i = 0; i < 5; i++)
        {
            Assert.Equal(["first", "second"], (await ListScalarsAsync(service, attach)).Order());
        }

        Assert.Equal(1, first.Builds);
        Assert.Equal(1, second.Builds);
    }

    [Fact]
    public async Task Listing_ReturnsTheSameBytesAsAFreshEncoding()
    {
        var registry = new CatalogRegistry();
        registry.RegisterScalar(new CountingScalar("only"));
        var service = new VgiServiceImpl(registry);

        var first = await service.CatalogSchemaContentsFunctionsAsync([], ["main"], SchemaObjectType.ScalarFunction, null);
        var second = await service.CatalogSchemaContentsFunctionsAsync([], ["main"], SchemaObjectType.ScalarFunction, null);
        var fresh = await new VgiServiceImpl(registry).CatalogSchemaContentsFunctionsAsync([], ["main"], SchemaObjectType.ScalarFunction, null);

        Assert.Equal(fresh.Items, second.Items);
        Assert.Equal(first.Items, second.Items);
        // Each response owns its list: a caller mutating one can't change what the next one gets.
        Assert.NotSame(first.Items, second.Items);
        first.Items.Clear();
        Assert.Single((await service.CatalogSchemaContentsFunctionsAsync([], ["main"], SchemaObjectType.ScalarFunction, null)).Items);
    }

    [Fact]
    public async Task Listing_IsPerSchema()
    {
        var registry = new CatalogRegistry();
        registry.RegisterScalar(new CountingScalar("in_main"));
        registry.RegisterScalar(new CountingScalar("in_data", schemaName: "data"));
        var service = new VgiServiceImpl(registry);

        Assert.Equal(["in_main"], await ListScalarsAsync(service, []));
        Assert.Equal(["in_data"], await ListScalarsAsync(service, [], "data"));
        Assert.Equal(["in_main"], await ListScalarsAsync(service, []));
    }

    [Fact]
    public async Task Listing_AdvertisesAFunctionRegisteredAfterTheFirstListing()
    {
        var registry = new CatalogRegistry();
        var early = new CountingScalar("early");
        registry.RegisterScalar(early);
        var service = new VgiServiceImpl(registry);

        Assert.Equal(["early"], await ListScalarsAsync(service, []));

        registry.RegisterScalar(new CountingScalar("late"));

        Assert.Equal(["early", "late"], (await ListScalarsAsync(service, [])).Order());
        Assert.Equal(1, early.Builds);
    }

    [Fact]
    public async Task Attach_BuildsEachGlobalFunctionInfoOnce_SharedWithTheSchemaListing()
    {
        var registry = new CatalogRegistry();
        var global = new CountingScalar("global_fn");
        registry.RegisterScalar(global);
        registry.RegisterGlobalFunction(global);
        var service = new VgiServiceImpl(registry);

        byte[]? attach = null;
        for (var i = 0; i < 5; i++)
        {
            var result = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
            Assert.Equal("global_fn", EmbeddedIpc.Decode<FunctionInfo>(Assert.Single(result.GlobalFunctions)).Name);
            attach = result.AttachOpaqueData;
        }

        Assert.Equal(["global_fn"], await ListScalarsAsync(service, attach!));
        Assert.Equal(1, global.Builds);
    }
}
