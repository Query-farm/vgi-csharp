using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Errors;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

/// <summary>
/// This port hosts the whole <c>vgi.v3</c> surface with the reference's exact schemas, so
/// <c>vgi_rpc.Reflection.v1</c> reports the same protocol hash here as every other SDK.
/// </summary>
public sealed class VgiProtocolHashTests
{
    /// <summary>
    /// The <c>vgi.v3</c> protocol hash the current vgi-python reference reports: 85 methods, over
    /// method names, types and params/result/header schemas (WIRE_PROTOCOL.md §14).
    /// </summary>
    /// <remarks>
    /// The one place this value is written down. It changes only when vgi.v3's protocol version
    /// (3.0.0) does — a new value here means the protocol changed, so take it from the reference,
    /// never from this port's own output.
    /// </remarks>
    public const string ReferenceVgiV3Hash = "50e9404173bee3262e96b8939eb5c425463336abca2a17916812d68b6e0f6cad";

    [Fact]
    public void HostsTheReferenceVgiV3Surface()
    {
        var server = new Worker().CatalogName("test_catalog").DefaultSchema("main")
            .NewRpcServer(Worker.ServerTransport.Stdio);
        Assert.Equal("vgi.v3", VgiProtocol.Name);
        Assert.Equal("3.0.0", Worker.DefaultProtocolVersion);
        Assert.Equal(VgiProtocol.Name, server.ProtocolName);
        Assert.Equal(ReferenceVgiV3Hash, server.ProtocolHash);
    }

    public static TheoryData<string, Func<IVgiService, Task>> Unimplemented() => new()
    {
        { "aggregate_window", s => s.AggregateWindowAsync(new AggregateWindowRequest()) },
        { "catalog_create", s => s.CatalogCreateAsync(new CatalogCreateRequest()) },
        { "catalog_drop", s => s.CatalogDropAsync("x") },
        { "catalog_dependencies", s => s.CatalogDependenciesAsync([], null) },
        { "catalog_table_scan_function_get", s => s.CatalogTableScanFunctionGetAsync([], ["main"], "t", null, null, null) },
        { "catalog_index_create", s => s.CatalogIndexCreateAsync(new IndexCreateRequest()) },
        { "catalog_index_drop", s => s.CatalogIndexDropAsync([], ["main"], "i", false, false, null) },
        { "catalog_index_option_definitions", s => s.CatalogIndexOptionDefinitionsAsync([], [], "ART", null) },
        { "catalog_schema_contents_indexes", s => s.CatalogSchemaContentsIndexesAsync([], ["main"], null) },
    };

    [Theory]
    [MemberData(nameof(Unimplemented))]
    public async Task AnUnimplementedMethodAnswersUnimplementedNeverSuccess(string method, Func<IVgiService, Task> call)
    {
        IVgiService service = new VgiServiceImpl(new CatalogRegistry());
        var error = await Assert.ThrowsAsync<MethodNotImplementedError>(() => call(service));
        Assert.Equal(ErrorCodes.Unimplemented, error.ErrorCode);
        Assert.Equal("method_not_implemented", error.ErrorKind);
        Assert.Equal(nameof(MethodNotImplementedError), error.GetType().Name); // the wire error type
        Assert.Equal($"{method} is not implemented by this worker", error.Message);
    }
}
