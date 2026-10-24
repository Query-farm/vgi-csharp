using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Errors;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

/// <summary>
/// This port hosts the whole <c>vgi.v2</c> surface with the reference's exact schemas, so
/// <c>vgi_rpc.Reflection.v1</c> reports the same protocol hash here as every other SDK.
/// </summary>
public sealed class VgiProtocolHashTests
{
    /// <summary>
    /// The <c>vgi.v2</c> protocol hash the current vgi-python reference reports: 85 methods, over
    /// method names, types and params/result/header schemas (WIRE_PROTOCOL.md §14).
    /// </summary>
    /// <remarks>
    /// The one place this value is written down. It changes only when vgi.v2's protocol version
    /// (2.2.0) does — a new value here means the protocol changed, so take it from the reference,
    /// never from this port's own output.
    /// </remarks>
    public const string ReferenceVgiV2Hash = "41f61f37c7ce42f63f3ca47f8861508f3f356d2422a5b8a0bab48ff92c0857a8";

    [Fact]
    public void HostsTheReferenceVgiV2Surface()
    {
        var server = new Worker().CatalogName("test_catalog").DefaultSchema("main")
            .NewRpcServer(Worker.ServerTransport.Stdio);
        Assert.Equal(VgiProtocol.Name, server.ProtocolName);
        Assert.Equal(ReferenceVgiV2Hash, server.ProtocolHash);
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
