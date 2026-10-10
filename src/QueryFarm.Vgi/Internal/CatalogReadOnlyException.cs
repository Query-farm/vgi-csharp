using QueryFarm.VgiRpc.Errors;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Thrown by <see cref="VgiServiceImpl"/>'s catalog DDL RPCs for every catalog except a DDL-capable
/// in-memory one (<see cref="Worker.RegisterInMemoryCatalog"/>), which serves schema/table/view
/// create and drop. Declarative catalogs support no schema/table/view
/// DDL (a declarative
/// <see cref="Catalog.CatalogTable"/>/<see cref="Catalog.CatalogView"/> is registered once at
/// worker startup, not created at runtime) — every DDL call fails the same way a real read-only
/// <c>vgi-python</c> <c>CatalogReadOnlyError</c> would. The message deliberately contains the exact
/// substring <c>attach/ddl_wire_contract.test</c> pins: "catalog is read-only". Reported as
/// <c>FAILED_PRECONDITION</c>: the request is well-formed, the catalog's state refuses it.
/// </summary>
public sealed class CatalogReadOnlyException(string operation)
    : Exception($"catalog is read-only: '{operation}' is not supported by this VGI worker."), IRpcErrorModel
{
    public string ErrorCode => ErrorCodes.FailedPrecondition;

    public string? ErrorKind => null;

    public IReadOnlyList<System.Text.Json.JsonElement> ErrorDetails => [];
}
