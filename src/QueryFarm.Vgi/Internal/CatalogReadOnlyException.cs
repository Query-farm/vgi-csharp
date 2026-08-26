namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Thrown by every catalog DDL RPC's default <see cref="Protocol.IVgiService"/> implementation.
/// None of this worker's catalogs support schema/table/view DDL (a declarative
/// <see cref="Catalog.CatalogTable"/>/<see cref="Catalog.CatalogView"/> is registered once at
/// worker startup, not created at runtime) — every DDL call fails the same way a real read-only
/// <c>vgi-python</c> <c>CatalogReadOnlyError</c> would. The message deliberately contains the exact
/// substring <c>attach/ddl_wire_contract.test</c> pins: "catalog is read-only".
/// </summary>
public sealed class CatalogReadOnlyException(string operation)
    : Exception($"catalog is read-only: '{operation}' is not supported by this VGI worker.");
