namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>catalog_attach</c> RPC's packed request. Property order matches the C++ extension's
/// <c>BuildCatalogAttachRequest</c> field order: name, options, data_version_spec,
/// implementation_version, client_capabilities.
/// </summary>
public sealed class CatalogAttachRequest
{
    public string Name { get; set; } = "";

    public byte[]? Options { get; set; }

    public string? DataVersionSpec { get; set; }

    public string? ImplementationVersion { get; set; }

    public byte[]? ClientCapabilities { get; set; }
}
