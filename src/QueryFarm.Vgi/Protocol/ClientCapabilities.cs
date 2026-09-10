namespace QueryFarm.Vgi.Protocol;

/// <summary>Capabilities advertised by an engine client inside a catalog attach request.</summary>
public sealed class ClientCapabilities
{
    public string Engine { get; set; } = "";

    public List<string> NativeFormats { get; set; } = [];

    public List<string> Catalogs { get; set; } = [];

    public bool CanStream { get; set; }

    public List<string> FilterEncodings { get; set; } = [];
}
