using Apache.Arrow;

namespace QueryFarm.Vgi.Catalog;

/// <summary>A declaratively-registered COPY TO/FROM custom format — see
/// <see cref="Protocol.CopyFromFormatInfo"/> for the wire shape this becomes. Built by
/// <see cref="Worker.RegisterCopyFromFormat"/>/<see cref="Worker.RegisterCopyToFormat"/>, not
/// constructed directly by most fixtures.</summary>
public sealed class CopyFormat
{
    public required string FormatName { get; init; }

    public required string Handler { get; init; }

    /// <summary><c>"from"</c> or <c>"to"</c>.</summary>
    public required string Direction { get; init; }

    public required Schema Options { get; init; }

    public bool Ordered { get; init; }

    public string Description { get; init; } = "";

    public string? Comment { get; init; }

    public Dictionary<string, string> Tags { get; init; } = [];
}
