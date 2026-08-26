namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One item of a <c>catalog_copy_from_formats</c> <see cref="ItemsResponse"/> — despite the RPC's
/// historical name, this single method covers BOTH <c>COPY ... TO</c> and <c>COPY ... FROM</c>
/// custom formats, disambiguated by <see cref="Direction"/>. The C++ extension validates this
/// type's embedded-IPC schema with STRICT <c>arrow::Schema::Equals</c> against its generated
/// <c>CopyFromFormatInfoSchema()</c> — property declaration order matters and must match that
/// schema exactly: comment, tags, format_name, handler, options, direction, description, ordered.
/// </summary>
public sealed class CopyFromFormatInfo
{
    public string? Comment { get; set; }

    public Dictionary<string, string> Tags { get; set; } = [];

    /// <summary>The bare (unqualified) name the <c>FORMAT '...'</c> COPY option names — the C++
    /// extension prefixes this with the ATTACH alias for display (<c>vgi_copy_formats()</c>'s own
    /// <c>format_name</c> column) and for the actual <c>FORMAT '&lt;alias&gt;.&lt;this&gt;'</c>
    /// SQL syntax; this worker never needs to know its own attach alias.</summary>
    public string FormatName { get; set; } = "";

    /// <summary>The bare schema-qualified-by-default-schema function name this format dispatches
    /// to — an ordinary <see cref="Table.ITableFunction"/> registration (COPY FROM) or
    /// <see cref="Buffering.ITableBufferingFunction"/> registration (COPY TO), found the SAME way
    /// any other bind/init call resolves a function name.</summary>
    public string Handler { get; set; } = "";

    /// <summary>Serialized (schema-only) Arrow IPC bytes describing the format's options — same
    /// shape/metadata conventions as <see cref="FunctionInfo.Arguments"/> (every field a NAMED
    /// argument; <c>vgi_doc</c> metadata carries <c>option_description</c>).</summary>
    public byte[] Options { get; set; } = [];

    /// <summary><c>"from"</c>, <c>"to"</c>, or <c>"both"</c>.</summary>
    public string Direction { get; set; } = "from";

    public string Description { get; set; } = "";

    /// <summary>COPY-TO-only: <see langword="true"/> forces a single-threaded, source-ordered sink
    /// (mirrors <see cref="Buffering.ITableBufferingFunction.SinkOrderDependent"/>) — always
    /// <see langword="false"/> for a COPY-FROM reader.</summary>
    public bool Ordered { get; set; }
}
