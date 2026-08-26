namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>catalog_attach</c> RPC's unary result. The C++ extension validates this type's
/// embedded-IPC schema with STRICT <c>arrow::Schema::Equals</c> against its generated
/// <c>CatalogAttachResultSchema()</c> — property declaration order matters and must match that
/// 17-field schema exactly, even though individual reads on the C++ side are by name (most via
/// <c>.value_or(default)</c>, tolerant of a missing/empty value but NOT of a missing/wrongly-typed
/// column).
/// </summary>
public sealed class CatalogAttachResult
{
    public byte[] AttachOpaqueData { get; set; } = [];

    public bool SupportsTransactions { get; set; }

    public bool SupportsTimeTravel { get; set; }

    public bool CatalogVersionFrozen { get; set; }

    public long CatalogVersion { get; set; }

    public bool AttachOpaqueDataRequired { get; set; }

    public string DefaultSchema { get; set; } = "main";

    /// <summary>Each element a serialized <c>Setting</c> — empty for M1 (no settings surface yet).</summary>
    public List<byte[]> Settings { get; set; } = [];

    /// <summary>Each element a serialized <c>SecretTypeSpec</c> — empty for M1.</summary>
    public List<byte[]> SecretTypes { get; set; } = [];

    /// <summary>Each element a serialized companion-catalog descriptor — empty for M1.</summary>
    public List<byte[]> AttachCatalogs { get; set; } = [];

    public string? Comment { get; set; }

    public Dictionary<string, string> Tags { get; set; } = [];

    public bool SupportsColumnStatistics { get; set; }

    /// <summary>Each element a serialized <see cref="FunctionInfo"/> (protocol 1.3.0+ globally-
    /// published functions) — empty for M1.</summary>
    public List<byte[]> GlobalFunctions { get; set; } = [];

    public string GlobalFunctionPrefix { get; set; } = "";

    public string? ResolvedDataVersion { get; set; }

    public string? ResolvedImplementationVersion { get; set; }
}
