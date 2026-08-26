namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>bind</c> RPC's unary result. The C++ extension validates this type's embedded-IPC
/// schema with STRICT <c>arrow::Schema::Equals</c> (field count, order, name, type, and
/// nullability all must match exactly) against its own generated <c>BindResultSchema()</c> — so
/// property declaration order matters here too, even though individual field reads on the C++
/// side are by name.
/// </summary>
public sealed class BindResponse
{
    /// <summary>Serialized (schema-only, no row) Arrow IPC bytes describing the function's return
    /// value: one field, conventionally named "result".</summary>
    public byte[] OutputSchema { get; set; } = [];

    public byte[]? OpaqueData { get; set; }

    public List<string> LookupSecretTypes { get; set; } = [];

    public List<string> LookupScopes { get; set; } = [];

    public List<string> LookupNames { get; set; } = [];
}
