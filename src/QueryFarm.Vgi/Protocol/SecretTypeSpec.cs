namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One element of <see cref="CatalogAttachResult.SecretTypes"/> — a custom DuckDB secret TYPE
/// (<c>CREATE SECRET (TYPE &lt;name&gt;, ...)</c>) a worker declares at attach time. Mirrors
/// vgi-python's <c>SecretTypeSpec.ARROW_SCHEMA</c>/vgi-java's <c>SecretTypeSpec</c> record on the C++
/// side (<c>vgi_catalog_metadata.hpp</c>'s <c>VgiSecretType</c>/<c>ParseVgiSecretType</c>): 3 columns,
/// <c>name</c>/<c>description</c> plain strings, <c>parameters_schema</c> a schema-only IPC blob (see
/// <see cref="Internal.SchemaIpc.WriteSchemaOnly"/>) describing the secret's key/value parameters —
/// mark a sensitive field's metadata <c>"redact":"true"</c> so DuckDB masks it in
/// <c>duckdb_secrets()</c>. Each <see cref="SecretTypeSpec"/> is itself embedded-IPC-encoded
/// (<see cref="Internal.EmbeddedIpc.Encode{T}"/>) before being placed in
/// <see cref="CatalogAttachResult.SecretTypes"/>'s <c>list(binary)</c>.
/// </summary>
public sealed class SecretTypeSpec
{
    public string Name { get; set; } = "";

    public string Description { get; set; } = "";

    /// <summary>Schema-only IPC bytes describing the secret's key/value parameters — field metadata
    /// <c>"redact":"true"</c> on a field marks it for masking in <c>duckdb_secrets()</c>.</summary>
    public byte[] ParametersSchema { get; set; } = [];
}
