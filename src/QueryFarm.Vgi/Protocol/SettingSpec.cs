namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One element of <see cref="CatalogAttachResult.Settings"/> — a global/session DuckDB setting
/// (<c>SET &lt;name&gt; = ...</c>) a worker declares at attach time, separate from the per-function
/// <see cref="Attributes.SettingAttribute"/>/<c>RequiredSettings</c> mechanism (which only reads an
/// already-declared setting's CURRENT value; a setting must appear here at least once, from some
/// worker, for DuckDB to know it exists at all — see <c>duckdb_settings()</c>).
///
/// Wire shape mirrors vgi-python's <c>SettingSpec.ARROW_SCHEMA</c>/<c>VgiSetting</c> on the C++ side
/// (<c>vgi_catalog_metadata.hpp</c>): 4 columns, <c>name</c>/<c>description</c> plain strings,
/// <c>type</c> a schema-only IPC blob for a single field named <c>"value"</c> (see
/// <see cref="Internal.SchemaIpc.WriteSchemaOnly"/>), <c>default_value</c> a full one-row IPC batch
/// for that same single <c>"value"</c> column (see <see cref="Internal.RecordBatchIpc.Write"/>) —
/// <see langword="null"/> when the setting has no default. Each <see cref="SettingSpec"/> is itself
/// embedded-IPC-encoded (<see cref="Internal.EmbeddedIpc.Encode{T}"/>) before being placed in
/// <see cref="CatalogAttachResult.Settings"/>'s <c>list(binary)</c>.
/// </summary>
public sealed class SettingSpec
{
    public string Name { get; set; } = "";

    public string Description { get; set; } = "";

    /// <summary>Schema-only IPC bytes for a single field named <c>"value"</c> carrying this
    /// setting's Arrow type.</summary>
    public byte[] Type { get; set; } = [];

    /// <summary>One-row IPC batch (single column <c>"value"</c>, typed per <see cref="Type"/>)
    /// holding this setting's default — <see langword="null"/> when there is none.</summary>
    public byte[]? DefaultValue { get; set; }
}
