using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One element of <see cref="CatalogInfo.AttachOptionSpecs"/> — a typed ATTACH-time option
/// (<c>ATTACH ... (opt_name value, ...)</c>) a catalog declares, distinct from
/// <see cref="SettingSpec"/> (a global/session <c>SET</c>): an attach option is delivered once via
/// <c>catalog_attach</c>'s <see cref="CatalogAttachRequest.Options"/>, never resent.
///
/// Wire shape mirrors vgi-python's <c>AttachOptionSpec.ARROW_SCHEMA</c> — the SAME four columns as
/// <see cref="SettingSpec"/> (<c>name</c>/<c>description</c>/<c>type</c>/<c>default_value</c>) plus
/// two appended nullable boolean columns, <c>required</c> then <c>secret</c> (see
/// <see cref="Secret"/>). Readers look both up by name, so a peer that predates either column
/// ignores it, and a spec without it reads as <see langword="false"/>. <c>required</c>: <see langword="true"/> means the caller MUST supply this
/// option at <c>ATTACH</c> time (mutually exclusive with a default — an option with a default is
/// always satisfiable without the caller). The C++ extension surfaces this on
/// <c>vgi_catalogs().attach_options[].required</c> for pre-attach discovery; it does NOT itself
/// enforce it — a worker that declares a required option must reject a missing one itself (e.g.
/// from a <see cref="Worker.OnAttach"/> handler), matching every other SDK's contract.
/// </summary>
public sealed class AttachOptionSpec
{
    public string Name { get; set; } = "";

    public string Description { get; set; } = "";

    /// <summary>Schema-only IPC bytes for a single field named <c>"value"</c> carrying this
    /// option's Arrow type — see <see cref="SettingSpec.Type"/>.</summary>
    public byte[] Type { get; set; } = [];

    /// <summary>One-row IPC batch (single column <c>"value"</c>, typed per <see cref="Type"/>)
    /// holding this option's default — <see langword="null"/> when there is none (required
    /// options never have one).</summary>
    public byte[]? DefaultValue { get; set; }

    /// <summary>The caller must supply this option at <c>ATTACH</c> time. See the type's summary.</summary>
    [WireOptional]
    public bool Required { get; set; }

    /// <summary>
    /// This option is a credential: an API key, token, password or anything else that must not
    /// leak. <b>Credential options MUST be declared secret.</b> The caller passes the value inline
    /// as an ordinary attach option; the DuckDB extension then redacts it from
    /// <c>duckdb_databases()</c>, keeps only a salted hash of it in its result-cache key, and never
    /// logs it, and clients mask it and keep it out of exported or shared configuration. To keep
    /// the credential out of the SQL text itself, pass it as an expression:
    /// <code>
    /// ATTACH 'sales' (TYPE vgi, LOCATION '&lt;worker url&gt;', api_key getenv('SALES_API_KEY'));
    /// </code>
    /// Combines with <see cref="Required"/> (a required secret lets a client ask for the credential
    /// before attaching). A default is allowed but a secret option should normally have none — a
    /// default credential is one every caller shares, published at discovery.
    /// </summary>
    [WireOptional]
    public bool Secret { get; set; }
}
