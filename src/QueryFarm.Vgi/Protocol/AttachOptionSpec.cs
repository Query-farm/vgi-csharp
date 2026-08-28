namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One element of <see cref="CatalogInfo.AttachOptionSpecs"/> — a typed ATTACH-time option
/// (<c>ATTACH ... (opt_name value, ...)</c>) a catalog declares, distinct from
/// <see cref="SettingSpec"/> (a global/session <c>SET</c>): an attach option is delivered once via
/// <c>catalog_attach</c>'s <see cref="CatalogAttachRequest.Options"/>, never resent.
///
/// Wire shape mirrors vgi-python's <c>AttachOptionSpec.ARROW_SCHEMA</c> — the SAME four columns as
/// <see cref="SettingSpec"/> (<c>name</c>/<c>description</c>/<c>type</c>/<c>default_value</c>) plus
/// one appended <c>required</c> column: <see langword="true"/> means the caller MUST supply this
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

    public bool Required { get; set; }
}
