namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One item of a <c>catalog_catalogs</c> <see cref="ItemsResponse"/> — the pre-<c>ATTACH</c>
/// discovery surface <c>vgi_catalogs('&lt;worker location&gt;')</c> reads. The C++ extension validates
/// this type's embedded-IPC schema with STRICT <c>arrow::Schema::Equals</c> against its generated
/// <c>CatalogInfoSchema()</c> — property declaration order matters and must match that schema
/// exactly: name, implementation_version, data_version_spec, attach_option_specs, releases,
/// source_url.
/// </summary>
public sealed class CatalogInfo
{
    public string Name { get; set; } = "";

    public string? ImplementationVersion { get; set; }

    public string? DataVersionSpec { get; set; }

    /// <summary>Each element a serialized attach-time option spec (same wire shape as a
    /// <see cref="SettingSpec"/>) — empty when this catalog declares none.</summary>
    public List<byte[]> AttachOptionSpecs { get; set; } = [];

    public List<CatalogRelease> Releases { get; set; } = [];

    public string? SourceUrl { get; set; }
}

/// <summary>Nested struct inside <see cref="CatalogInfo.Releases"/>. Property order matches the
/// C++ side's struct field order: version, released_at, summary, notes_url.</summary>
public sealed class CatalogRelease
{
    public string Version { get; set; } = "";

    public DateTimeOffset? ReleasedAt { get; set; }

    public string Summary { get; set; } = "";

    public string? NotesUrl { get; set; }
}
