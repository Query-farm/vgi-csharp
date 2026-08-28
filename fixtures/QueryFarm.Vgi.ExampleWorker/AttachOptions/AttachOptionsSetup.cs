using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.ExampleWorker.AttachOptions;

/// <summary>
/// The <c>attach_options</c> / <c>attach_options_required</c> catalogs (<c>attach/
/// attach_options_echo.test</c>, <c>attach/attach_options_required.test</c>) — two catalogs from
/// the SAME worker process (unlike <c>versioned</c>/<c>versioned_tables</c>, their discovery
/// queries always filter <c>WHERE catalog = '...'</c>, so sharing <c>ExampleWorker</c> is safe;
/// see that project's own doc comment for the contrast).
///
/// <c>attach_options</c> declares 19 typed options (all defaulted — <see cref="AttachOptionEntries"/>)
/// and echoes the merged (supplied-over-default) values back via <see cref="EchoAttachOptionsFunction"/>.
/// <c>attach_options_required</c> declares 2 (<c>api_key</c> required/no-default, <c>region</c>
/// defaulted) and refuses an ATTACH missing <c>api_key</c> — the C++ extension surfaces
/// <c>required</c> at discovery from <see cref="CatalogInfo.AttachOptionSpecs"/> but does NOT
/// itself enforce it (verified: no such check exists client-side), so this worker must.
///
/// Values received at <c>catalog_attach</c> travel to <see cref="EchoAttachOptionsFunction"/> via
/// <see cref="AttachContext.ExtraOpaqueData"/> (see <c>Worker.OnAttach</c>'s doc comment) — never
/// stored on <c>self</c>/statics, so this is safe under pooled-worker reuse and stateless (HTTP)
/// dispatch, mirroring every other SDK's identical fixture.
/// </summary>
internal static class AttachOptionsSetup
{
    public const string CatalogName = "attach_options";
    public const string RequiredCatalogName = "attach_options_required";

    private static readonly AttachOptionEntry RequiredApiKey = new("api_key", "API key", StringType.Default, Default: null!);
    private static readonly AttachOptionEntry RequiredRegion = new("region", "Region", StringType.Default, new StringArray.Builder().Append("us-east-1").Build());

    public static CatalogInfo Info => new()
    {
        Name = CatalogName,
        AttachOptionSpecs = AttachOptionEntries.All
            .Select(e => AttachOptionSpecBuilder.Build(e.Name, e.Description, e.Type, e.Default))
            .Select(EmbeddedIpc.Encode)
            .ToList(),
    };

    public static CatalogInfo RequiredInfo => new()
    {
        Name = RequiredCatalogName,
        AttachOptionSpecs =
        [
            EmbeddedIpc.Encode(AttachOptionSpecBuilder.Build(RequiredApiKey.Name, RequiredApiKey.Description, RequiredApiKey.Type, defaultValue: null, required: true)),
            EmbeddedIpc.Encode(AttachOptionSpecBuilder.Build(RequiredRegion.Name, RequiredRegion.Description, RequiredRegion.Type, RequiredRegion.Default)),
        ],
    };

    /// <returns><see langword="null"/> when <paramref name="request"/> isn't for either catalog —
    /// lets <c>Program.cs</c> chain every setup module's <c>Handle</c> with <c>??</c>.</returns>
    public static AttachContext? Handle(CatalogAttachRequest request)
    {
        if (request.Name == RequiredCatalogName)
        {
            var suppliedNames = SuppliedOptionNames(request.Options);
            if (!suppliedNames.Contains("api_key"))
            {
                throw new InvalidOperationException(
                    $"Catalog '{RequiredCatalogName}' cannot be attached without the required option 'api_key'.");
            }

            // attach_options_required.test never calls echo_attach_options on this catalog — no
            // opaque payload needed.
            return new AttachContext();
        }

        if (request.Name == CatalogName)
        {
            var echoBatch = BuildEchoBatch(request.Options);
            return new AttachContext { ExtraOpaqueData = RecordBatchIpc.Write(echoBatch) };
        }

        return null;
    }

    private static HashSet<string> SuppliedOptionNames(byte[]? optionsBytes)
    {
        if (optionsBytes is not { Length: > 0 })
        {
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        using var supplied = RecordBatchIpc.Read(optionsBytes);
        return new HashSet<string>(supplied.Schema.FieldsList.Select(f => f.Name), StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>Merges caller-supplied option values (a subset — only what the ATTACH statement
    /// named) over <see cref="AttachOptionEntries.All"/>'s declared defaults, column by column —
    /// no per-type value decoding needed: each column is copied through as the <see cref="IArrowArray"/>
    /// the C++ client already coerced to this option's declared type.</summary>
    private static RecordBatch BuildEchoBatch(byte[]? optionsBytes)
    {
        RecordBatch? supplied = optionsBytes is { Length: > 0 } ? RecordBatchIpc.Read(optionsBytes) : null;

        var fields = new List<Field>();
        var arrays = new List<IArrowArray>();
        foreach (var entry in AttachOptionEntries.All)
        {
            fields.Add(new Field(entry.Name, entry.Type, nullable: true));
            var suppliedIndex = supplied?.Schema.GetFieldIndex(entry.Name) ?? -1;
            arrays.Add(suppliedIndex >= 0 ? supplied!.Column(suppliedIndex) : entry.Default);
        }

        return new RecordBatch(new Schema(fields, metadata: null), arrays, 1);
    }
}
