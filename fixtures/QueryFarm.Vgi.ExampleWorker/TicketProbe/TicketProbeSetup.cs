using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TicketProbe;

/// <summary>
/// <c>ticket_probe</c>: the cross-SDK fixture catalog for attach tickets (vgi-python
/// <c>docs/protocol/vgi-attach-tickets.md</c> §7). Every SDK's fixture worker serves it
/// identically, and the extension's <c>attach_ticket/*.test</c> sqllogictests run against each.
/// </summary>
/// <remarks>
/// <para>Attach options, in this order: <c>region</c> (<c>VARCHAR</c>, default
/// <c>'us-east-1'</c>) and <c>api_key</c> (<c>VARCHAR</c>, required, secret). Table
/// <c>main.probe</c>, backed by the table function <c>main.ticket_probe</c>, returns exactly one
/// row: the attached <c>region</c> and <c>api_key_sha256</c>, the first 12 lowercase hex characters
/// of SHA-256(UTF-8(api_key)). The key itself is never returned.</para>
/// <para>So a reattach with nothing but <c>vgi_attach_ticket</c> reading the same row proves the
/// secret option took effect without travelling again, and a reattach without the ticket fails for
/// want of the required <c>api_key</c>. The values ride <see cref="AttachContext.ExtraOpaqueData"/>
/// as <c>region \0 digest</c>, never stored on the worker.</para>
/// </remarks>
public static class TicketProbeSetup
{
    public const string CatalogName = "ticket_probe";
    public const string DefaultRegion = "us-east-1";

    public static CatalogInfo Info => new()
    {
        Name = CatalogName,
        AttachOptionSpecs =
        [
            EmbeddedIpc.Encode(AttachOptionSpecBuilder.Build(
                "region", "Region the probe reports back", StringType.Default,
                new StringArray.Builder().Append(DefaultRegion).Build())),
            EmbeddedIpc.Encode(AttachOptionSpecBuilder.Build(
                "api_key", "API key; only its digest is ever returned", StringType.Default,
                defaultValue: null, required: true, secret: true)),
        ],
    };

    /// <summary>The first 12 lowercase hex characters of SHA-256(UTF-8(<paramref name="apiKey"/>)).</summary>
    public static string ApiKeyDigest(string apiKey) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(apiKey)))[..12];

    /// <summary>Registers the catalog, its table and its function on <paramref name="worker"/>.</summary>
    public static Worker Register(Worker worker)
    {
        worker.RegisterCatalog(Info, exclusive: true);
        worker.RegisterCatalogTable(new CatalogTable
        {
            Name = "probe",
            SchemaName = "main",
            Comment = "The options this attach was made with",
            ScanFunction = new TicketProbeFunction(),
        }, identity: CatalogName);
        return worker;
    }

    /// <returns><see langword="null"/> when <paramref name="request"/> is for another catalog, so
    /// <c>Program.cs</c> can chain every setup module's <c>Handle</c> with <c>??</c>.</returns>
    public static AttachContext? Handle(CatalogAttachRequest request)
    {
        if (request.Name != CatalogName)
        {
            return null;
        }

        string? region = null;
        string? apiKey = null;
        if (request.Options is { Length: > 0 } bytes)
        {
            using var options = RecordBatchIpc.Read(bytes);
            for (var i = 0; i < options.Schema.FieldsList.Count; i++)
            {
                var name = options.Schema.FieldsList[i].Name;
                var value = options.Length > 0 && options.Column(i) is StringArray s && !s.IsNull(0) ? s.GetString(0) : null;
                if (string.Equals(name, "region", StringComparison.OrdinalIgnoreCase))
                {
                    region = value;
                }
                else if (string.Equals(name, "api_key", StringComparison.OrdinalIgnoreCase))
                {
                    apiKey = value;
                }
            }
        }

        if (apiKey is null)
        {
            throw new InvalidOperationException(
                $"Catalog '{CatalogName}' cannot be attached without the required option 'api_key'.");
        }

        var extra = Encoding.UTF8.GetBytes((string.IsNullOrEmpty(region) ? DefaultRegion : region) + "\0" + ApiKeyDigest(apiKey));
        return new AttachContext { ExtraOpaqueData = extra };
    }
}

/// <summary><c>ticket_probe.main.ticket_probe()</c>: one row, the attached <c>region</c> and a
/// digest of the attached <c>api_key</c>, decoded from <c>attach_opaque_data</c>.</summary>
public sealed class TicketProbeFunction : ITableFunction
{
    public static readonly Schema FixedSchema = new(
        [
            new Field("region", StringType.Default, nullable: true),
            new Field("api_key_sha256", StringType.Default, nullable: true),
        ],
        metadata: null);

    public string Name => "ticket_probe";

    public string SchemaName => "main";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema => FixedSchema;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(Decode(initParams.AttachOpaqueData));

    /// <summary>Strips <c>&lt;identity&gt;\0&lt;16-byte GUID&gt;</c> off <c>attach_opaque_data</c>
    /// and splits the remaining <c>region \0 digest</c>.</summary>
    internal static (string Region, string Digest) Decode(byte[] attachOpaqueData)
    {
        var separator = System.Array.IndexOf(attachOpaqueData, (byte)0);
        var extraStart = separator + 1 + 16;
        if (separator < 0 || extraStart > attachOpaqueData.Length)
        {
            throw new InvalidOperationException("ticket_probe must be read through an attach of the ticket_probe catalog");
        }

        var extra = attachOpaqueData[extraStart..];
        var split = System.Array.IndexOf(extra, (byte)0);
        if (split < 0)
        {
            throw new InvalidOperationException("ticket_probe must be read through an attach of the ticket_probe catalog");
        }

        return (Encoding.UTF8.GetString(extra, 0, split), Encoding.UTF8.GetString(extra, split + 1, extra.Length - split - 1));
    }

    private sealed class Producer((string Region, string Digest) row) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                output.Emit(new RecordBatch(
                    FixedSchema,
                    [new StringArray.Builder().Append(row.Region).Build(), new StringArray.Builder().Append(row.Digest).Build()],
                    1));
            }

            output.Finish();
        }
    }
}
