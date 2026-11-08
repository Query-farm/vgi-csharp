using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Catalog;

/// <summary>
/// ATTACH options: declaring them for discovery (<see cref="Spec"/>), reading the values a
/// <c>catalog_attach</c> request carries (<see cref="Read(CatalogAttachRequest)"/>), and reading them
/// back in every function and transaction hook of the attach (<see cref="FromAttachData"/>, or the
/// <c>AttachOptions</c> property of the params / <see cref="TransactionContext"/>).
/// </summary>
/// <remarks>
/// <para>The worker keeps an attach's options in its <c>attach_opaque_data</c>: non-secret ones in
/// plain form, <c>secret</c> ones (declared with <c>secret: true</c>) encrypted with
/// XChaCha20-Poly1305 under a key derived (HKDF-SHA256) from the worker's secret-options key
/// (<see cref="Worker.AttachSecretOptionsKey"/>), else its HTTP signing key on HTTP, else a host-local
/// key file (<c>$VGI_ATTACH_SECRET_KEY_FILE</c>, <c>$XDG_RUNTIME_DIR/vgi/attach-secret-options.key</c>
/// or <c>&lt;temp&gt;/vgi-&lt;user&gt;/attach-secret-options.key</c>, 32 random bytes, mode 0600,
/// created on first use). The encrypted part is bound to its attach, so it cannot be moved into
/// another one. Secret values therefore never appear in plaintext in a value the client holds, on
/// any transport, and catalog code never needs to copy them into
/// <see cref="AttachContext.ExtraOpaqueData"/> or keep them server-side.</para>
/// </remarks>
/// <example>
/// <code>
/// worker.RegisterCatalog(new CatalogInfo
/// {
///     Name = "sales",
///     AttachOptionSpecs =
///     [
///         AttachOptions.Spec("region", "Region to read", StringType.Default, defaultValue: "us-east-1"),
///         AttachOptions.Spec("api_key", "API key", StringType.Default, required: true, secret: true),
///         AttachOptions.Spec("transactional", "Stage writes until COMMIT", BooleanType.Default, defaultValue: false),
///     ],
/// })
/// .OnAttach(request =>
/// {
///     var options = AttachOptions.Read(request);
///     return new AttachContext { SupportsTransactions = options.GetBoolean("transactional") ?? false };
/// });
/// </code>
/// </example>
public sealed class AttachOptions
{
    private readonly Dictionary<string, object?> _values;

    private AttachOptions(Dictionary<string, object?> values) => _values = values;

    private static readonly AttachOptions s_empty = new(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

    /// <summary>The ATTACH options of the attach <paramref name="attachOpaqueData"/> belongs to (any
    /// <c>*Params.AttachOpaqueData</c>, or <see cref="TransactionContext.AttachOpaqueData"/>),
    /// <c>secret</c> ones included. Shorthand for <c>AttachData.Parse(attachOpaqueData).Options</c>.</summary>
    /// <exception cref="Exception">The encrypted part does not open: the uniform
    /// <c>attach_opaque_data not recognized</c> error.</exception>
    public static AttachOptions FromAttachData(byte[]? attachOpaqueData) => AttachData.Parse(attachOpaqueData).Options;

    internal static AttachOptions FromSection(byte[]? section, byte[] attachId)
    {
        if (section is null)
        {
            return s_empty;
        }

        var (plain, blob) = AttachOptionsEnvelope.Parts(section);
        var options = Read(plain);
        if (blob.Length > 0)
        {
            foreach (var (name, value) in Read(AttachSecretOptions.Decrypt(blob, attachId)).Values)
            {
                options._values[name] = value;
            }
        }

        return options;
    }

    /// <summary>Every supplied option (DuckDB fills in declared defaults), by name — compared
    /// case-insensitively, as DuckDB compares option names. Values are boxed CLR values
    /// (<see cref="string"/>, <see cref="bool"/>, <see cref="long"/>, <see cref="double"/>,
    /// <see cref="byte"/>[], ...), or <see langword="null"/> for SQL NULL; a type with no plain
    /// CLR scalar (timestamp, list, struct, ...) is the one-row <see cref="IArrowArray"/> itself.</summary>
    public IReadOnlyDictionary<string, object?> Values => _values;

    /// <summary>Whether the request carries <paramref name="name"/> at all.</summary>
    public bool Contains(string name) => _values.ContainsKey(name);

    /// <summary>The raw value of <paramref name="name"/>, or <see langword="null"/> when absent or NULL.</summary>
    public object? Get(string name) => _values.GetValueOrDefault(name);

    /// <summary>The option as a string (any value's invariant text form), or <see langword="null"/>.</summary>
    public string? GetString(string name) => Get(name) switch
    {
        null => null,
        string s => s,
        bool b => b ? "true" : "false",
        IFormattable f => f.ToString(null, System.Globalization.CultureInfo.InvariantCulture),
        var other => other.ToString(),
    };

    /// <summary>The option as a boolean (a BOOLEAN value, or the text <c>true</c>/<c>false</c>/<c>1</c>/<c>0</c>),
    /// or <see langword="null"/> when absent or NULL.</summary>
    /// <exception cref="FormatException">The value is not a boolean.</exception>
    public bool? GetBoolean(string name) => Get(name) switch
    {
        null => null,
        bool b => b,
        string s when s.Equals("true", StringComparison.OrdinalIgnoreCase) || s == "1" => true,
        string s when s.Equals("false", StringComparison.OrdinalIgnoreCase) || s == "0" => false,
        var other when IsInteger(other) => Convert.ToInt64(other, System.Globalization.CultureInfo.InvariantCulture) != 0,
        var other => throw new FormatException($"Attach option '{name}' is not a boolean (got {other.GetType().Name}).")
    };

    /// <summary>The option as a 64-bit integer, or <see langword="null"/> when absent or NULL.</summary>
    /// <exception cref="FormatException">The value is not an integer.</exception>
    public long? GetInt64(string name) => Get(name) switch
    {
        null => null,
        var v when IsInteger(v) => Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture),
        string s when long.TryParse(s, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed) => parsed,
        var other => throw new FormatException($"Attach option '{name}' is not an integer (got {other.GetType().Name}).")
    };

    /// <summary>The option as a double, or <see langword="null"/> when absent or NULL.</summary>
    /// <exception cref="FormatException">The value is not numeric.</exception>
    public double? GetDouble(string name) => Get(name) switch
    {
        null => null,
        double d => d,
        float f => f,
        decimal m => (double)m,
        var v when IsInteger(v) => Convert.ToDouble(v, System.Globalization.CultureInfo.InvariantCulture),
        string s when double.TryParse(s, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var parsed) => parsed,
        var other => throw new FormatException($"Attach option '{name}' is not numeric (got {other.GetType().Name}).")
    };

    /// <summary>The option as bytes (a BLOB value), or <see langword="null"/> when absent or NULL.</summary>
    public byte[]? GetBinary(string name) => Get(name) switch
    {
        null => null,
        byte[] b => b,
        var other => throw new FormatException($"Attach option '{name}' is not a BLOB (got {other.GetType().Name}).")
    };

    private static bool IsInteger(object value) =>
        value is sbyte or byte or short or ushort or int or uint or long or ulong;

    /// <summary>Decodes the options of a <c>catalog_attach</c> request (the one-row Arrow batch in
    /// <see cref="CatalogAttachRequest.Options"/>). A request without options reads as empty.</summary>
    public static AttachOptions Read(CatalogAttachRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        return Read(request.Options);
    }

    /// <summary>Decodes raw <see cref="CatalogAttachRequest.Options"/> bytes; see <see cref="Read(CatalogAttachRequest)"/>.</summary>
    public static AttachOptions Read(byte[]? options)
    {
        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        if (options is not { Length: > 0 })
        {
            return new AttachOptions(values);
        }

        // Not disposed: an unconverted column below is handed out and must outlive this call.
        var batch = RecordBatchIpc.Read(options);
        for (var i = 0; i < batch.ColumnCount; i++)
        {
            var column = batch.Column(i);
            object? value;
            try
            {
                value = column.Length == 0 ? null : ScalarArgCodec.ReadScalar(column, 0);
            }
            catch (NotSupportedException)
            {
                // A type without a plain CLR scalar (timestamp, list, struct, ...): hand back the
                // one-row Arrow array itself.
                value = column;
            }

            values[batch.Schema.GetFieldByIndex(i).Name] = value;
        }

        return new AttachOptions(values);
    }

    /// <summary>Builds one encoded attach-option declaration for
    /// <see cref="CatalogInfo.AttachOptionSpecs"/> — the C# counterpart of vgi-python's
    /// <c>AttachOption</c> / vgi-typescript's <c>AttachOptionSpec</c>. DuckDB validates and casts
    /// the user's value to <paramref name="type"/> before the worker sees it.</summary>
    /// <param name="name">The option name used in the ATTACH statement. <c>vgi_attach_ticket</c> is reserved.</param>
    /// <param name="description">Human-readable description (discovery UIs, <c>vgi_catalogs()</c>).</param>
    /// <param name="type">The Arrow type DuckDB casts the value to.</param>
    /// <param name="defaultValue">The default used when the option is omitted, as a CLR value of
    /// <paramref name="type"/> (<see cref="string"/>, <see cref="bool"/>, <see cref="long"/>, ...) or a
    /// one-element <see cref="IArrowArray"/>; <see langword="null"/> for none.</param>
    /// <param name="required">The ATTACH must supply the option (mutually exclusive with a default).</param>
    /// <param name="secret">The option carries a credential: DuckDB redacts it from
    /// <c>duckdb_databases()</c>, logs and the result-cache key, and the worker keeps it encrypted in
    /// the attach data (see the remarks above); functions read it with <see cref="FromAttachData"/>.
    /// Never copy such a value into <see cref="AttachContext.ExtraOpaqueData"/>.</param>
    public static byte[] Spec(string name, string description, IArrowType type, object? defaultValue = null, bool required = false, bool secret = false)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (required && defaultValue is not null)
        {
            throw new ArgumentException($"Attach option '{name}' cannot be both required and have a default value.", nameof(defaultValue));
        }

        var array = defaultValue switch
        {
            null => null,
            IArrowArray a => a,
            _ => AnyArrayBuilder.Build(type, [defaultValue]),
        };
        return EmbeddedIpc.Encode(AttachOptionSpecBuilder.Build(name, description, type, array, required, secret));
    }
}
