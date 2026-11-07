using System.Collections;
using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Arguments;

/// <summary>
/// One resolved DuckDB secret (<c>CREATE SECRET name (TYPE ..., KEY 'value', ...)</c>). Every
/// field is a single-row Arrow array; read them with <see cref="GetString"/>/<see cref="GetValue"/>.
/// The secret is also an <see cref="IReadOnlyDictionary{TKey,TValue}"/> of field name → array, so it
/// can be passed wherever the older raw dictionary shape is expected.
/// </summary>
public sealed class Secret : IReadOnlyDictionary<string, IArrowArray>
{
    private readonly IReadOnlyDictionary<string, IArrowArray> _fields;

    /// <summary>Wraps a resolved secret's fields. <paramref name="name"/> is the secret's DuckDB name.</summary>
    public Secret(string name, IReadOnlyDictionary<string, IArrowArray> fields)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(fields);
        Name = name;
        _fields = fields;
    }

    /// <summary>The secret's DuckDB name (its <c>CREATE SECRET &lt;name&gt;</c> identifier).</summary>
    public string Name { get; }

    /// <summary>The secret's <c>type</c> field (e.g. <c>s3</c>), or <see langword="null"/>.</summary>
    public string? Type => GetString("type");

    /// <summary>The secret's <c>provider</c> field (e.g. <c>config</c>), or <see langword="null"/>.</summary>
    public string? Provider => GetString("provider");

    /// <summary>The secret's <c>scope</c> field — newline-joined path prefixes; <see langword="null"/>
    /// or empty when unscoped.</summary>
    public string? Scope => GetString("scope");

    /// <summary>Field <paramref name="field"/> rendered as a string; <see langword="null"/> when the
    /// field is absent or SQL NULL.</summary>
    public string? GetString(string field) => SecretArgCodec.FieldString(_fields, field);

    /// <summary>Field <paramref name="field"/> as a boxed CLR value; <see langword="null"/> when the
    /// field is absent or SQL NULL.</summary>
    public object? GetValue(string field) => SecretArgCodec.FieldValue(_fields, field);

    /// <summary>Field <paramref name="field"/> as a string, throwing
    /// <see cref="VgiInvalidArgumentException"/> when it is absent or NULL.</summary>
    public string GetRequiredString(string field) =>
        GetString(field) ?? throw new VgiInvalidArgumentException($"Secret '{Name}' has no value for required field '{field}'.");

    public IArrowArray this[string key] => _fields[key];

    public IEnumerable<string> Keys => _fields.Keys;

    public IEnumerable<IArrowArray> Values => _fields.Values;

    public int Count => _fields.Count;

    public bool ContainsKey(string key) => _fields.ContainsKey(key);

    public bool TryGetValue(string key, [MaybeNullWhen(false)] out IArrowArray value) => _fields.TryGetValue(key, out value);

    public IEnumerator<KeyValuePair<string, IArrowArray>> GetEnumerator() => _fields.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// The secrets a call can see. At bind time (<see cref="Table.TableBindParams.SecretValues"/>,
/// <see cref="TableInOut.TableInOutBindParams.SecretValues"/>) <see cref="Get"/> also drives
/// DuckDB's two-phase secret lookup: on the first bind attempt a lookup that is not statically
/// declared returns <see langword="null"/> and registers a request, the framework asks DuckDB to
/// resolve it, and the bind is retried with the answer. Everywhere else (init, process, scalar,
/// aggregate) the view is read-only: <see cref="Get"/> just searches what was resolved.
/// </summary>
/// <example>
/// <code>
/// public void Bind(TableBindParams p)
/// {
///     var secret = p.SecretValues.Get("s3", scope: p.Arguments.Get&lt;string&gt;(0));
///     if (secret is null) return;               // first attempt: DuckDB resolves, bind retries
///     var key = secret.GetRequiredString("key_id");
/// }
/// </code>
/// </example>
public sealed class SecretValues
{
    private readonly SecretsAccessor _accessor;
    private IReadOnlyDictionary<string, Secret>? _resolved;

    internal SecretValues(SecretsAccessor accessor) => _accessor = accessor;

    /// <summary>A read-only view of already-resolved secrets from serialized secret bytes (e.g.
    /// <see cref="Table.TableInitParams.Secrets"/>). <see cref="Get"/> on this view never registers
    /// a lookup.</summary>
    public static SecretValues FromResolved(byte[]? secretsBytes) => new(new SecretsAccessor(secretsBytes, isRetry: true));

    /// <summary>An empty, read-only view.</summary>
    public static SecretValues Empty { get; } = FromResolved(null);

    /// <summary>Every resolved secret, keyed by its DuckDB name.</summary>
    public IReadOnlyDictionary<string, Secret> Resolved =>
        _resolved ??= _accessor.Resolved.ToDictionary(kv => kv.Key, kv => new Secret(kv.Key, kv.Value), StringComparer.Ordinal);

    /// <summary>Number of resolved secrets.</summary>
    public int Count => _accessor.Resolved.Count;

    /// <summary>Looks a secret up by type, optionally narrowed by <paramref name="scope"/> (a path
    /// matched by longest scope prefix) and/or an exact <paramref name="name"/>. At bind time a
    /// first-attempt call registers a two-phase lookup and returns <see langword="null"/>; see the
    /// class remarks.</summary>
    public Secret? Get(string secretType, string? scope = null, string? name = null)
    {
        var fields = _accessor.Get(secretType, scope, name);
        return fields is null ? null : Wrap(fields);
    }

    /// <summary>The first resolved secret whose <c>type</c> is <paramref name="secretType"/>, without
    /// registering a lookup. Use this for secrets the function declared statically
    /// (<c>RequiredSecrets</c>).</summary>
    public Secret? FindByType(string secretType) =>
        SecretArgCodec.FindByType(_accessor.Resolved, secretType) is { } f ? Wrap(f) : null;

    /// <summary>Every resolved secret whose <c>type</c> is <paramref name="secretType"/>.</summary>
    public IReadOnlyList<Secret> AllOfType(string secretType) =>
        SecretArgCodec.AllOfType(_accessor.Resolved, secretType).Select(Wrap).ToList();

    /// <summary>The resolved secret (optionally of <paramref name="secretType"/>) whose scope is the
    /// longest prefix of <paramref name="path"/>; an unscoped secret is the fallback. Never
    /// registers a lookup.</summary>
    public Secret? ForScope(string path, string? secretType = null) =>
        SecretArgCodec.ForScopeOfType(_accessor.Resolved, path, secretType) is { } f ? Wrap(f) : null;

    private Secret Wrap(IReadOnlyDictionary<string, IArrowArray> fields)
    {
        foreach (var (name, secret) in Resolved)
        {
            if (ReferenceEquals(_accessor.Resolved[name], fields))
            {
                return secret;
            }
        }

        return new Secret(SecretArgCodec.FieldString(fields, "name") ?? "", fields);
    }
}
