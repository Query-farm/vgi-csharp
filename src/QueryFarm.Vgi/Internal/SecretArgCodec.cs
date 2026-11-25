using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Decodes <see cref="Protocol.BindRequest.Secrets"/>/<see cref="Protocol.AggregateBindRequest.Secrets"/>
/// (resolved DuckDB secrets, pre-resolved by the C++ extension from a function's declared/dynamically-
/// requested secret requirements) plus the by-type/by-scope selection helpers every consumer needs.
///
/// Wire shape (<c>vgi_arrow_utils.cpp</c>'s <c>BuildSecretsBatch</c>): one embedded-IPC batch, one row,
/// one column PER RESOLVED SECRET — the column is named by the secret's own DuckDB NAME (its <c>CREATE
/// SECRET &lt;name&gt;</c> identifier; falls back to the requested secret TYPE only for the rare
/// anonymous/nameless secret), so several secrets of the same type (e.g. one per S3 bucket) all
/// survive under distinct keys. Each column's type is <c>struct&lt;type: utf8, provider: utf8, name:
/// utf8, ...custom key/value fields..., scope: utf8&gt;</c> — <c>type</c>/<c>scope</c> are what the
/// by-type/by-scope selectors below actually filter on, NOT the outer column name.
/// </summary>
public static class SecretArgCodec
{
    /// <summary>Decodes <c>Secrets</c> into secret-DB-name → (field-name → single-element value
    /// column). Returns an empty dictionary when <paramref name="secrets"/> is <c>null</c>/empty (no
    /// secrets were resolved — nothing was declared/requested, or nothing matched).</summary>
    public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> Decode(byte[]? secrets)
    {
        var result = new Dictionary<string, IReadOnlyDictionary<string, IArrowArray>>(StringComparer.Ordinal);
        if (secrets is null || secrets.Length == 0)
        {
            return result;
        }

        RecordBatch? batch;
        try
        {
            using var stream = new MemoryStream(secrets);
            using var reader = new ArrowStreamReader(stream);
            batch = reader.ReadNextRecordBatch();
        }
        catch (ArgumentNullException)
        {
            // Same vendored Apache.Arrow IPC reader crash guard as ScalarArgCodec.DecodeConstStruct/
            // TableArgCodec.Decode — a zero-child struct column (a resolved secret with no fields at
            // all, which shouldn't occur in practice but costs nothing to guard against) crashes the
            // reader rather than parsing as empty. Treat it as "no secrets".
            return result;
        }

        if (batch is null)
        {
            return result;
        }

        for (var i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            if (batch.Column(i) is not StructArray secretStruct || secretStruct.IsNull(0))
            {
                continue;
            }

            var structType = (Apache.Arrow.Types.StructType)secretStruct.Data.DataType;
            var fields = new Dictionary<string, IArrowArray>(StringComparer.Ordinal);
            for (var f = 0; f < structType.Fields.Count; f++)
            {
                fields[structType.Fields[f].Name] = secretStruct.Fields[f];
            }

            result[batch.Schema.GetFieldByIndex(i).Name] = fields;
        }

        return result;
    }

    /// <summary>The resolved secret of type <paramref name="secretType"/>: one with an empty scope if
    /// there is one, else the first of that type. The match is on each secret's own <c>type</c> field,
    /// never on the outer column name — that column is the secret's DuckDB NAME, so an unnamed
    /// <c>CREATE SECRET (TYPE t, ...)</c> arrives as <c>__default_t</c> and a named one under its
    /// name. Only when no secret has the type does this fall back to a column literally named
    /// <paramref name="secretType"/> (clients that key the batch by type and send bare key/value
    /// structs without a <c>type</c> field).</summary>
    public static IReadOnlyDictionary<string, IArrowArray>? FindByType(
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> resolved, string secretType) =>
        Lookup(resolved, secretType);

    /// <summary>The resolved secret a <c>(secretType, name, scope)</c> request refers to — the same
    /// rule the client uses to resolve that request, so a worker finds what it asked for among
    /// everything that was delivered (vgi-python <c>ResolvedSecrets.lookup</c>):
    /// <list type="bullet">
    /// <item><paramref name="name"/> — the secret with that name, and only if its <c>type</c> is
    /// <paramref name="secretType"/> (or it carries no <c>type</c> field). <paramref name="scope"/>
    /// is ignored, as on the client.</item>
    /// <item><paramref name="scope"/> — the secret of that type whose scope is the longest prefix
    /// of <paramref name="scope"/> (matched by <see cref="ScopeMatches"/>, so an authority-only scope
    /// stops at the authority boundary), else one of that type with an empty scope.</item>
    /// <item>neither — a secret of that type with an empty scope, else the first of that type.</item>
    /// </list>
    /// When no secret has the requested type (and no <paramref name="name"/> was given), a secret
    /// whose column is named <paramref name="secretType"/> is returned.</summary>
    public static IReadOnlyDictionary<string, IArrowArray>? Lookup(
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> resolved,
        string secretType,
        string? name = null,
        string? scope = null)
    {
        ArgumentNullException.ThrowIfNull(resolved);
        ArgumentNullException.ThrowIfNull(secretType);
        if (!string.IsNullOrEmpty(name))
        {
            if (!resolved.TryGetValue(name, out var named))
            {
                return null;
            }

            var namedType = FieldString(named, "type");
            return string.IsNullOrEmpty(namedType) || string.Equals(namedType, secretType, StringComparison.Ordinal) ? named : null;
        }

        var found = !string.IsNullOrEmpty(scope)
            ? ForScopeOfType(resolved, scope, secretType)
            : ForScopeOfType(resolved, "", secretType) ?? AllOfType(resolved, secretType).FirstOrDefault();
        return found ?? resolved.GetValueOrDefault(secretType);
    }

    /// <summary>Every resolved secret whose own <c>type</c> field matches <paramref name="secretType"/>.</summary>
    public static IReadOnlyList<IReadOnlyDictionary<string, IArrowArray>> AllOfType(
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> resolved, string secretType) =>
        resolved.Values.Where(fields => string.Equals(FieldString(fields, "type"), secretType, StringComparison.Ordinal)).ToList();

    /// <summary>The secret (optionally narrowed to <paramref name="secretType"/>) whose <c>scope</c>
    /// field (newline-joined prefix list) is the longest prefix of <paramref name="path"/> — failing
    /// that, the first secret with no/empty scope; failing that, <see langword="null"/> (a scoped
    /// secret whose prefixes do not match <paramref name="path"/> is never returned). Prefixes match
    /// exactly as DuckDB matches them (<see cref="ScopeMatches"/>): an authority-only scope such as
    /// <c>snmp://10.0.0.1</c> covers that host and paths under it, never <c>snmp://10.0.0.15</c> or
    /// <c>snmp://10.0.0.1:161</c>.</summary>
    public static IReadOnlyDictionary<string, IArrowArray>? ForScopeOfType(
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> resolved, string path, string? secretType)
    {
        ArgumentNullException.ThrowIfNull(resolved);
        ArgumentNullException.ThrowIfNull(path);
        IReadOnlyDictionary<string, IArrowArray>? best = null;
        var bestLength = -1;
        IReadOnlyDictionary<string, IArrowArray>? fallback = null;

        foreach (var fields in resolved.Values)
        {
            if (secretType is not null && !string.Equals(FieldString(fields, "type"), secretType, StringComparison.Ordinal))
            {
                continue;
            }

            var scope = FieldString(fields, "scope");
            if (string.IsNullOrEmpty(scope))
            {
                fallback ??= fields;
                continue;
            }

            foreach (var prefix in scope.Split('\n'))
            {
                // All matching prefixes are prefixes of the same path, so they are nested and
                // ordering them by UTF-16 length agrees with DuckDB's ordering by UTF-8 byte length.
                if (prefix.Length > 0 && ScopeMatches(prefix, path) && prefix.Length > bestLength)
                {
                    bestLength = prefix.Length;
                    best = fields;
                }
            }
        }

        return best ?? fallback;
    }

    /// <summary>Whether a secret scope <paramref name="prefix"/> covers <paramref name="path"/>,
    /// exactly as DuckDB decides (<c>BaseSecret::MatchScore</c> as of the Haybarn 2.0.1 engine,
    /// upstream duckdb#28684).
    ///
    /// A scope matches when it is a string prefix of <paramref name="path"/>, except that an
    /// authority-only URI scope (<c>scheme://authority</c> with nothing after the authority) has an
    /// implicit trailing <c>/</c>: <c>snmp://10.0.0.1</c> covers <c>snmp://10.0.0.1</c> and
    /// <c>snmp://10.0.0.1/x</c> but not <c>snmp://10.0.0.15</c> or <c>snmp://10.0.0.1:161</c>, and
    /// <c>s3://bucket</c> no longer covers <c>s3://bucket-evil/</c>. Matching DuckDB keeps a worker
    /// from choosing a secret DuckDB itself would not resolve for that path. An empty
    /// <paramref name="prefix"/> is not a scope (returns <see langword="false"/>); callers treat
    /// unscoped secrets as the fallback separately.
    ///
    /// DuckDB indexes UTF-8 bytes; this indexes UTF-16 code units. They agree: every delimiter the
    /// rule inspects (<c>:</c>, <c>/</c>, <c>\</c>, <c>?</c>, <c>#</c>) is ASCII, one code unit in
    /// UTF-16 and one byte in UTF-8, and neither encoding uses those values inside a multi-unit
    /// character. So an ordinal prefix test of two well-formed strings gives the same answer in
    /// either encoding, the "authority runs to the end of the prefix" test is the same (it only asks
    /// whether a delimiter follows the authority), and the character right after the prefix is
    /// <c>/</c> in one encoding exactly when it is in the other.</summary>
    public static bool ScopeMatches(string prefix, string path)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(path);
        if (prefix.Length == 0 || !path.StartsWith(prefix, StringComparison.Ordinal))
        {
            return false;
        }

        var authority = UriAuthority(prefix);
        var authorityOnly = authority.Length > 0 &&
            prefix.Length == prefix.IndexOf("://", StringComparison.Ordinal) + 3 + authority.Length;
        if (!authorityOnly)
        {
            return true;
        }

        return path.Length == prefix.Length || path[prefix.Length] == '/';
    }

    private static readonly char[] Slashes = ['/', '\\'];
    private static readonly char[] AuthorityEnds = ['/', '\\', '?', '#'];

    /// <summary>The URI authority of <paramref name="text"/>, or <c>""</c>; mirrors DuckDB's
    /// <c>StringUtil::GetURIAuthority</c>. <paramref name="text"/> counts as a URI only if it
    /// contains <c>://</c> after a non-empty scheme and its first <c>/</c> or <c>\</c> is the one
    /// that starts that <c>://</c>. The authority runs from after <c>://</c> to the first <c>/</c>,
    /// <c>\</c>, <c>?</c> or <c>#</c> (or the end).</summary>
    private static string UriAuthority(string text)
    {
        var schemeEnd = text.IndexOf("://", StringComparison.Ordinal);
        if (schemeEnd <= 0 || text.IndexOfAny(Slashes) != schemeEnd + 1)
        {
            return "";
        }

        var start = schemeEnd + 3;
        var end = text.IndexOfAny(AuthorityEnds, start);
        return end < 0 ? text[start..] : text[start..end];
    }

    /// <summary>Reads one field of a resolved secret as a rendered string (<c>null</c> when the
    /// secret or the field itself is absent/SQL-NULL).</summary>
    public static string? FieldString(IReadOnlyDictionary<string, IArrowArray>? secret, string field)
    {
        if (secret is null || !secret.TryGetValue(field, out var array))
        {
            return null;
        }

        return ScalarArgCodec.ReadScalar(array)?.ToString();
    }

    /// <summary>Reads one field of a resolved secret as a boxed scalar value (see
    /// <see cref="ScalarArgCodec.ReadScalar"/>) — <c>null</c> when the secret or the field is
    /// absent/SQL-NULL.</summary>
    public static object? FieldValue(IReadOnlyDictionary<string, IArrowArray>? secret, string field) =>
        secret is not null && secret.TryGetValue(field, out var array) ? ScalarArgCodec.ReadScalar(array) : null;
}
