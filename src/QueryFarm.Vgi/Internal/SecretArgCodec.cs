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

    /// <summary>The first resolved secret whose own <c>type</c> field matches <paramref name="secretType"/>
    /// — falls back to a direct column-name lookup (the rare case a secret's DuckDB name literally
    /// equals its type) when no field-based match exists.</summary>
    public static IReadOnlyDictionary<string, IArrowArray>? FindByType(
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> resolved, string secretType)
    {
        foreach (var fields in resolved.Values)
        {
            if (string.Equals(FieldString(fields, "type"), secretType, StringComparison.Ordinal))
            {
                return fields;
            }
        }

        return resolved.GetValueOrDefault(secretType);
    }

    /// <summary>Every resolved secret whose own <c>type</c> field matches <paramref name="secretType"/>.</summary>
    public static IReadOnlyList<IReadOnlyDictionary<string, IArrowArray>> AllOfType(
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> resolved, string secretType) =>
        resolved.Values.Where(fields => string.Equals(FieldString(fields, "type"), secretType, StringComparison.Ordinal)).ToList();

    /// <summary>The secret (optionally narrowed to <paramref name="secretType"/>) whose <c>scope</c>
    /// field (newline-joined prefix list) is the longest prefix of <paramref name="path"/> — a secret
    /// with no/empty scope matches as a last-resort fallback. <see langword="null"/> only when there
    /// are no candidate secrets at all.</summary>
    public static IReadOnlyDictionary<string, IArrowArray>? ForScopeOfType(
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> resolved, string path, string? secretType)
    {
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
                if (prefix.Length > 0 && path.StartsWith(prefix, StringComparison.Ordinal) && prefix.Length > bestLength)
                {
                    bestLength = prefix.Length;
                    best = fields;
                }
            }
        }

        return best ?? fallback;
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
