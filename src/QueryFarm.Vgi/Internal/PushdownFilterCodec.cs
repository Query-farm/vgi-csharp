using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Decodes <c>InitRequest.PushdownFilters</c> — an embedded Arrow IPC stream (built by the C++
/// extension's <c>VgiSerializeFilters</c>) whose single-row batch carries a JSON filter-tree string
/// in its first column (<c>filter_spec</c>, metadata <c>vgi_filter_version=1</c>) plus one sibling
/// Arrow column per referenced constant literal (<c>_val_0</c>, <c>_val_1</c>, ...), so filter
/// literals keep full DuckDB type fidelity instead of being JSON-stringified.
///
/// This is a best-effort worker-side decoder (not itself part of the wire spec) used by
/// pushdown-introspection fixtures (<c>filter_echo</c>, <c>dynamic_filter</c>, <c>value_prune</c>,
/// ...) — a fixture that just wants to ECHO/inspect what DuckDB pushed down, not necessarily
/// re-implement full filter evaluation.
/// </summary>
public static class PushdownFilterCodec
{
    public static DecodedFilters? Decode(byte[]? pushdownFilters, IReadOnlyList<byte[]>? joinKeys = null)
    {
        var joinKeyColumns = DecodeJoinKeys(joinKeys);

        if (pushdownFilters is null || pushdownFilters.Length == 0)
        {
            return joinKeyColumns.Count == 0 ? null : new DecodedFilters(default, [], [], joinKeyColumns);
        }

        using var stream = new MemoryStream(pushdownFilters);
        using var reader = new ArrowStreamReader(stream);
        var batch = reader.ReadNextRecordBatch();
        if (batch is null || batch.Schema.FieldsList.Count == 0)
        {
            return null;
        }

        if (batch.Column(0) is not StringArray specColumn || specColumn.Length == 0 || specColumn.IsNull(0))
        {
            return null;
        }

        var specJson = specColumn.GetString(0);
        using var doc = JsonDocument.Parse(specJson);

        var values = new List<IArrowArray>();
        var valueFields = new List<Field>();
        for (var i = 1; i < batch.Schema.FieldsList.Count; i++)
        {
            values.Add(batch.Column(i));
            valueFields.Add(batch.Schema.GetFieldByIndex(i));
        }

        return new DecodedFilters(doc.RootElement.Clone(), values, valueFields, joinKeyColumns);
    }

    private static IReadOnlyDictionary<string, IArrowArray> DecodeJoinKeys(IReadOnlyList<byte[]>? joinKeys)
    {
        if (joinKeys is null || joinKeys.Count == 0)
        {
            return new Dictionary<string, IArrowArray>(StringComparer.Ordinal);
        }

        var result = new Dictionary<string, IArrowArray>(StringComparer.Ordinal);
        foreach (var bytes in joinKeys)
        {
            using var stream = new MemoryStream(bytes);
            using var reader = new ArrowStreamReader(stream);
            var batch = reader.ReadNextRecordBatch();
            if (batch is null || batch.Schema.FieldsList.Count == 0)
            {
                continue;
            }

            result[batch.Schema.GetFieldByIndex(0).Name] = batch.Column(0);
        }

        return result;
    }
}

/// <summary>A decoded filter-spec tree (a JSON array of filter nodes, per <see cref="PushdownFilterCodec"/>'s
/// doc comment) plus its sibling constant-value columns (referenced from the tree by index) and
/// its sibling join-key/IN-list value columns (referenced from the tree by <c>keys_column</c> name).</summary>
public sealed class DecodedFilters(JsonElement root, IReadOnlyList<IArrowArray> values, IReadOnlyList<Field> valueFields, IReadOnlyDictionary<string, IArrowArray> joinKeys)
{
    public JsonElement Root { get; } = root;

    public object? ValueRef(int index) =>
        index >= 0 && index < values.Count ? ScalarArgCodec.ReadScalar(values[index]) : null;

    /// <summary>The Arrow field the C++ extension built for a <c>"constant"</c>/<c>"expression"</c>
    /// node's <c>value_ref</c> — carries <c>ARROW:extension:name</c> metadata when the underlying
    /// DuckDB value has an Arrow extension type (e.g. the spatial extension's <c>GEOMETRY</c> type
    /// serializes as <c>geoarrow.wkb</c>, per <c>ArrowTypeExtensionData::GetExtensionTypes</c> in
    /// <c>vgi_table_function_impl.cpp</c>'s <c>VgiSerializeFilters</c>). <see langword="null"/> when
    /// out of range. See <see cref="ExpressionFilterEvaluator"/>, which uses this to decide whether
    /// a constant needs <c>ST_GeomFromWKB(...)</c> wrapping to compare against a WKB-tagged column.</summary>
    public Field? ValueField(int index) => index >= 0 && index < valueFields.Count ? valueFields[index] : null;

    /// <summary>The candidate value set for a <c>"join_keys"</c> filter node's <c>keys_column</c>.</summary>
    public IReadOnlyList<object?> JoinKeyValues(string keysColumn)
    {
        if (!joinKeys.TryGetValue(keysColumn, out var array))
        {
            return [];
        }

        var result = new List<object?>(array.Length);
        for (var i = 0; i < array.Length; i++)
        {
            result.Add(ScalarArgCodec.ReadScalar(array, i));
        }

        return result;
    }
}
