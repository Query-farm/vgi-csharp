using System.Text.Json;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Evaluates a <see cref="DecodedFilters"/> tree against one candidate row's column values —
/// discovered empirically (against the real C++ extension, via <c>filter_echo.test</c>) that a
/// function declaring <see cref="Table.ITableFunction.FilterPushdown"/> is trusted UNCONDITIONALLY:
/// DuckDB does not install its own residual post-scan filter for a pushdown-capable function
/// regardless of <see cref="Table.ITableFunction.FiltersExactlyApplied"/>, so a function that
/// advertises filter pushdown MUST actually apply the pushed filters itself or rows will leak
/// through unfiltered.
/// </summary>
public static class PushdownFilterEvaluator
{
    public static bool Matches(DecodedFilters? filters, IReadOnlyDictionary<string, object?> row)
    {
        if (filters is null || filters.Root.ValueKind != JsonValueKind.Array)
        {
            return true;
        }

        foreach (var node in filters.Root.EnumerateArray())
        {
            if (!Eval(node, filters, row))
            {
                return false;
            }
        }

        return true;
    }

    private static bool Eval(JsonElement node, DecodedFilters filters, IReadOnlyDictionary<string, object?> row)
    {
        var type = node.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : null;
        switch (type)
        {
            case "constant":
                return EvalConstant(node, filters, row);
            case "is_null":
                return !row.TryGetValue(ColumnName(node), out var v1) || v1 is null;
            case "is_not_null":
                return row.TryGetValue(ColumnName(node), out var v2) && v2 is not null;
            case "and":
                return Children(node).All(c => Eval(c, filters, row));
            case "or":
                return Children(node).Any(c => Eval(c, filters, row));
            case "in":
            case "in_list":
                return EvalIn(node, filters, row);
            case "join_keys":
                return EvalJoinKeys(node, filters, row);
            default:
                // Unknown node shape — fail open (don't drop rows we don't understand how to check;
                // DuckDB never installs its own residual filter for a pushdown-capable function, so
                // failing closed here would silently under-return instead of over-return).
                return true;
        }
    }

    private static bool EvalConstant(JsonElement node, DecodedFilters filters, IReadOnlyDictionary<string, object?> row)
    {
        var op = node.TryGetProperty("op", out var opProp) ? opProp.GetString() : "eq";
        var target = node.TryGetProperty("value_ref", out var vr) ? filters.ValueRef(vr.GetInt32()) : null;
        row.TryGetValue(ColumnName(node), out var actual);

        var cmp = Compare(actual, target);
        return op switch
        {
            "eq" => cmp == 0,
            "ne" => cmp != 0,
            "gt" => cmp > 0,
            "ge" => cmp >= 0,
            "lt" => cmp < 0,
            "le" => cmp <= 0,
            _ => true,
        };
    }

    private static bool EvalIn(JsonElement node, DecodedFilters filters, IReadOnlyDictionary<string, object?> row)
    {
        row.TryGetValue(ColumnName(node), out var actual);
        IEnumerable<JsonElement> refs = node.TryGetProperty("value_refs", out var r1) && r1.ValueKind == JsonValueKind.Array
            ? r1.EnumerateArray()
            : node.TryGetProperty("values", out var r2) && r2.ValueKind == JsonValueKind.Array
                ? r2.EnumerateArray()
                : [];

        return refs.Any(r => Compare(actual, filters.ValueRef(r.GetInt32())) == 0);
    }

    private static bool EvalJoinKeys(JsonElement node, DecodedFilters filters, IReadOnlyDictionary<string, object?> row)
    {
        row.TryGetValue(ColumnName(node), out var actual);
        var keysColumn = node.TryGetProperty("keys_column", out var kc) ? kc.GetString() ?? "" : "";
        return filters.JoinKeyValues(keysColumn).Any(v => Compare(actual, v) == 0);
    }

    private static IEnumerable<JsonElement> Children(JsonElement node) =>
        node.TryGetProperty("children", out var children) && children.ValueKind == JsonValueKind.Array
            ? children.EnumerateArray()
            : [];

    private static string ColumnName(JsonElement node) =>
        node.TryGetProperty("column_name", out var name) ? name.GetString() ?? "" : "";

    private static int Compare(object? actual, object? target)
    {
        if (actual is null || target is null)
        {
            return actual is null && target is null ? 0 : -2;
        }

        if (actual is string sa && target is string sb)
        {
            return string.CompareOrdinal(sa, sb);
        }

        try
        {
            var da = Convert.ToDouble(actual);
            var db = Convert.ToDouble(target);
            return da.CompareTo(db);
        }
        catch
        {
            return Equals(actual, target) ? 0 : -2;
        }
    }
}
