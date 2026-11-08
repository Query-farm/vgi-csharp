using System.Text.Json;
using Apache.Arrow;
using QueryFarm.Vgi.Catalog;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Min/max pruning for protocol 3.2 scan planning (<see cref="ScanPlanningFilters.MayMatch"/>): a
/// three-valued "can this predicate be true for some row of the candidate" analysis over the VGI
/// Filter Encoding v2 expression tree. It answers <see langword="false"/> only when a predicate is
/// provably false for every row the statistics describe; anything it does not understand (an
/// expression shape, a value type, a column without statistics, incomparable values) is "may match".
/// </summary>
internal static class ScanPlanningEvaluator
{
    public static bool MayMatch(DecodedFilters filters, IReadOnlyDictionary<string, ColumnStatisticsInput> statistics)
    {
        foreach (var predicate in filters.Predicates)
        {
            try
            {
                if (!MayBeTrue(predicate.Expression, filters, statistics))
                {
                    return false;
                }
            }
            catch (Exception exception) when (exception is InvalidCastException or NotSupportedException
                or InvalidDataException or KeyNotFoundException or InvalidOperationException
                or ArgumentException or OverflowException)
            {
                // Anything this helper cannot reason about keeps the candidate.
            }
        }

        return true;
    }

    private static bool MayBeTrue(JsonElement expression, DecodedFilters filters,
        IReadOnlyDictionary<string, ColumnStatisticsInput> statistics)
    {
        switch (expression.GetProperty("node").GetString())
        {
            case "and":
                foreach (var child in expression.GetProperty("children").EnumerateArray())
                {
                    if (!MayBeTrue(child, filters, statistics))
                    {
                        return false;
                    }
                }

                return true;
            case "or":
                foreach (var child in expression.GetProperty("children").EnumerateArray())
                {
                    if (MayBeTrue(child, filters, statistics))
                    {
                        return true;
                    }
                }

                return false;
            case "comparison":
                return ComparisonMayBeTrue(expression, filters, statistics);
            case "in":
                return InMayBeTrue(expression, filters, statistics);
            case "is_null":
                if (Stats(expression.GetProperty("expression"), statistics) is not { } stats)
                {
                    return true;
                }

                return expression.GetProperty("negated").GetBoolean() ? stats.HasNotNull : stats.HasNull;
            default:
                return true;
        }
    }

    private static bool ComparisonMayBeTrue(JsonElement expression, DecodedFilters filters,
        IReadOnlyDictionary<string, ColumnStatisticsInput> statistics)
    {
        var op = expression.GetProperty("op").GetString();
        var left = expression.GetProperty("left");
        var right = expression.GetProperty("right");
        ColumnStatisticsInput? stats;
        JsonElement literal;
        if ((stats = Stats(left, statistics)) is not null && IsLiteral(right))
        {
            literal = right;
        }
        else if ((stats = Stats(right, statistics)) is not null && IsLiteral(left))
        {
            literal = left;
            op = Flip(op);
        }
        else
        {
            return true;
        }

        if (op == "distinct_from")
        {
            return true;
        }

        var value = ReadValue(filters.ValueArray(literal.GetProperty("value_ref").GetUInt64()), 0);
        if (value is null)
        {
            // `x = NULL` and friends are never true; `x IS NOT DISTINCT FROM NULL` is `x IS NULL`.
            return op == "not_distinct_from" && stats.HasNull;
        }

        if (!stats.HasNotNull)
        {
            // Entirely NULL: no value comparison can be true.
            return false;
        }

        var min = stats.Min;
        var max = stats.Max;
        return op switch
        {
            "eq" or "not_distinct_from" => !(min is not null && Cmp(value, min) < 0) && !(max is not null && Cmp(value, max) > 0),
            "ne" => !(min is not null && max is not null && Cmp(min, value) == 0 && Cmp(max, value) == 0),
            "lt" => !(min is not null && Cmp(min, value) >= 0),
            "le" => !(min is not null && Cmp(min, value) > 0),
            "gt" => !(max is not null && Cmp(max, value) <= 0),
            "ge" => !(max is not null && Cmp(max, value) < 0),
            _ => true,
        };
    }

    private static bool InMayBeTrue(JsonElement expression, DecodedFilters filters,
        IReadOnlyDictionary<string, ColumnStatisticsInput> statistics)
    {
        if (expression.GetProperty("negated").GetBoolean() ||
            Stats(expression.GetProperty("expression"), statistics) is not { } stats)
        {
            return true;
        }

        var set = expression.GetProperty("set");
        if (set.GetProperty("kind").GetString() != "literal")
        {
            return true;
        }

        if (!stats.HasNotNull)
        {
            return false;
        }

        if (filters.ValueArray(set.GetProperty("value_ref").GetUInt64()) is not ListArray list || list.Length < 1)
        {
            return true;
        }

        var start = list.ValueOffsets[0];
        var end = list.ValueOffsets[1];
        for (var i = start; i < end; i++)
        {
            var value = ReadValue(list.Values, i);
            if (value is null)
            {
                continue;
            }

            if (!(stats.Min is { } min && Cmp(value, min) < 0) && !(stats.Max is { } max && Cmp(value, max) > 0))
            {
                return true;
            }
        }

        // Every listed value lies outside [min, max] (or the list holds only NULLs).
        return false;
    }

    private static ColumnStatisticsInput? Stats(JsonElement expression,
        IReadOnlyDictionary<string, ColumnStatisticsInput> statistics)
    {
        if (expression.GetProperty("node").GetString() != "column_ref")
        {
            return null;
        }

        var name = expression.GetProperty("column_name").GetString()!;
        if (statistics.TryGetValue(name, out var stats))
        {
            return stats;
        }

        foreach (var (key, value) in statistics)
        {
            if (string.Equals(key, name, StringComparison.OrdinalIgnoreCase))
            {
                return value;
            }
        }

        return null;
    }

    private static bool IsLiteral(JsonElement expression) => expression.GetProperty("node").GetString() == "literal";

    /// <summary><see cref="ScalarArgCodec.ReadScalar"/> plus the temporal types min/max statistics
    /// commonly describe; <see cref="NotSupportedException"/> for anything else.</summary>
    internal static object? ReadValue(IArrowArray array, int index)
    {
        if (index >= array.Length || array.IsNull(index))
        {
            return null;
        }

        return array switch
        {
            Date32Array dates => dates.GetDateOnly(index),
            Date64Array dates => dates.GetDateOnly(index),
            TimestampArray timestamps => timestamps.GetTimestamp(index),
            _ => ScalarArgCodec.ReadScalar(array, index),
        };
    }

    private static int Cmp(object left, object right) => PushdownFilterEvaluator.Compare(left, right);

    private static string? Flip(string? op) => op switch
    {
        "lt" => "gt",
        "le" => "ge",
        "gt" => "lt",
        "ge" => "le",
        _ => op,
    };
}
