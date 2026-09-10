using System.Collections;
using System.Globalization;
using System.Text.Json;

namespace QueryFarm.Vgi.Internal;

/// <summary>Exact row evaluator for the capability-gated core of VGI Filter Encoding v2.</summary>
public static class PushdownFilterEvaluator
{
    public static bool Matches(DecodedFilters? filters, IReadOnlyDictionary<string, object?> row)
    {
        if (filters is null)
        {
            return true;
        }

        foreach (var predicate in filters.Predicates)
        {
            if (!MatchesPredicate(filters, predicate, row))
            {
                return false;
            }
        }

        return true;
    }

    internal static bool MatchesPredicate(DecodedFilters filters, DecodedPredicate predicate,
        IReadOnlyDictionary<string, object?> row)
    {
        try
        {
            return Evaluate(predicate.Expression, filters, row) is true;
        }
        catch (Exception exception) when (predicate.Mode == "advisory" &&
            exception is NotSupportedException or InvalidCastException or OverflowException or DivideByZeroException)
        {
            return true;
        }
    }

    private static bool? Evaluate(JsonElement expression, DecodedFilters filters,
        IReadOnlyDictionary<string, object?> row)
    {
        var node = expression.GetProperty("node").GetString();
        return node switch
        {
            "comparison" => CompareExpression(expression, filters, row),
            "and" => And(expression.GetProperty("children").EnumerateArray().Select(c => Evaluate(c, filters, row))),
            "or" => Or(expression.GetProperty("children").EnumerateArray().Select(c => Evaluate(c, filters, row))),
            "not" => Not(Evaluate(expression.GetProperty("expression"), filters, row)),
            "is_null" => (Value(expression.GetProperty("expression"), filters, row) is null) ^
                         expression.GetProperty("negated").GetBoolean(),
            "in" => In(expression, filters, row),
            _ => Value(expression, filters, row) as bool?,
        };
    }

    private static object? Value(JsonElement expression, DecodedFilters filters,
        IReadOnlyDictionary<string, object?> row)
    {
        var node = expression.GetProperty("node").GetString();
        switch (node)
        {
            case "column_ref":
                var name = expression.GetProperty("column_name").GetString()!;
                if (!row.TryGetValue(name, out var column))
                {
                    throw new InvalidDataException($"Row has no filter column '{name}'.");
                }

                return column;
            case "field_ref":
                return Field(Value(expression.GetProperty("expression"), filters, row),
                    expression.GetProperty("field_name").GetString()!);
            case "literal":
                return filters.ValueRef(expression.GetProperty("value_ref").GetUInt64());
            case "comparison":
            case "and":
            case "or":
            case "not":
            case "is_null":
            case "in":
                return Evaluate(expression, filters, row);
            case "cast":
                // The DuckDB 1.5 producer admits only context-independent casts. Numeric CLR
                // normalization is sufficient for the exact comparison/arithmetic evaluator.
                return Numeric(Value(expression.GetProperty("expression"), filters, row));
            case "negate":
                var negated = Numeric(Value(expression.GetProperty("expression"), filters, row));
                return negated is null ? null : -negated.Value;
            case "arithmetic":
                return Arithmetic(expression, filters, row);
            case "call":
                return Call(expression, filters, row);
            default:
                throw new NotSupportedException($"Unsupported v2 expression node '{node}'.");
        }
    }

    private static object? Field(object? value, string name)
    {
        if (value is null)
        {
            return null;
        }

        if (value is IReadOnlyDictionary<string, object?> readOnly && readOnly.TryGetValue(name, out var result))
        {
            return result;
        }

        if (value is IDictionary dictionary && dictionary.Contains(name))
        {
            return dictionary[name];
        }

        var property = value.GetType().GetProperty(name);
        return property is not null
            ? property.GetValue(value)
            : throw new InvalidDataException($"Struct value has no field '{name}'.");
    }

    private static bool? CompareExpression(JsonElement expression, DecodedFilters filters,
        IReadOnlyDictionary<string, object?> row)
    {
        var left = Value(expression.GetProperty("left"), filters, row);
        var right = Value(expression.GetProperty("right"), filters, row);
        var op = expression.GetProperty("op").GetString();
        if (op == "distinct_from")
        {
            return left is null ? right is not null : right is null || Compare(left, right) != 0;
        }

        if (op == "not_distinct_from")
        {
            return left is null ? right is null : right is not null && Compare(left, right) == 0;
        }

        if (left is null || right is null)
        {
            return null;
        }

        var comparison = Compare(left, right);
        return op switch
        {
            "eq" => comparison == 0,
            "ne" => comparison != 0,
            "lt" => comparison < 0,
            "le" => comparison <= 0,
            "gt" => comparison > 0,
            "ge" => comparison >= 0,
            _ => throw new NotSupportedException($"Unsupported comparison '{op}'."),
        };
    }

    private static bool? In(JsonElement expression, DecodedFilters filters,
        IReadOnlyDictionary<string, object?> row)
    {
        var candidate = Value(expression.GetProperty("expression"), filters, row);
        if (candidate is null)
        {
            return null;
        }

        var set = expression.GetProperty("set");
        IReadOnlyList<object?> values = set.GetProperty("kind").GetString() switch
        {
            "literal" => filters.LiteralSet(set.GetProperty("value_ref").GetUInt64()),
            "external" => filters.ExternalSet(set.GetProperty("batch_index").GetInt32(),
                set.GetProperty("column_index").GetInt32(), set.GetProperty("column_name").GetString()!),
            var kind => throw new NotSupportedException($"Unsupported IN set '{kind}'."),
        };
        var foundNull = false;
        foreach (var item in values)
        {
            if (item is null)
            {
                foundNull = true;
            }
            else if (Compare(candidate, item) == 0)
            {
                return !expression.GetProperty("negated").GetBoolean();
            }
        }

        bool? result = foundNull ? null : false;
        return expression.GetProperty("negated").GetBoolean() ? Not(result) : result;
    }

    private static decimal? Arithmetic(JsonElement expression, DecodedFilters filters,
        IReadOnlyDictionary<string, object?> row)
    {
        var left = Numeric(Value(expression.GetProperty("left"), filters, row));
        var right = Numeric(Value(expression.GetProperty("right"), filters, row));
        if (left is null || right is null)
        {
            return null;
        }

        return expression.GetProperty("op").GetString() switch
        {
            "add" => left + right,
            "subtract" => left - right,
            "multiply" => left * right,
            "divide" => left / right,
            "modulo" => left % right,
            var op => throw new NotSupportedException($"Unsupported arithmetic '{op}'."),
        };
    }

    private static bool? Call(JsonElement expression, DecodedFilters filters,
        IReadOnlyDictionary<string, object?> row)
    {
        var arguments = expression.GetProperty("arguments").EnumerateArray()
            .Select(argument => Value(argument, filters, row)).ToList();
        if (arguments.Any(argument => argument is null))
        {
            return null;
        }

        var functionElement = expression.GetProperty("function");
        if (functionElement.ValueKind == JsonValueKind.Object)
        {
            var ns = functionElement.GetProperty("namespace").GetString();
            var name = functionElement.GetProperty("name").GetString();
            var version = functionElement.GetProperty("version").GetUInt64();
            if (ns == "duckdb.spatial" && name == "intersects_extent" && version == 1 &&
                arguments is [byte[] left, byte[] right])
            {
                return ExpressionFilterEvaluator.SpatialIntersectsExtent(left, right);
            }

            throw new NotSupportedException($"Unsupported extension filter function '{ns}/{name}@{version}'.");
        }

        return functionElement.GetString() switch
        {
            "starts_with" when arguments is [string value, string prefix] => value.StartsWith(prefix, StringComparison.Ordinal),
            "ends_with" when arguments is [string value, string suffix] => value.EndsWith(suffix, StringComparison.Ordinal),
            "contains" when arguments is [string value, string needle] => value.Contains(needle, StringComparison.Ordinal),
            "list_contains" when arguments is [IEnumerable values, object needle] =>
                values.Cast<object?>().Any(item => item is not null && Compare(item, needle) == 0),
            var function => throw new NotSupportedException($"Unsupported standard filter function '{function}'."),
        };
    }

    private static bool? And(IEnumerable<bool?> values)
    {
        var hasNull = false;
        foreach (var value in values)
        {
            if (value is false)
            {
                return false;
            }

            hasNull |= value is null;
        }

        return hasNull ? null : true;
    }

    private static bool? Or(IEnumerable<bool?> values)
    {
        var hasNull = false;
        foreach (var value in values)
        {
            if (value is true)
            {
                return true;
            }

            hasNull |= value is null;
        }

        return hasNull ? null : false;
    }

    private static bool? Not(bool? value) => value is null ? null : !value.Value;

    private static int Compare(object left, object right)
    {
        if (left is string leftString && right is string rightString)
        {
            return string.CompareOrdinal(leftString, rightString);
        }

        if (left is byte[] leftBytes && right is byte[] rightBytes)
        {
            return leftBytes.AsSpan().SequenceCompareTo(rightBytes);
        }

        if (IsNumeric(left) && IsNumeric(right) && (left is float or double || right is float or double))
        {
            var leftFloat = Convert.ToDouble(left, CultureInfo.InvariantCulture);
            var rightFloat = Convert.ToDouble(right, CultureInfo.InvariantCulture);
            if (double.IsNaN(leftFloat))
            {
                return double.IsNaN(rightFloat) ? 0 : 1;
            }

            if (double.IsNaN(rightFloat))
            {
                return -1;
            }

            return leftFloat.CompareTo(rightFloat);
        }

        var leftNumber = Numeric(left);
        var rightNumber = Numeric(right);
        if (leftNumber is not null && rightNumber is not null)
        {
            return leftNumber.Value.CompareTo(rightNumber.Value);
        }

        if (left.GetType() == right.GetType() && left is IComparable comparable)
        {
            return comparable.CompareTo(right);
        }

        return Equals(left, right) ? 0 : throw new InvalidCastException("Filter values are not comparable.");
    }

    private static decimal? Numeric(object? value)
    {
        if (value is null)
        {
            return null;
        }

        return IsNumeric(value)
            ? Convert.ToDecimal(value, CultureInfo.InvariantCulture)
            : null;
    }

    private static bool IsNumeric(object value) =>
        value is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal;
}
