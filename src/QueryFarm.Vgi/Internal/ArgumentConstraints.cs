using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Apache.Arrow;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Bind-time enforcement of the per-argument constraint metadata a function declares on its
/// arguments schema — <c>vgi_range</c> (interval notation from ge/gt/le/lt), <c>vgi_choices</c>
/// (JSON array) and <c>vgi_pattern</c> (regex, unanchored search like JSON Schema / ECMA
/// <c>pattern</c>). The same metadata is what <c>vgi_function_arguments()</c> surfaces, so a
/// declared constraint is always also an enforced one. SQL NULL values skip every check.
/// </summary>
internal static class ArgumentConstraints
{
    /// <summary>How positional values map onto declared fields.</summary>
    public enum Positions
    {
        /// <summary>Scalar/aggregate: positions count <c>vgi_const</c> fields only.</summary>
        ConstOnly,

        /// <summary>Table-family: positions count every non-named, non-TABLE field.</summary>
        AllPositional,

        /// <summary>Only named fields are checked (e.g. a blended table-in-out whose positional
        /// arguments are per-row input columns).</summary>
        NamedOnly,
    }

    private sealed record Constraint(
        string Name, double? Lower, bool LowerInclusive, double? Upper, bool UpperInclusive, string? Range,
        IReadOnlyList<JsonElement>? Choices, string? ChoicesJson, Regex? Pattern, string? PatternText)
    {
        public bool HasAny => Range is not null || Choices is not null || Pattern is not null;
    }

    private sealed record Plan(
        IReadOnlyList<(int Position, bool Varargs, Constraint Constraint)> Positional,
        IReadOnlyList<Constraint> Named)
    {
        public bool IsEmpty => Positional.Count == 0 && Named.Count == 0;
    }

    private static readonly ConditionalWeakTable<Schema, ConcurrentDictionary<Positions, Plan>> Plans = new();
    private static readonly ConcurrentDictionary<string, Regex> RegexCache = new(StringComparer.Ordinal);

    /// <summary><see langword="true"/> when <paramref name="schema"/> declares any enforceable
    /// constraint for <paramref name="positions"/> — lets callers skip decoding entirely.</summary>
    public static bool HasConstraints(Schema schema, Positions positions) => !PlanFor(schema, positions).IsEmpty;

    /// <summary>Validates every constrained argument; throws <see cref="VgiInvalidArgumentException"/>
    /// naming the argument on the first violation.</summary>
    public static void Enforce(Schema schema, Positions positions, Func<int, object?> positional, int positionalCount,
        Func<string, object?> named)
    {
        var plan = PlanFor(schema, positions);
        foreach (var (position, varargs, constraint) in plan.Positional)
        {
            if (varargs)
            {
                for (var i = position; i < positionalCount; i++)
                {
                    Check(constraint, positional(i), $"positional argument {i}");
                }
            }
            else if (position < positionalCount)
            {
                Check(constraint, positional(position), $"positional argument {position}");
            }
        }

        foreach (var constraint in plan.Named)
        {
            Check(constraint, named(constraint.Name), $"named argument '{constraint.Name}'");
        }
    }

    /// <summary>Validates table-family arguments.</summary>
    public static void Enforce(Schema schema, Positions positions, Arguments.TableArguments args)
    {
        if (HasConstraints(schema, positions))
        {
            Enforce(schema, positions, args.Positional, args.PositionalCount, args.Named);
        }
    }

    /// <summary>Validates a scalar call's const arguments from their serialized bytes (decoded
    /// only when the schema declares a constraint).</summary>
    public static void EnforceConstants(Schema schema, byte[] arguments)
    {
        if (!HasConstraints(schema, Positions.ConstOnly))
        {
            return;
        }

        var constants = Arguments.ConstantArguments.Decode(arguments, schema);
        Enforce(schema, Positions.ConstOnly, constants.Get, constants.Count, constants.Get);
    }

    private static Plan PlanFor(Schema schema, Positions positions) =>
        Plans.GetValue(schema, static _ => new ConcurrentDictionary<Positions, Plan>())
            .GetOrAdd(positions, p => BuildPlan(schema, p));

    private static Plan BuildPlan(Schema schema, Positions positions)
    {
        var positional = new List<(int, bool, Constraint)>();
        var named = new List<Constraint>();
        var index = 0;
        foreach (var field in schema.FieldsList)
        {
            var meta = field.Metadata;
            var isNamed = Is(meta, VgiWireMetadata.ArgKey, VgiWireMetadata.ArgNamedValue);
            var isTable = Is(meta, VgiWireMetadata.TypeKey, VgiWireMetadata.TypeTableValue);
            var isConst = Is(meta, VgiWireMetadata.ConstKey, VgiWireMetadata.ConstTrueValue);
            var isVarargs = Is(meta, VgiWireMetadata.VarargsKey, VgiWireMetadata.VarargsTrueValue);
            var constraint = Parse(field);

            if (isNamed)
            {
                if (constraint.HasAny)
                {
                    named.Add(constraint);
                }

                continue;
            }

            if (isTable)
            {
                continue;
            }

            var counts = positions switch
            {
                Positions.ConstOnly => isConst,
                Positions.AllPositional => true,
                _ => false,
            };
            if (!counts)
            {
                continue;
            }

            if (constraint.HasAny)
            {
                positional.Add((index, isVarargs, constraint));
            }

            index++;
        }

        return new Plan(positional, named);
    }

    private static bool Is(IReadOnlyDictionary<string, string>? meta, string key, string value) =>
        meta is not null && meta.TryGetValue(key, out var v) && string.Equals(v, value, StringComparison.Ordinal);

    private static Constraint Parse(Field field)
    {
        var meta = field.Metadata;
        double? lower = null, upper = null;
        bool lowerInclusive = false, upperInclusive = false;
        string? range = null;
        IReadOnlyList<JsonElement>? choices = null;
        string? choicesJson = null;
        Regex? pattern = null;
        string? patternText = null;

        if (meta is not null && meta.TryGetValue(VgiWireMetadata.RangeKey, out var r) && !string.IsNullOrWhiteSpace(r))
        {
            range = r.Trim();
            (lower, lowerInclusive, upper, upperInclusive) = ParseRange(field.Name, range);
        }

        if (meta is not null && meta.TryGetValue(VgiWireMetadata.ChoicesKey, out var c) && !string.IsNullOrWhiteSpace(c))
        {
            try
            {
                using var doc = JsonDocument.Parse(c);
                if (doc.RootElement.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidOperationException(
                        $"Argument '{field.Name}': {VgiWireMetadata.ChoicesKey} must be a JSON array, got: {c}");
                }

                choices = doc.RootElement.EnumerateArray().Select(e => e.Clone()).ToList();
                choicesJson = c;
            }
            catch (JsonException e)
            {
                throw new InvalidOperationException(
                    $"Argument '{field.Name}': {VgiWireMetadata.ChoicesKey} is not valid JSON ({e.Message}): {c}", e);
            }
        }

        if (meta is not null && meta.TryGetValue(VgiWireMetadata.PatternKey, out var p) && !string.IsNullOrEmpty(p))
        {
            patternText = p;
            pattern = CompilePattern(field.Name, p);
        }

        return new Constraint(field.Name, lower, lowerInclusive, upper, upperInclusive, range, choices, choicesJson, pattern, patternText);
    }

    /// <summary>Compiles a <c>vgi_pattern</c> regex (cached; unanchored search semantics).</summary>
    public static Regex CompilePattern(string argumentName, string pattern)
    {
        try
        {
            return RegexCache.GetOrAdd(pattern, static p => new Regex(p, RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1)));
        }
        catch (ArgumentException e)
        {
            throw new InvalidOperationException($"Argument '{argumentName}': invalid {VgiWireMetadata.PatternKey} regex '{pattern}': {e.Message}", e);
        }
    }

    private static (double?, bool, double?, bool) ParseRange(string name, string range)
    {
        if (range.Length < 2 || (range[0] != '[' && range[0] != '(') || (range[^1] != ']' && range[^1] != ')'))
        {
            throw new InvalidOperationException($"Argument '{name}': malformed {VgiWireMetadata.RangeKey} '{range}' (expected e.g. \"[0, 10]\").");
        }

        var parts = range[1..^1].Split(',');
        if (parts.Length != 2)
        {
            throw new InvalidOperationException($"Argument '{name}': malformed {VgiWireMetadata.RangeKey} '{range}' (expected two bounds).");
        }

        return (Bound(name, range, parts[0]), range[0] == '[', Bound(name, range, parts[1]), range[^1] == ']');
    }

    private static double? Bound(string name, string range, string text)
    {
        text = text.Trim();
        if (text is "-inf" or "+inf" or "inf" or "")
        {
            return null;
        }

        return double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var v)
            ? v
            : throw new InvalidOperationException($"Argument '{name}': malformed {VgiWireMetadata.RangeKey} bound '{text}' in '{range}'.");
    }

    private static void Check(Constraint constraint, object? value, string where)
    {
        if (value is null)
        {
            return;
        }

        var label = $"Argument '{constraint.Name}' ({where})";
        if (constraint.Range is not null && TryNumber(value, out var number))
        {
            if (constraint.Lower is { } lo && (constraint.LowerInclusive ? number < lo : number <= lo))
            {
                throw Violation($"{label} is too small: {Render(value)} must be {(constraint.LowerInclusive ? ">=" : ">")} {Render(lo)} (valid range {constraint.Range}).");
            }

            if (constraint.Upper is { } hi && (constraint.UpperInclusive ? number > hi : number >= hi))
            {
                throw Violation($"{label} is too large: {Render(value)} must be {(constraint.UpperInclusive ? "<=" : "<")} {Render(hi)} (valid range {constraint.Range}).");
            }
        }

        if (constraint.Choices is not null && !constraint.Choices.Any(choice => ChoiceEquals(choice, value)))
        {
            throw Violation($"{label} has an invalid value {Render(value)}; must be one of {constraint.ChoicesJson}.");
        }

        if (constraint.Pattern is not null)
        {
            if (value is not string s)
            {
                throw Violation($"{label} must be a string matching pattern '{constraint.PatternText}', got {Render(value)}.");
            }

            bool matched;
            try
            {
                matched = constraint.Pattern.IsMatch(s);
            }
            catch (RegexMatchTimeoutException)
            {
                throw Violation($"{label}: pattern '{constraint.PatternText}' timed out matching {Render(value)}.");
            }

            if (!matched)
            {
                throw Violation($"{label} value {Render(value)} does not match pattern '{constraint.PatternText}'.");
            }
        }
    }

    private static VgiInvalidArgumentException Violation(string message) => new(message);

    private static bool TryNumber(object value, out double number)
    {
        switch (value)
        {
            case sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal:
                number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                return true;
            default:
                number = 0;
                return false;
        }
    }

    private static bool ChoiceEquals(JsonElement choice, object value)
    {
        switch (choice.ValueKind)
        {
            case JsonValueKind.String:
                return value is string s && string.Equals(s, choice.GetString(), StringComparison.Ordinal);
            case JsonValueKind.Number:
                if (value is decimal dec && choice.TryGetDecimal(out var cd))
                {
                    return dec == cd;
                }

                return TryNumber(value, out var n) && choice.TryGetDouble(out var cn) && n == cn;
            case JsonValueKind.True:
                return value is true;
            case JsonValueKind.False:
                return value is false;
            default:
                return false;
        }
    }

    private static string Render(object value) => value switch
    {
        string s => $"'{s}'",
        double d => d.ToString("R", CultureInfo.InvariantCulture),
        float f => f.ToString("R", CultureInfo.InvariantCulture),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    /// <summary>Encodes a choices list as the <c>vgi_choices</c> JSON array, matching Python's
    /// <c>json.dumps(list(choices))</c> spacing.</summary>
    public static string EncodeChoices(IEnumerable<object?> choices)
    {
        var sb = new StringBuilder("[");
        var first = true;
        foreach (var choice in choices)
        {
            if (!first)
            {
                sb.Append(", ");
            }

            first = false;
            sb.Append(choice switch
            {
                null => "null",
                bool b => b ? "true" : "false",
                string s => JsonSerializer.Serialize(s),
                IFormattable f when choice is not char => f.ToString(null, CultureInfo.InvariantCulture),
                _ => JsonSerializer.Serialize(choice.ToString()),
            });
        }

        return sb.Append(']').ToString();
    }
}
