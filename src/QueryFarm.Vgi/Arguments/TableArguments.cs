using Apache.Arrow;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Arguments;

/// <summary>
/// The decoded bind-time SQL arguments of a table, table-in-out, table-buffering or aggregate
/// call — every value is a single-row constant. Positional arguments are indexed from 0 in the
/// order the function's arguments schema declares them (a <c>TABLE</c>-typed slot is skipped and
/// the rest renumbered contiguously; for an aggregate only the <c>vgi_const</c> parameters count).
/// Named arguments are keyed by their SQL name (<c>count := 5</c> is <c>"count"</c>).
/// </summary>
/// <example>
/// <code>
/// var count = args.Get&lt;long&gt;(0);                    // required positional
/// var label = args.GetOrDefault("label", "none");     // optional named
/// </code>
/// </example>
public sealed class TableArguments
{
    private readonly IReadOnlyList<IArrowArray?> _positional;
    private readonly IReadOnlyDictionary<string, IArrowArray> _named;
    private readonly IReadOnlyList<IReadOnlyDictionary<string, string>?>? _positionalMetadata;
    private readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>?>? _namedMetadata;

    /// <summary>Builds an argument view from already-decoded single-row arrays (useful for unit
    /// tests). Use <see cref="Decode"/> for the wire bytes.</summary>
    public TableArguments(
        IReadOnlyList<IArrowArray?> positional,
        IReadOnlyDictionary<string, IArrowArray> named,
        IReadOnlyList<IReadOnlyDictionary<string, string>?>? positionalMetadata = null,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>?>? namedMetadata = null)
    {
        ArgumentNullException.ThrowIfNull(positional);
        ArgumentNullException.ThrowIfNull(named);
        _positional = positional;
        _named = named;
        _positionalMetadata = positionalMetadata;
        _namedMetadata = namedMetadata;
    }

    /// <summary>An empty argument list.</summary>
    public static TableArguments Empty { get; } = new([], new Dictionary<string, IArrowArray>(StringComparer.Ordinal));

    /// <summary>Decodes serialized argument bytes (e.g. <see cref="Table.TableBindParams.ArgumentsBytes"/>).
    /// <see langword="null"/>/empty bytes decode to an empty argument list.</summary>
    public static TableArguments Decode(byte[]? arguments) => TableArgCodec.Decode(arguments);

    /// <summary>Number of positional arguments.</summary>
    public int PositionalCount => _positional.Count;

    /// <summary>Every named argument the call supplied, as single-row Arrow arrays.</summary>
    public IReadOnlyDictionary<string, IArrowArray> NamedArrays => _named;

    /// <summary>The names of the named arguments the call supplied.</summary>
    public IEnumerable<string> Names => _named.Keys;

    /// <summary><see langword="true"/> when the call supplied named argument <paramref name="name"/>.</summary>
    public bool HasNamed(string name) => _named.ContainsKey(name);

    /// <summary>The raw single-row array for positional argument <paramref name="index"/>, or
    /// <see langword="null"/> when absent.</summary>
    public IArrowArray? PositionalArray(int index) => index >= 0 && index < _positional.Count ? _positional[index] : null;

    /// <summary>The raw single-row array for named argument <paramref name="name"/>, or
    /// <see langword="null"/> when absent.</summary>
    public IArrowArray? NamedArray(string name) => _named.GetValueOrDefault(name);

    /// <summary>The wire struct field's own Arrow metadata for positional argument
    /// <paramref name="index"/> — e.g. an <c>ARROW:extension:name</c> annotation DuckDB attaches
    /// to an exotic constant (HUGEINT, UUID, ...) under <c>arrow_lossless_conversion</c>.
    /// <see langword="null"/> when the argument carried no metadata or doesn't exist.</summary>
    public IReadOnlyDictionary<string, string>? PositionalMetadata(int index) =>
        _positionalMetadata is not null && index >= 0 && index < _positionalMetadata.Count ? _positionalMetadata[index] : null;

    /// <summary>See <see cref="PositionalMetadata"/>.</summary>
    public IReadOnlyDictionary<string, string>? NamedMetadata(string name) =>
        _namedMetadata?.GetValueOrDefault(name);

    /// <summary>Positional argument <paramref name="index"/> as a boxed CLR value
    /// (<see langword="long"/>, <see langword="double"/>, <see langword="string"/>, <see langword="bool"/>,
    /// <see langword="byte"/>[], <see langword="decimal"/>, ...); <see langword="null"/> for SQL NULL
    /// or a missing argument.</summary>
    public object? Positional(int index) => ScalarArgCodec.ReadScalar(PositionalArray(index));

    /// <summary>Named argument <paramref name="name"/> as a boxed CLR value; <see langword="null"/>
    /// for SQL NULL or when the call did not supply it.</summary>
    public object? Named(string name) => ScalarArgCodec.ReadScalar(NamedArray(name));

    /// <summary>Positional argument <paramref name="index"/> converted to <typeparamref name="T"/>
    /// (numeric widening/narrowing included). For a nullable <typeparamref name="T"/>
    /// (<c>long?</c>, <c>string?</c>) SQL NULL/missing yields <see langword="null"/>; for a
    /// non-nullable value type it throws <see cref="VgiInvalidArgumentException"/>.</summary>
    public T Get<T>(int index) => ArgumentValues.Convert<T>(Positional(index), $"positional argument {index}");

    /// <summary>Named argument <paramref name="name"/> converted to <typeparamref name="T"/> — see
    /// <see cref="Get{T}(int)"/>.</summary>
    public T Get<T>(string name) => ArgumentValues.Convert<T>(Named(name), $"named argument '{name}'");

    /// <summary>Positional argument <paramref name="index"/> converted to <typeparamref name="T"/>,
    /// or <paramref name="defaultValue"/> when SQL NULL/missing.</summary>
    public T GetOrDefault<T>(int index, T defaultValue) =>
        Positional(index) is { } v ? ArgumentValues.Convert<T>(v, $"positional argument {index}") : defaultValue;

    /// <summary>Named argument <paramref name="name"/> converted to <typeparamref name="T"/>, or
    /// <paramref name="defaultValue"/> when SQL NULL/not supplied.</summary>
    public T GetOrDefault<T>(string name, T defaultValue) =>
        Named(name) is { } v ? ArgumentValues.Convert<T>(v, $"named argument '{name}'") : defaultValue;

    public long Int64(int index) =>
        System.Convert.ToInt64(Positional(index) ?? throw new VgiInvalidArgumentException($"Missing required positional argument {index}."),
            System.Globalization.CultureInfo.InvariantCulture);

    public long? Int64OrNull(int index) =>
        Positional(index) is { } v ? System.Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture) : null;

    public long Int64Named(string name, long defaultValue) =>
        Named(name) is { } v ? System.Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture) : defaultValue;

    public double DoubleNamed(string name, double defaultValue) =>
        Named(name) is { } v ? System.Convert.ToDouble(v, System.Globalization.CultureInfo.InvariantCulture) : defaultValue;

    public string StringPositional(int index) =>
        (string?)Positional(index) ?? throw new VgiInvalidArgumentException($"Missing required positional argument {index}.");

    public string StringNamed(string name, string defaultValue) =>
        Named(name) is string s ? s : defaultValue;

    public bool BoolNamed(string name, bool defaultValue) =>
        Named(name) is bool b ? b : defaultValue;
}

/// <summary>Shared boxed-value → <c>T</c> conversion for the typed argument accessors.</summary>
internal static class ArgumentValues
{
    public static T Convert<T>(object? value, string what)
    {
        if (value is null)
        {
            if (default(T) is null)
            {
                return default!;
            }

            throw new VgiInvalidArgumentException($"Missing required {what} (got NULL or no value).");
        }

        if (value is T typed)
        {
            return typed;
        }

        try
        {
            return (T)ScalarArgCodec.ConvertTo(value, typeof(T))!;
        }
        catch (Exception e) when (e is InvalidCastException or FormatException or OverflowException)
        {
            throw new VgiInvalidArgumentException(
                $"Cannot convert {what} value {value} ({value.GetType().Name}) to {typeof(T).Name}: {e.Message}");
        }
    }
}
