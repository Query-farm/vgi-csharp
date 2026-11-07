using Apache.Arrow;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Arguments;

/// <summary>
/// The bind-time constant (<c>vgi_const</c> / <c>[ConstParam]</c>) arguments of a scalar call.
/// Positions count constant parameters only, in declaration order — in
/// <c>f(x, 3, 'a')</c> with <c>x</c> a per-row column, <c>Get&lt;long&gt;(0)</c> is <c>3</c> and
/// <c>Get&lt;string&gt;(1)</c> is <c>'a'</c>. Lookup by name uses the constant parameter's
/// declared field name.
/// </summary>
/// <example>
/// <code>
/// public void Bind(ScalarBindParams p)
/// {
///     var precision = p.Constants.Get&lt;long&gt;("precision");
/// }
/// </code>
/// </example>
public sealed class ConstantArguments
{
    private readonly IReadOnlyDictionary<int, IArrowArray> _positional;
    private readonly IReadOnlyDictionary<string, IArrowArray> _named;
    private readonly IReadOnlyList<string> _constNames;

    private ConstantArguments(IReadOnlyDictionary<int, IArrowArray> positional,
        IReadOnlyDictionary<string, IArrowArray> named, IReadOnlyList<string> constNames)
    {
        _positional = positional;
        _named = named;
        _constNames = constNames;
    }

    /// <summary>No constants.</summary>
    public static ConstantArguments Empty { get; } = new(
        new Dictionary<int, IArrowArray>(), new Dictionary<string, IArrowArray>(StringComparer.Ordinal), []);

    /// <summary>Decodes serialized constant-argument bytes (<see cref="Scalar.ScalarBindParams.Arguments"/>).
    /// Pass the function's <paramref name="argumentsSchema"/> to enable lookup by name.</summary>
    public static ConstantArguments Decode(byte[]? arguments, Schema? argumentsSchema = null)
    {
        if (arguments is null || arguments.Length == 0)
        {
            return argumentsSchema is null ? Empty : new(Empty._positional, Empty._named, ConstNames(argumentsSchema));
        }

        var (positional, named) = ScalarArgCodec.DecodeArgumentStruct(arguments);
        return new ConstantArguments(positional, named, argumentsSchema is null ? [] : ConstNames(argumentsSchema));
    }

    /// <summary>Names of the declared constant parameters, in position order (empty when the
    /// arguments schema was not supplied).</summary>
    public IReadOnlyList<string> Names => _constNames;

    /// <summary>Number of constant values the call carried.</summary>
    public int Count => _positional.Count;

    /// <summary>The raw single-row array for constant <paramref name="index"/>, or <see langword="null"/>.</summary>
    public IArrowArray? Array(int index) => _positional.GetValueOrDefault(index);

    /// <summary>The raw single-row array for constant <paramref name="name"/>, or <see langword="null"/>.</summary>
    public IArrowArray? Array(string name)
    {
        if (_named.TryGetValue(name, out var named))
        {
            return named;
        }

        for (var i = 0; i < _constNames.Count; i++)
        {
            if (string.Equals(_constNames[i], name, StringComparison.Ordinal))
            {
                return Array(i);
            }
        }

        return null;
    }

    /// <summary><see langword="true"/> when constant <paramref name="index"/> is present.</summary>
    public bool Contains(int index) => _positional.ContainsKey(index);

    /// <summary><see langword="true"/> when constant <paramref name="name"/> is present.</summary>
    public bool Contains(string name) => Array(name) is not null;

    /// <summary>Constant <paramref name="index"/> as a boxed CLR value; <see langword="null"/> for SQL
    /// NULL or a missing value.</summary>
    public object? Get(int index) => ScalarArgCodec.ReadScalar(Array(index));

    /// <summary>Constant <paramref name="name"/> as a boxed CLR value; <see langword="null"/> for SQL
    /// NULL or a missing value.</summary>
    public object? Get(string name) => ScalarArgCodec.ReadScalar(Array(name));

    /// <summary>Constant <paramref name="index"/> converted to <typeparamref name="T"/>. A nullable
    /// <typeparamref name="T"/> yields <see langword="null"/> for SQL NULL; a non-nullable value type
    /// throws <see cref="VgiInvalidArgumentException"/>.</summary>
    public T Get<T>(int index) => ArgumentValues.Convert<T>(Get(index), $"constant argument {Describe(index)}");

    /// <summary>Constant <paramref name="name"/> converted to <typeparamref name="T"/> — see
    /// <see cref="Get{T}(int)"/>.</summary>
    public T Get<T>(string name) => ArgumentValues.Convert<T>(Get(name), $"constant argument '{name}'");

    /// <summary>Constant <paramref name="index"/> converted to <typeparamref name="T"/>, or
    /// <paramref name="defaultValue"/> when NULL/missing.</summary>
    public T GetOrDefault<T>(int index, T defaultValue) =>
        Get(index) is { } v ? ArgumentValues.Convert<T>(v, $"constant argument {Describe(index)}") : defaultValue;

    /// <summary>Constant <paramref name="name"/> converted to <typeparamref name="T"/>, or
    /// <paramref name="defaultValue"/> when NULL/missing.</summary>
    public T GetOrDefault<T>(string name, T defaultValue) =>
        Get(name) is { } v ? ArgumentValues.Convert<T>(v, $"constant argument '{name}'") : defaultValue;

    private string Describe(int index) =>
        index >= 0 && index < _constNames.Count ? $"'{_constNames[index]}' (position {index})" : index.ToString(System.Globalization.CultureInfo.InvariantCulture);

    internal static IReadOnlyList<string> ConstNames(Schema schema) =>
        schema.FieldsList
            .Where(f => f.Metadata is not null &&
                        f.Metadata.TryGetValue(VgiWireMetadata.ConstKey, out var v) &&
                        string.Equals(v, VgiWireMetadata.ConstTrueValue, StringComparison.Ordinal))
            .Select(f => f.Name)
            .ToList();
}
