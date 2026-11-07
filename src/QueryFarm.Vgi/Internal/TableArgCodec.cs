using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Decodes a table (or aggregate) function's <c>BindRequest.Arguments</c>/<c>TableFunctionCardinalityRequest</c>-
/// style bind-time argument struct — every table-function SQL argument is a bind-time constant (no
/// per-row concept the way a scalar function has one), so the WHOLE argument list (positional AND
/// named/keyword) travels this one way, unlike <see cref="ScalarArgCodec"/>'s const-parameters-only
/// slice of a scalar call.
///
/// Wire shape (<c>vgi_arrow_utils.cpp</c>'s <c>BuildArgumentsFromValues</c>, byte-for-byte per
/// <c>vgi_protocol_constants.hpp</c>): one embedded-IPC batch, single column named <c>args</c>,
/// type <c>struct&lt;positional_0: T0, positional_1: T1, ..., named_&lt;key&gt;: Tk, ...&gt;</c>,
/// exactly one row.
/// </summary>
public static class TableArgCodec
{
    public const string PositionalPrefix = "positional_";
    public const string NamedPrefix = "named_";

    public static TableArguments Decode(byte[]? arguments)
    {
        if (arguments is null || arguments.Length == 0)
        {
            return Empty();
        }

        RecordBatch? batch;
        try
        {
            using var stream = new MemoryStream(arguments);
            using var reader = new ArrowStreamReader(stream);
            batch = reader.ReadNextRecordBatch();
        }
        catch (ArgumentNullException)
        {
            // The vendored Apache.Arrow C# IPC reader crashes parsing a `struct<>` (zero-CHILD
            // struct) schema field: Apache.Arrow.Types.StructType's constructor rejects a null
            // `fields` list, which is exactly what MessageSerializer.GetFieldArrowType passes when
            // FlatBuffers encodes a struct with no children. The C++ extension sends exactly this
            // shape for BindRequest.Arguments whenever a table/table-in-out/table-buffering call
            // site supplies NO non-table positional/named arguments (a TABLE-only signature, or
            // every optional named arg left at its default) — a legitimate, common wire shape
            // (see also ComputePlan's _hasConstParams/_hasSettings guards for the scalar-path
            // analog of this exact class of bug), not malformed input. Treat it as "no arguments".
            return Empty();
        }

        if (batch is null || batch.Schema.FieldsList.Count == 0 || batch.Column(0) is not StructArray args)
        {
            return Empty();
        }

        var structType = (Apache.Arrow.Types.StructType)args.Data.DataType;
        var positional = new SortedDictionary<int, IArrowArray>();
        var positionalMeta = new SortedDictionary<int, IReadOnlyDictionary<string, string>?>();
        var named = new Dictionary<string, IArrowArray>(StringComparer.Ordinal);
        var namedMeta = new Dictionary<string, IReadOnlyDictionary<string, string>?>(StringComparer.Ordinal);

        for (var i = 0; i < structType.Fields.Count; i++)
        {
            var field = structType.Fields[i];
            var name = field.Name;
            var value = args.Fields[i];
            if (name.StartsWith(PositionalPrefix, StringComparison.Ordinal) &&
                int.TryParse(name.AsSpan(PositionalPrefix.Length), out var index))
            {
                positional[index] = value;
                positionalMeta[index] = field.Metadata;
            }
            else if (name.StartsWith(NamedPrefix, StringComparison.Ordinal))
            {
                var key = name[NamedPrefix.Length..];
                named[key] = value;
                namedMeta[key] = field.Metadata;
            }
        }

        var maxIndex = positional.Count == 0 ? -1 : positional.Keys.Max();
        var positionalList = new IArrowArray?[maxIndex + 1];
        var positionalMetaList = new IReadOnlyDictionary<string, string>?[maxIndex + 1];
        foreach (var (index, value) in positional)
        {
            positionalList[index] = value;
            positionalMetaList[index] = positionalMeta[index];
        }

        return new TableArguments(positionalList, named, positionalMetaList, namedMeta);
    }

    private static TableArguments Empty() => new(
        [], new Dictionary<string, IArrowArray>(StringComparer.Ordinal),
        [], new Dictionary<string, IReadOnlyDictionary<string, string>?>(StringComparer.Ordinal));
}

/// <summary>Legacy name for <see cref="Arguments.TableArguments"/>, kept so code written against
/// QueryFarm.Vgi 0.23 and earlier keeps compiling: it converts implicitly to and from
/// <see cref="Arguments.TableArguments"/> (namespace <c>QueryFarm.Vgi.Arguments</c>), which is what
/// every <c>*Params.Arguments</c> property is typed as now. New code should use that type.</summary>
[System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
public sealed class TableArguments
{
    private readonly Arguments.TableArguments _inner;

    public TableArguments(
        IReadOnlyList<IArrowArray?> positional,
        IReadOnlyDictionary<string, IArrowArray> named,
        IReadOnlyList<IReadOnlyDictionary<string, string>?>? positionalMetadata = null,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>?>? namedMetadata = null)
        : this(new Arguments.TableArguments(positional, named, positionalMetadata, namedMetadata))
    {
    }

    private TableArguments(Arguments.TableArguments inner) => _inner = inner;

    [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull(nameof(arguments))]
    public static implicit operator Arguments.TableArguments?(TableArguments? arguments) => arguments?._inner;

    [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull(nameof(arguments))]
    public static implicit operator TableArguments?(Arguments.TableArguments? arguments) =>
        arguments is null ? null : new TableArguments(arguments);

    public int PositionalCount => _inner.PositionalCount;

    public IReadOnlyDictionary<string, IArrowArray> NamedArrays => _inner.NamedArrays;

    public IArrowArray? PositionalArray(int index) => _inner.PositionalArray(index);

    public IArrowArray? NamedArray(string name) => _inner.NamedArray(name);

    public IReadOnlyDictionary<string, string>? PositionalMetadata(int index) => _inner.PositionalMetadata(index);

    public IReadOnlyDictionary<string, string>? NamedMetadata(string name) => _inner.NamedMetadata(name);

    public object? Positional(int index) => _inner.Positional(index);

    public object? Named(string name) => _inner.Named(name);

    public long Int64(int index) => _inner.Int64(index);

    public long? Int64OrNull(int index) => _inner.Int64OrNull(index);

    public long Int64Named(string name, long defaultValue) => _inner.Int64Named(name, defaultValue);

    public double DoubleNamed(string name, double defaultValue) => _inner.DoubleNamed(name, defaultValue);

    public string StringPositional(int index) => _inner.StringPositional(index);

    public string StringNamed(string name, string defaultValue) => _inner.StringNamed(name, defaultValue);

    public bool BoolNamed(string name, bool defaultValue) => _inner.BoolNamed(name, defaultValue);
}
