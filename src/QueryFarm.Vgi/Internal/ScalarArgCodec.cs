using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Decodes the two small embedded-IPC "extra argument" shapes a scalar bind call carries:
/// <see cref="Scalar.ScalarBindParams.Arguments"/>/<see cref="Scalar.ScalarProcessParams.Arguments"/>
/// (const-parameter values, wrapped one level in a <c>struct</c> named <c>args</c> per
/// <c>vgi_arrow_utils.cpp</c>'s <c>BuildArgumentsFromValues</c>) and
/// <see cref="Scalar.ScalarBindParams.Settings"/>/<see cref="Scalar.ScalarProcessParams.Settings"/>
/// (resolved DuckDB setting values, one flat row keyed by setting name). Shared by
/// <see cref="Scalar.ScalarFn"/>'s <c>[ConstParam]</c>/<c>[Setting]</c> binders and by any
/// hand-rolled <see cref="Scalar.IScalarFunction"/> that needs a const value of a shape
/// <see cref="Scalar.ScalarFn"/> doesn't auto-bind (e.g. a nested struct const).
/// </summary>
public static class ScalarArgCodec
{
    /// <summary>Decodes <c>Arguments</c> into <c>positional_&lt;i&gt;</c> → value, by the const
    /// argument's OWN sequential index (0.. over const parameters only — see
    /// <see cref="Scalar.ScalarBindParams.Arguments"/>'s doc comment), keyed here by that bare
    /// integer index for convenience.</summary>
    public static IReadOnlyDictionary<int, IArrowArray> DecodeConstStruct(byte[] arguments)
    {
        var result = new Dictionary<int, IArrowArray>();
        RecordBatch? batch;
        try
        {
            batch = ReadFirstBatch(arguments);
        }
        catch (ArgumentNullException)
        {
            // The vendored Apache.Arrow C# IPC reader crashes parsing a `struct<>` (zero-CHILD
            // struct) schema field (see TableArgCodec.Decode's identical guard, and ComputePlan's
            // `_hasConstParams` short-circuit, which normally avoids calling this method at all for
            // such a call) — reached here too now that OverloadResolver.SelectScalar must call this
            // unconditionally (for EVERY multi-candidate name) even for a call site with zero
            // actual const arguments, e.g. resolving among format_number's 0/1/2-ConstParam
            // overloads for a 0-ConstParam call. Treat it as "no const arguments", same as the
            // TableArgCodec precedent.
            return result;
        }

        if (batch is null || batch.Schema.FieldsList.Count == 0)
        {
            return result;
        }

        if (batch.Column(0) is not StructArray args)
        {
            return result;
        }

        var structType = (Apache.Arrow.Types.StructType)args.Data.DataType;
        for (var i = 0; i < structType.Fields.Count; i++)
        {
            var name = structType.Fields[i].Name;
            if (name.StartsWith("positional_", StringComparison.Ordinal) &&
                int.TryParse(name.AsSpan("positional_".Length), out var index))
            {
                result[index] = args.Fields[i];
            }
        }

        return result;
    }

    /// <summary>Decodes <c>Settings</c> into setting-key → single-row value column. Returns an
    /// empty dictionary when <paramref name="settings"/> is <c>null</c>/empty (no settings were
    /// resolved — nothing was declared in <c>RequiredSettings</c>, or none matched).</summary>
    public static IReadOnlyDictionary<string, IArrowArray> DecodeSettings(byte[]? settings)
    {
        var result = new Dictionary<string, IArrowArray>(StringComparer.Ordinal);
        if (settings is null || settings.Length == 0)
        {
            return result;
        }

        var batch = ReadFirstBatch(settings);
        if (batch is null)
        {
            return result;
        }

        for (var i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            result[batch.Schema.GetFieldByIndex(i).Name] = batch.Column(i);
        }

        return result;
    }

    /// <summary>Reads a single boxed scalar value out of any of the primitive Arrow array types
    /// the VGI wire uses for const/setting values (row 0 only — every shape this codec deals with
    /// is a single-row batch). Returns <c>null</c> for a SQL NULL.</summary>
    public static object? ReadScalar(IArrowArray? array, int index = 0)
    {
        if (array is null || index >= array.Length || array.IsNull(index))
        {
            return null;
        }

        return array switch
        {
            Int8Array a => a.GetValue(index),
            Int16Array a => a.GetValue(index),
            Int32Array a => a.GetValue(index),
            Int64Array a => a.GetValue(index),
            UInt8Array a => a.GetValue(index),
            UInt16Array a => a.GetValue(index),
            UInt32Array a => a.GetValue(index),
            UInt64Array a => a.GetValue(index),
            FloatArray a => a.GetValue(index),
            DoubleArray a => a.GetValue(index),
            StringArray a => a.GetString(index),
            BooleanArray a => a.GetValue(index),
            BinaryArray a => a.GetBytes(index).ToArray(),
            // A bind-time DOUBLE-typed const argument can arrive on the wire as decimal128 rather
            // than float64 — e.g. an aggregate ConstParam bound from a bare numeric literal
            // (`vgi_percentile(x, 0.5)`): DuckDB's custom aggregate-bind const-extraction reads the
            // literal's ORIGINAL parsed type (DECIMAL) rather than the function's declared
            // parameter type, unlike a scalar function's argument-casting pipeline. CLR `decimal`
            // (not `double`) preserves exact precision; callers needing a `double` convert via
            // `Convert.ToDouble`, which handles `decimal` natively.
            Decimal128Array a => a.GetValue(index),
            _ => throw new NotSupportedException($"Unsupported const/setting array type '{array.GetType()}'."),
        };
    }

    /// <summary>Converts a boxed scalar (as returned by <see cref="ReadScalar"/>) to the given CLR
    /// target type — handles the numeric widening a declared ConstParam type may need (e.g. the
    /// wire value arrives as a boxed <see cref="int"/> but the parameter is declared <see cref="long"/>).</summary>
    public static object? ConvertTo(object? value, Type targetType)
    {
        if (value is null)
        {
            return null;
        }

        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
        if (underlying.IsInstanceOfType(value))
        {
            return value;
        }

        if (underlying == typeof(string) || underlying == typeof(byte[]) || underlying == typeof(bool))
        {
            return value;
        }

        return Convert.ChangeType(value, underlying);
    }

    private static RecordBatch? ReadFirstBatch(byte[] bytes)
    {
        if (bytes.Length == 0)
        {
            return null;
        }

        using var stream = new MemoryStream(bytes);
        using var reader = new ArrowStreamReader(stream);
        return reader.ReadNextRecordBatch();
    }
}
