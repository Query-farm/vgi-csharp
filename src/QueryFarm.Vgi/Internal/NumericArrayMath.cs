using System.Data.SqlTypes;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Per-row numeric arithmetic (double / add / sum) against arbitrary-typed Arrow input columns,
/// producing an array of a given (already <see cref="Types.TypeRules"/>-promoted) output type.
/// Shared by the ANY-typed scalar fixtures (<c>double</c>, <c>add_values</c>, <c>sum_values</c>)
/// so each only has to handle its own arg-count/bind-time-validation shape.
///
/// Decimal columns are handled via <see cref="SqlDecimal"/> (not CLR <see cref="decimal"/>, which
/// tops out at 29 significant digits) since Arrow's decimal128 — and this suite's
/// <c>double(DECIMAL(38,0))</c> overflow-rejection regression test — needs the full 38-digit range;
/// every other numeric type is handled via <see cref="double"/> arithmetic (adequate for every
/// integer width this suite promotes to, since the promoted width never exceeds 64 bits and no
/// fixture pushes a value close enough to 2^53 for float64 rounding to matter).
/// </summary>
public static class NumericArrayMath
{
    public static IArrowArray Double(IArrowArray input, IArrowType outputType, int length)
    {
        if (outputType is Decimal128Type outDec)
        {
            return DoubleDecimal((Decimal128Array)input, outDec);
        }

        return BuildNumeric(outputType, length, i => ReadAsDouble(input, i) is { } v ? v * 2 : null);
    }

    public static IArrowArray Add(IArrowArray a, IArrowArray b, IArrowType outputType, int length)
    {
        if (outputType is Decimal128Type outDec)
        {
            return AddDecimal(a, b, outDec, length);
        }

        return BuildNumeric(outputType, length, i =>
        {
            var av = ReadAsDouble(a, i);
            var bv = ReadAsDouble(b, i);
            return av is null || bv is null ? null : av.Value + bv.Value;
        });
    }

    public static IArrowArray Sum(IReadOnlyList<IArrowArray> values, IArrowType outputType, int length)
    {
        if (outputType is Decimal128Type outDec)
        {
            return SumDecimal(values, outDec, length);
        }

        return BuildNumeric(outputType, length, i =>
        {
            double total = 0;
            foreach (var array in values)
            {
                var v = ReadAsDouble(array, i);
                if (v is null)
                {
                    return null;
                }

                total += v.Value;
            }

            return total;
        });
    }

    public static double? ReadAsDouble(IArrowArray array, int index) => array switch
    {
        Int8Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int16Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int32Array a => a.IsNull(index) ? null : a.GetValue(index),
        Int64Array a => a.IsNull(index) ? null : a.GetValue(index),
        UInt8Array a => a.IsNull(index) ? null : a.GetValue(index),
        UInt16Array a => a.IsNull(index) ? null : a.GetValue(index),
        UInt32Array a => a.IsNull(index) ? null : a.GetValue(index),
        UInt64Array a => a.IsNull(index) ? null : a.GetValue(index),
        FloatArray a => a.IsNull(index) ? null : a.GetValue(index),
        DoubleArray a => a.IsNull(index) ? null : a.GetValue(index),
        _ => throw new NotSupportedException($"Unsupported numeric array type '{array.GetType()}'."),
    };

    private static IArrowArray BuildNumeric(IArrowType outputType, int length, Func<int, double?> valueAt)
    {
        switch (outputType)
        {
            case Int8Type:
                {
                    var builder = new Int8Array.Builder();
                    for (var i = 0; i < length; i++)
                    {
                        var v = valueAt(i);
                        builder.Append(v is null ? (sbyte?)null : (sbyte)v.Value);
                    }

                    return builder.Build();
                }

            case Int16Type:
                {
                    var builder = new Int16Array.Builder();
                    for (var i = 0; i < length; i++)
                    {
                        var v = valueAt(i);
                        builder.Append(v is null ? (short?)null : (short)v.Value);
                    }

                    return builder.Build();
                }

            case Int32Type:
                {
                    var builder = new Int32Array.Builder();
                    for (var i = 0; i < length; i++)
                    {
                        var v = valueAt(i);
                        builder.Append(v is null ? (int?)null : (int)v.Value);
                    }

                    return builder.Build();
                }

            case Int64Type:
                {
                    var builder = new Int64Array.Builder();
                    for (var i = 0; i < length; i++)
                    {
                        var v = valueAt(i);
                        builder.Append(v is null ? (long?)null : (long)v.Value);
                    }

                    return builder.Build();
                }

            case UInt8Type:
                {
                    var builder = new UInt8Array.Builder();
                    for (var i = 0; i < length; i++)
                    {
                        var v = valueAt(i);
                        builder.Append(v is null ? (byte?)null : (byte)v.Value);
                    }

                    return builder.Build();
                }

            case UInt16Type:
                {
                    var builder = new UInt16Array.Builder();
                    for (var i = 0; i < length; i++)
                    {
                        var v = valueAt(i);
                        builder.Append(v is null ? (ushort?)null : (ushort)v.Value);
                    }

                    return builder.Build();
                }

            case UInt32Type:
                {
                    var builder = new UInt32Array.Builder();
                    for (var i = 0; i < length; i++)
                    {
                        var v = valueAt(i);
                        builder.Append(v is null ? (uint?)null : (uint)v.Value);
                    }

                    return builder.Build();
                }

            case UInt64Type:
                {
                    var builder = new UInt64Array.Builder();
                    for (var i = 0; i < length; i++)
                    {
                        var v = valueAt(i);
                        builder.Append(v is null ? (ulong?)null : (ulong)v.Value);
                    }

                    return builder.Build();
                }
            case FloatType:
                {
                    var builder = new FloatArray.Builder();
                    for (var i = 0; i < length; i++)
                    {
                        var v = valueAt(i);
                        builder.Append(v is null ? (float?)null : (float)v.Value);
                    }

                    return builder.Build();
                }

            case DoubleType:
                {
                    var builder = new DoubleArray.Builder();
                    for (var i = 0; i < length; i++)
                    {
                        builder.Append((double?)valueAt(i));
                    }

                    return builder.Build();
                }

            default:
                throw new NotSupportedException($"Unsupported numeric output type '{outputType}'.");
        }
    }

    private static IArrowArray DoubleDecimal(Decimal128Array input, Decimal128Type outputType)
    {
        var builder = new Decimal128Array.Builder(outputType);
        for (var i = 0; i < input.Length; i++)
        {
            if (input.IsNull(i))
            {
                builder.AppendNull();
                continue;
            }

            var value = input.GetSqlDecimal(i)!.Value;
            builder.Append(SafeAdd(value, value, outputType.Precision));
        }

        return builder.Build();
    }

    private static IArrowArray AddDecimal(IArrowArray a, IArrowArray b, Decimal128Type outputType, int length)
    {
        var builder = new Decimal128Array.Builder(outputType);
        for (var i = 0; i < length; i++)
        {
            var av = ReadAsSqlDecimal(a, i);
            var bv = ReadAsSqlDecimal(b, i);
            if (av is null || bv is null)
            {
                builder.AppendNull();
                continue;
            }

            builder.Append(SafeAdd(av.Value, bv.Value, outputType.Precision));
        }

        return builder.Build();
    }

    private static IArrowArray SumDecimal(IReadOnlyList<IArrowArray> values, Decimal128Type outputType, int length)
    {
        var builder = new Decimal128Array.Builder(outputType);
        for (var i = 0; i < length; i++)
        {
            SqlDecimal? total = new SqlDecimal(0);
            foreach (var array in values)
            {
                var v = ReadAsSqlDecimal(array, i);
                if (v is null)
                {
                    total = null;
                    break;
                }

                total = SafeAdd(total!.Value, v.Value, outputType.Precision);
            }

            if (total is null)
            {
                builder.AppendNull();
                continue;
            }

            builder.Append(total.Value);
        }

        return builder.Build();
    }

    private static SqlDecimal? ReadAsSqlDecimal(IArrowArray array, int index)
    {
        if (array is Decimal128Array d)
        {
            return d.IsNull(index) ? null : d.GetSqlDecimal(index);
        }

        var v = ReadAsDouble(array, index);
        return v is null ? null : new SqlDecimal((decimal)v.Value);
    }

    /// <summary>Adds two <see cref="SqlDecimal"/>s (decimal128's own arithmetic naturally grows
    /// precision by up to a digit, matching decimal addition) and rejects a result that no longer
    /// fits <paramref name="maxPrecision"/> — <see cref="SqlDecimal"/>'s own <c>+</c> operator
    /// already throws <see cref="OverflowException"/> when the TRUE result needs more than its own
    /// 38-digit ceiling, and a result that fits SqlDecimal's ceiling but not this narrower
    /// <paramref name="maxPrecision"/> is caught by the explicit check below.</summary>
    private static SqlDecimal SafeAdd(SqlDecimal a, SqlDecimal b, int maxPrecision)
    {
        SqlDecimal sum;
        try
        {
            sum = a + b;
        }
        catch (OverflowException)
        {
            throw new InvalidOperationException($"value {a} + {b} does not fit in precision {maxPrecision}");
        }

        if (sum.Precision > maxPrecision)
        {
            throw new InvalidOperationException($"value {sum} does not fit in precision {maxPrecision}");
        }

        return sum;
    }
}
