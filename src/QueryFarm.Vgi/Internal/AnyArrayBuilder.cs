using Apache.Arrow;
using Apache.Arrow.Types;

namespace QueryFarm.Vgi.Internal;

/// <summary>Builds an Arrow array of a given (dynamically resolved) primitive type from a list of
/// boxed CLR values (as produced by <see cref="ScalarArgCodec.ReadScalar"/>) — the write-side
/// counterpart used where a function's output element type isn't known until bind time (e.g.
/// <c>unnest_tensor</c>'s cell/axis-coordinate arrays).</summary>
public static class AnyArrayBuilder
{
    public static IArrowArray Build(IArrowType type, IReadOnlyList<object?> values)
    {
        switch (type)
        {
            case Int8Type:
                {
                    var b = new Int8Array.Builder();
                    foreach (var v in values) b.Append(v is null ? (sbyte?)null : Convert.ToSByte(v));
                    return b.Build();
                }

            case Int16Type:
                {
                    var b = new Int16Array.Builder();
                    foreach (var v in values) b.Append(v is null ? (short?)null : Convert.ToInt16(v));
                    return b.Build();
                }

            case Int32Type:
                {
                    var b = new Int32Array.Builder();
                    foreach (var v in values) b.Append(v is null ? (int?)null : Convert.ToInt32(v));
                    return b.Build();
                }

            case Int64Type:
                {
                    var b = new Int64Array.Builder();
                    foreach (var v in values) b.Append(v is null ? (long?)null : Convert.ToInt64(v));
                    return b.Build();
                }

            case UInt8Type:
                {
                    var b = new UInt8Array.Builder();
                    foreach (var v in values) b.Append(v is null ? (byte?)null : Convert.ToByte(v));
                    return b.Build();
                }

            case UInt16Type:
                {
                    var b = new UInt16Array.Builder();
                    foreach (var v in values) b.Append(v is null ? (ushort?)null : Convert.ToUInt16(v));
                    return b.Build();
                }

            case UInt32Type:
                {
                    var b = new UInt32Array.Builder();
                    foreach (var v in values) b.Append(v is null ? (uint?)null : Convert.ToUInt32(v));
                    return b.Build();
                }

            case UInt64Type:
                {
                    var b = new UInt64Array.Builder();
                    foreach (var v in values) b.Append(v is null ? (ulong?)null : Convert.ToUInt64(v));
                    return b.Build();
                }

            case FloatType:
                {
                    var b = new FloatArray.Builder();
                    foreach (var v in values) b.Append(v is null ? (float?)null : Convert.ToSingle(v));
                    return b.Build();
                }

            case DoubleType:
                {
                    var b = new DoubleArray.Builder();
                    foreach (var v in values) b.Append(v is null ? (double?)null : Convert.ToDouble(v));
                    return b.Build();
                }

            case StringType:
                {
                    var b = new StringArray.Builder();
                    foreach (var v in values)
                    {
                        if (v is null) b.AppendNull();
                        else b.Append((string)v);
                    }

                    return b.Build();
                }

            case BooleanType:
                {
                    var b = new BooleanArray.Builder();
                    foreach (var v in values)
                    {
                        if (v is null) b.AppendNull();
                        else b.Append((bool)v);
                    }

                    return b.Build();
                }

            case BinaryType:
                {
                    var b = new BinaryArray.Builder();
                    foreach (var v in values)
                    {
                        if (v is null) b.AppendNull();
                        else b.Append((byte[])v);
                    }

                    return b.Build();
                }

            default:
                throw new NotSupportedException($"AnyArrayBuilder: unsupported dynamic array type '{type}'.");
        }
    }
}
