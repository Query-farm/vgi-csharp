using Apache.Arrow.Types;

namespace QueryFarm.Vgi.Types;

/// <summary>
/// Numeric-promotion rules ported from vgi-java's <c>farm.query.vgi.types.TypeRules</c> (itself
/// mirroring vgi-python's <c>_promote_for_addition</c>/common-type helpers) — used by any scalar
/// fixture whose output type is derived dynamically from its ("any"-typed) input type(s) rather
/// than declared statically (<c>double</c>, <c>add_values</c>, <c>sum_values</c>).
///
/// The core idea in every rule below: promote for OVERFLOW HEADROOM, not just "the wider of the
/// inputs" — doubling/adding two N-bit integers can overflow N bits, so the result gets the next
/// tier up (capped at 64-bit); float always lands on float64; decimal gains exactly one more digit
/// of precision (capped at Arrow's decimal128 38-digit ceiling).
/// </summary>
public static class TypeRules
{
    public static bool IsInteger(IArrowType type) => type is IntegerType;

    public static bool IsFloating(IArrowType type) => type is FloatingPointType;

    public static bool IsNumeric(IArrowType type) => IsInteger(type) || IsFloating(type);

    public static bool IsAddable(IArrowType type) => IsNumeric(type) || type is Decimal128Type;

    /// <summary>Promotes a single input type one tier up — the rule <c>double(value)</c> uses.</summary>
    public static IArrowType PromoteForAddition(IArrowType type)
    {
        if (type is IntegerType i)
        {
            var next = Math.Min(64, i.BitWidth * 2);
            return IntType(next, i.IsSigned);
        }

        if (IsFloating(type))
        {
            return DoubleType.Default;
        }

        if (type is Decimal128Type d)
        {
            var newPrecision = Math.Min(d.Precision + 1, 38);
            return new Decimal128Type(newPrecision, d.Scale);
        }

        return Int64Type.Default;
    }

    /// <summary>Common type of two operands for addition (<c>add_values(a, b)</c>): float wins
    /// outright; both-integer widens the WIDER operand one tier (capped at 64); decimal (or a
    /// mixed int/decimal pair) merges precision/scale via the DuckDB decimal-add rule, then adds
    /// two digits of headroom, capped at 38.</summary>
    public static IArrowType CommonTypeForAddition(IArrowType a, IArrowType b)
    {
        if (IsFloating(a) || IsFloating(b))
        {
            return DoubleType.Default;
        }

        if (a is IntegerType ai && b is IntegerType bi)
        {
            var width = Math.Max(ai.BitWidth, bi.BitWidth);
            var next = Math.Min(64, width * 2);
            return IntType(next, ai.IsSigned || bi.IsSigned);
        }

        if (a is Decimal128Type || b is Decimal128Type)
        {
            var da = ToDecimal(a);
            var db = ToDecimal(b);
            var scale = Math.Max(da.Scale, db.Scale);
            var whole = Math.Max(da.Precision - da.Scale, db.Precision - db.Scale);
            var precision = Math.Min(38, whole + scale + 1 + 1);
            return new Decimal128Type(precision, Math.Min(scale, precision));
        }

        return Int64Type.Default;
    }

    /// <summary>Widest-of-N-operand promotion for varargs addition (<c>sum_values(...)</c>):
    /// scans left to right, floating wins immediately, otherwise keeps the widest integer seen,
    /// then applies <see cref="PromoteForAddition"/> to the final widest type.</summary>
    public static IArrowType CommonTypeForAddition(IReadOnlyList<IArrowType> types)
    {
        if (types.Count == 0)
        {
            throw new ArgumentException("At least one type is required.", nameof(types));
        }

        var widest = types[0];
        for (var i = 1; i < types.Count; i++)
        {
            var candidate = types[i];
            if (IsFloating(candidate) && !IsFloating(widest))
            {
                widest = candidate;
            }
            else if (widest is IntegerType wi && candidate is IntegerType ci && ci.BitWidth > wi.BitWidth)
            {
                widest = candidate;
            }
        }

        return PromoteForAddition(widest);
    }

    private static Decimal128Type ToDecimal(IArrowType type)
    {
        if (type is Decimal128Type d)
        {
            return d;
        }

        var bitWidth = type is IntegerType i ? i.BitWidth : 64;
        var digits = bitWidth switch
        {
            8 => 3,
            16 => 5,
            32 => 10,
            _ => 19,
        };

        return new Decimal128Type(digits, 0);
    }

    private static IntegerType IntType(int bitWidth, bool signed) => (bitWidth, signed) switch
    {
        (8, true) => Int8Type.Default,
        (8, false) => UInt8Type.Default,
        (16, true) => Int16Type.Default,
        (16, false) => UInt16Type.Default,
        (32, true) => Int32Type.Default,
        (32, false) => UInt32Type.Default,
        (64, true) => Int64Type.Default,
        (64, false) => UInt64Type.Default,
        _ => throw new ArgumentOutOfRangeException(nameof(bitWidth), bitWidth, "Unsupported integer width."),
    };
}
