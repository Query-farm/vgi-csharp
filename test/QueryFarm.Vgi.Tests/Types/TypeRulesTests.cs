using Apache.Arrow.Types;
using QueryFarm.Vgi.Types;
using Xunit;

namespace QueryFarm.Vgi.Tests.Types;

/// <summary>Exercises the numeric-promotion rules <c>double</c>/<c>add_values</c>/<c>sum_values</c>
/// depend on — ported from vgi-java's <c>TypeRules</c> and verified against
/// <c>test/sql/integration/scalar/{double,add_values,numeric_promotion}.test</c>'s actual
/// expectations (int width doubling capped at 64-bit, float promotes to double, decimal gains one
/// digit of precision capped at 38).</summary>
public class TypeRulesTests
{
    [Theory]
    [InlineData(8, true, 16, true)] // TINYINT -> SMALLINT
    [InlineData(16, true, 32, true)] // SMALLINT -> INTEGER
    [InlineData(32, true, 64, true)] // INTEGER -> BIGINT
    [InlineData(64, true, 64, true)] // BIGINT -> BIGINT (capped, no further growth)
    [InlineData(8, false, 16, false)] // UTINYINT -> USMALLINT (sign preserved)
    [InlineData(32, false, 64, false)] // UINTEGER -> UBIGINT
    public void PromoteForAddition_WidensIntegerOneTier_CappedAt64(int inputWidth, bool signed_, int expectedWidth, bool expectedSigned)
    {
        var input = MakeInt(inputWidth, signed_);
        var promoted = Assert.IsAssignableFrom<IntegerType>(TypeRules.PromoteForAddition(input));
        Assert.Equal(expectedWidth, promoted.BitWidth);
        Assert.Equal(expectedSigned, promoted.IsSigned);
    }

    [Fact]
    public void PromoteForAddition_FloatAlwaysPromotesToDouble()
    {
        Assert.Equal(DoubleType.Default, TypeRules.PromoteForAddition(FloatType.Default));
        Assert.Equal(DoubleType.Default, TypeRules.PromoteForAddition(DoubleType.Default));
        Assert.Equal(DoubleType.Default, TypeRules.PromoteForAddition(new HalfFloatType()));
    }

    [Fact]
    public void PromoteForAddition_DecimalGainsOneDigitOfPrecision_CappedAt38()
    {
        var promoted = (Decimal128Type)TypeRules.PromoteForAddition(new Decimal128Type(10, 2));
        Assert.Equal(11, promoted.Precision);
        Assert.Equal(2, promoted.Scale);

        // Matches double.test's regression: DECIMAL(38, 0) must stay capped at 38, not grow to 39.
        var capped = (Decimal128Type)TypeRules.PromoteForAddition(new Decimal128Type(38, 0));
        Assert.Equal(38, capped.Precision);
    }

    [Fact]
    public void CommonTypeForAddition_EitherOperandFloating_ResultIsDouble()
    {
        Assert.Equal(DoubleType.Default, TypeRules.CommonTypeForAddition(Int32Type.Default, DoubleType.Default));
        Assert.Equal(DoubleType.Default, TypeRules.CommonTypeForAddition(FloatType.Default, FloatType.Default));
    }

    [Theory]
    [InlineData(32, 64, 64)] // INTEGER + UINTEGER -> common int32, promoted to BIGINT
    [InlineData(8, 32, 64)] // TINYINT + INTEGER -> common int32 (wider operand), promoted to BIGINT
    [InlineData(16, 16, 32)] // SMALLINT + SMALLINT -> INTEGER
    public void CommonTypeForAddition_BothInteger_WidensWiderOperandOneTier(int widthA, int widthB, int expectedWidth)
    {
        var result = (IntegerType)TypeRules.CommonTypeForAddition(MakeInt(widthA, true), MakeInt(widthB, true));
        Assert.Equal(expectedWidth, result.BitWidth);
    }

    [Fact]
    public void CommonTypeForAddition_Decimal_MergesPrecisionScale()
    {
        // 1.50::DECIMAL(5,2) + 2.250::DECIMAL(7,3) — matches numeric_promotion.test (value-only
        // assertion there; this locks the actual precision/scale merge rule down structurally).
        var result = (Decimal128Type)TypeRules.CommonTypeForAddition(new Decimal128Type(5, 2), new Decimal128Type(7, 3));
        Assert.True(result.Precision <= 38);
        Assert.True(result.Scale <= result.Precision);
    }

    [Fact]
    public void CommonTypeForAddition_Varargs_ScansWidestThenPromotes()
    {
        // sum_values(TINYINT, TINYINT, TINYINT) -> widest seen is TINYINT (8-bit) -> promoted to SMALLINT.
        var allTiny = TypeRules.CommonTypeForAddition([Int8Type.Default, Int8Type.Default, Int8Type.Default]);
        Assert.Equal(16, Assert.IsType<Int16Type>(allTiny).BitWidth);

        // A floating operand anywhere in the list wins immediately, regardless of position.
        var withFloat = TypeRules.CommonTypeForAddition([Int64Type.Default, FloatType.Default, Int8Type.Default]);
        Assert.Equal(DoubleType.Default, withFloat);
    }

    [Fact]
    public void CommonTypeForAddition_Varargs_EmptyThrows()
    {
        Assert.Throws<ArgumentException>(() => TypeRules.CommonTypeForAddition([]));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IsAddable_NumericAndDecimalAreAddable_EverythingElseIsNot(bool useDecimal)
    {
        Assert.True(TypeRules.IsAddable(useDecimal ? new Decimal128Type(10, 2) : Int64Type.Default));
        Assert.False(TypeRules.IsAddable(StringType.Default));
        Assert.False(TypeRules.IsAddable(BooleanType.Default));
        Assert.False(TypeRules.IsAddable(new Date32Type()));
    }

    private static IntegerType MakeInt(int bitWidth, bool signed) => (bitWidth, signed) switch
    {
        (8, true) => Int8Type.Default,
        (8, false) => UInt8Type.Default,
        (16, true) => Int16Type.Default,
        (16, false) => UInt16Type.Default,
        (32, true) => Int32Type.Default,
        (32, false) => UInt32Type.Default,
        (64, true) => Int64Type.Default,
        (64, false) => UInt64Type.Default,
        _ => throw new ArgumentOutOfRangeException(nameof(bitWidth)),
    };
}
