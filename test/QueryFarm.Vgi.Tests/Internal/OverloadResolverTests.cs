using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// Exercises <see cref="OverloadResolver"/> — the mechanism <c>CatalogRegistry</c> uses to pick one
/// candidate among several functions sharing a name (<c>test/sql/integration/overload/*.test</c>).
/// Uses plain <see cref="Schema"/> instances as the "candidate" type (via the identity selector
/// <c>s =&gt; s</c>) since the resolver's logic never actually needs the wrapping function object.
/// </summary>
public class OverloadResolverTests
{
    private static readonly Dictionary<string, string> ConstMetadata = new() { [VgiWireMetadata.ConstKey] = VgiWireMetadata.ConstTrueValue };
    private static readonly Dictionary<string, string> AnyMetadata = new() { [VgiWireMetadata.TypeKey] = VgiWireMetadata.TypeAnyValue };
    private static readonly Dictionary<string, string> VarargsMetadata = new() { [VgiWireMetadata.VarargsKey] = VgiWireMetadata.VarargsTrueValue };
    private static readonly Dictionary<string, string> NamedMetadata = new() { [VgiWireMetadata.ArgKey] = VgiWireMetadata.ArgNamedValue };

    private static Field ConstField(string name, IArrowType type) => new(name, type, nullable: true, ConstMetadata);

    private static Field ParamField(string name, IArrowType type) => new(name, type, nullable: true);

    private static Field AnyField(string name) => new(name, NullType.Default, nullable: true, AnyMetadata);

    private static Field VarargsField(string name, IArrowType type) => new(name, type, nullable: true, VarargsMetadata);

    private static Field NamedField(string name, IArrowType type) => new(name, type, nullable: true, NamedMetadata);

    /// <summary>Builds the const-argument wire bytes <see cref="ScalarArgCodec.DecodeConstStruct"/>
    /// expects: a single <c>args</c> struct column with <c>positional_&lt;i&gt;</c> fields, one row
    /// — mirrors <c>TableArgCodecTests</c>'s identical helper for the table-argument shape.</summary>
    private static byte[] BuildConstArgsBytes(params (IArrowType Type, object? Value)[] values)
    {
        var fields = values.Select((v, i) => new Field($"positional_{i}", v.Type, nullable: true)).ToList();
        var arrays = values.Select(BuildScalarArray).ToList();
        var structType = new StructType(fields);
        var structArray = new StructArray(structType, 1, arrays, ArrowBuffer.Empty, nullCount: 0);

        var schema = new Schema([new Field("args", structType, nullable: false)], metadata: null);
        var batch = new RecordBatch(schema, [structArray], 1);

        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true))
        {
            writer.WriteStart();
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }

        return stream.ToArray();
    }

    private static IArrowArray BuildScalarArray((IArrowType Type, object? Value) v) => v.Type switch
    {
        Int32Type => Build(new Int32Array.Builder(), (int?)v.Value),
        Int64Type => Build(new Int64Array.Builder(), (long?)v.Value),
        StringType => BuildString((string?)v.Value),
        DoubleType => Build(new DoubleArray.Builder(), (double?)v.Value),
        _ => throw new NotSupportedException(v.Type.ToString()),
    };

    private static IArrowArray Build(Int32Array.Builder b, int? v) { if (v is null) b.AppendNull(); else b.Append(v.Value); return b.Build(); }
    private static IArrowArray Build(Int64Array.Builder b, long? v) { if (v is null) b.AppendNull(); else b.Append(v.Value); return b.Build(); }
    private static IArrowArray Build(DoubleArray.Builder b, double? v) { if (v is null) b.AppendNull(); else b.Append(v.Value); return b.Build(); }
    private static IArrowArray BuildString(string? v) { var b = new StringArray.Builder(); b.Append(v); return b.Build(); }

    // Deliberately NOT built via BuildConstArgsBytes() with zero values — the vendored
    // Apache.Arrow C# StructType constructor rejects a zero-field struct outright (throws
    // ArgumentNullException even though `fields` is merely empty, not null; see NestedType's
    // ctor). An empty byte array is exactly what ScalarArgCodec.DecodeConstStruct already treats
    // as "no const arguments" (short-circuits on `arguments.Length == 0`), so it's the correct
    // stand-in here too.
    private static readonly byte[] NoConstArgs = [];

    [Fact]
    public void SelectScalar_SingleCandidate_ShortCircuitsRegardlessOfArgs()
    {
        var only = new Schema([ParamField("value", DoubleType.Default)], metadata: null);
        var result = OverloadResolver.SelectScalar([only], s => s, NoConstArgs, paramSchema: null, "f");
        Assert.Same(only, result);
    }

    [Fact]
    public void SelectScalar_DistinguishesByConstParamCount()
    {
        // format_number's three overloads: 0/1/2 ConstParams, always exactly 1 Param(value).
        var zero = new Schema([ParamField("value", DoubleType.Default)], metadata: null);
        var one = new Schema([ConstField("precision", Int32Type.Default), ParamField("value", DoubleType.Default)], metadata: null);
        var two = new Schema([ConstField("precision", Int32Type.Default), ConstField("prefix", StringType.Default), ParamField("value", DoubleType.Default)], metadata: null);
        var candidates = new List<Schema> { zero, one, two };

        var paramSchema = new Schema([new Field("value", DoubleType.Default, nullable: true)], metadata: null);

        Assert.Same(zero, OverloadResolver.SelectScalar(candidates, s => s, NoConstArgs, paramSchema, "format_number"));
        Assert.Same(one, OverloadResolver.SelectScalar(candidates, s => s, BuildConstArgsBytes((Int32Type.Default, 2)), paramSchema, "format_number"));
        Assert.Same(two, OverloadResolver.SelectScalar(candidates, s => s, BuildConstArgsBytes((Int32Type.Default, 2), (StringType.Default, "$")), paramSchema, "format_number"));
    }

    [Fact]
    public void SelectScalar_DistinguishesByParamColumnType_NoConstsInvolved()
    {
        // type_info's five overloads: one Param, distinguished purely by its column type.
        var int32Overload = new Schema([ParamField("v", Int32Type.Default)], metadata: null);
        var int64Overload = new Schema([ParamField("v", Int64Type.Default)], metadata: null);
        var strOverload = new Schema([ParamField("v", StringType.Default)], metadata: null);
        var candidates = new List<Schema> { int32Overload, int64Overload, strOverload };

        var int64Input = new Schema([new Field("v", Int64Type.Default, nullable: true)], metadata: null);
        Assert.Same(int64Overload, OverloadResolver.SelectScalar(candidates, s => s, NoConstArgs, int64Input, "type_info"));

        var strInput = new Schema([new Field("v", StringType.Default, nullable: true)], metadata: null);
        Assert.Same(strOverload, OverloadResolver.SelectScalar(candidates, s => s, NoConstArgs, strInput, "type_info"));
    }

    [Fact]
    public void SelectScalar_AnyTypedFieldMatchesAnyActualType()
    {
        // any_mixed's two overloads: ANY first param (always matches), int64 vs string second param.
        var anyInt = new Schema([AnyField("a"), ParamField("b", Int64Type.Default)], metadata: null);
        var anyStr = new Schema([AnyField("a"), ParamField("b", StringType.Default)], metadata: null);
        var candidates = new List<Schema> { anyInt, anyStr };

        // First column resolved as DOUBLE at the call site — irrelevant, the ANY field must match it.
        var doubleThenInt = new Schema([new Field("a", DoubleType.Default, nullable: true), new Field("b", Int64Type.Default, nullable: true)], metadata: null);
        Assert.Same(anyInt, OverloadResolver.SelectScalar(candidates, s => s, NoConstArgs, doubleThenInt, "any_mixed"));

        var doubleThenStr = new Schema([new Field("a", DoubleType.Default, nullable: true), new Field("b", StringType.Default, nullable: true)], metadata: null);
        Assert.Same(anyStr, OverloadResolver.SelectScalar(candidates, s => s, NoConstArgs, doubleThenStr, "any_mixed"));
    }

    [Fact]
    public void SelectScalar_VarargsFieldMatchesEveryRemainingColumnOfItsType()
    {
        // concat_values's two overloads: a single varargs field, int64 vs string, consuming ALL columns.
        var intVarargs = new Schema([VarargsField("values", Int64Type.Default)], metadata: null);
        var strVarargs = new Schema([VarargsField("values", StringType.Default)], metadata: null);
        var candidates = new List<Schema> { intVarargs, strVarargs };

        var threeInts = new Schema(
            [new Field("a", Int64Type.Default, nullable: true), new Field("b", Int64Type.Default, nullable: true), new Field("c", Int64Type.Default, nullable: true)],
            metadata: null);
        Assert.Same(intVarargs, OverloadResolver.SelectScalar(candidates, s => s, NoConstArgs, threeInts, "concat_values"));

        var twoStrings = new Schema([new Field("a", StringType.Default, nullable: true), new Field("b", StringType.Default, nullable: true)], metadata: null);
        Assert.Same(strVarargs, OverloadResolver.SelectScalar(candidates, s => s, NoConstArgs, twoStrings, "concat_values"));

        // Mixed types across the vararg columns match NEITHER overload.
        var mixed = new Schema([new Field("a", Int64Type.Default, nullable: true), new Field("b", StringType.Default, nullable: true)], metadata: null);
        Assert.Throws<InvalidOperationException>(() => OverloadResolver.SelectScalar(candidates, s => s, NoConstArgs, mixed, "concat_values"));
    }

    [Fact]
    public void SelectScalar_NoMatch_Throws()
    {
        var only = new Schema([ParamField("value", Int64Type.Default)], metadata: null);
        var other = new Schema([ParamField("value", StringType.Default)], metadata: null);
        var candidates = new List<Schema> { only, other };

        var boolInput = new Schema([new Field("value", BooleanType.Default, nullable: true)], metadata: null);
        var ex = Assert.Throws<InvalidOperationException>(() => OverloadResolver.SelectScalar(candidates, s => s, NoConstArgs, boolInput, "f"));
        Assert.Contains("no registered overload", ex.Message);
    }

    [Fact]
    public void SelectTable_DistinguishesByPositionalArgCountAndType()
    {
        // make_series-style overloads: 1-arg int64 count, 2-arg (start, stop), 1-arg string CSV.
        var countOverload = new Schema([ParamField("count", Int64Type.Default)], metadata: null);
        var rangeOverload = new Schema([ParamField("start", Int64Type.Default), ParamField("stop", Int64Type.Default)], metadata: null);
        var csvOverload = new Schema([ParamField("csv", StringType.Default)], metadata: null);
        var candidates = new List<Schema> { countOverload, rangeOverload, csvOverload };

        var intArg = Build(new Int64Array.Builder(), 5L);
        var oneIntArgs = new TableArguments([intArg], new Dictionary<string, IArrowArray>());
        Assert.Same(countOverload, OverloadResolver.SelectTable(candidates, s => s, oneIntArgs, "make_series"));

        var strArg = BuildString("1,2,3");
        var oneStrArgs = new TableArguments([strArg], new Dictionary<string, IArrowArray>());
        Assert.Same(csvOverload, OverloadResolver.SelectTable(candidates, s => s, oneStrArgs, "make_series"));

        var twoIntArgs = new TableArguments([intArg, intArg], new Dictionary<string, IArrowArray>());
        Assert.Same(rangeOverload, OverloadResolver.SelectTable(candidates, s => s, twoIntArgs, "make_series"));
    }

    [Fact]
    public void SelectTable_VarargsMatchesRemainingPositionalArgs()
    {
        // repeat_value-style overloads: fixed `count` prefix + varargs tail, int64 vs string.
        var intOverload = new Schema([ParamField("count", Int64Type.Default), VarargsField("values", Int64Type.Default)], metadata: null);
        var strOverload = new Schema([ParamField("count", Int64Type.Default), VarargsField("values", StringType.Default)], metadata: null);
        var candidates = new List<Schema> { intOverload, strOverload };

        var count = Build(new Int64Array.Builder(), 3L);
        var v1 = Build(new Int64Array.Builder(), 10L);
        var v2 = Build(new Int64Array.Builder(), 20L);
        var args = new TableArguments([count, v1, v2], new Dictionary<string, IArrowArray>());

        Assert.Same(intOverload, OverloadResolver.SelectTable(candidates, s => s, args, "repeat_value"));
    }

    [Fact]
    public void SelectTableInOut_SingleCandidate_ShortCircuitsRegardlessOfInputSchema()
    {
        var only = new Schema([ParamField("latitude", DoubleType.Default)], metadata: null);
        Assert.Same(only, OverloadResolver.SelectTableInOut([only], s => s, inputSchema: null, "geo_encode"));
    }

    [Fact]
    public void SelectTableInOut_DistinguishesBlendedOverloadsByPositionalArity()
    {
        // geo_encode's two blended arity overloads: (lat, lon [, precision]) vs (lat, lon, alt [, precision]).
        // The trailing NAMED 'precision' field is identical on both and must be excluded from matching
        // (it never appears in InputSchema — see SelectTableInOut's doc comment).
        var twoArg = new Schema(
            [ParamField("latitude", DoubleType.Default), ParamField("longitude", DoubleType.Default), NamedField("precision", Int64Type.Default)],
            metadata: null);
        var threeArg = new Schema(
            [
                ParamField("latitude", DoubleType.Default), ParamField("longitude", DoubleType.Default),
                ParamField("altitude", DoubleType.Default), NamedField("precision", Int64Type.Default),
            ],
            metadata: null);
        var candidates = new List<Schema> { twoArg, threeArg };

        var twoColumnInput = new Schema(
            [new Field("latitude", DoubleType.Default, nullable: true), new Field("longitude", DoubleType.Default, nullable: true)], metadata: null);
        Assert.Same(twoArg, OverloadResolver.SelectTableInOut(candidates, s => s, twoColumnInput, "geo_encode"));

        var threeColumnInput = new Schema(
            [
                new Field("latitude", DoubleType.Default, nullable: true), new Field("longitude", DoubleType.Default, nullable: true),
                new Field("altitude", DoubleType.Default, nullable: true),
            ],
            metadata: null);
        Assert.Same(threeArg, OverloadResolver.SelectTableInOut(candidates, s => s, threeColumnInput, "geo_encode"));
    }

    [Fact]
    public void SelectTableInOut_NoMatch_Throws()
    {
        var twoArg = new Schema([ParamField("latitude", DoubleType.Default), ParamField("longitude", DoubleType.Default)], metadata: null);
        var threeArg = new Schema(
            [ParamField("latitude", DoubleType.Default), ParamField("longitude", DoubleType.Default), ParamField("altitude", DoubleType.Default)],
            metadata: null);
        var candidates = new List<Schema> { twoArg, threeArg };

        var oneColumnInput = new Schema([new Field("latitude", DoubleType.Default, nullable: true)], metadata: null);
        var ex = Assert.Throws<InvalidOperationException>(
            () => OverloadResolver.SelectTableInOut(candidates, s => s, oneColumnInput, "geo_encode"));
        Assert.Contains("no registered overload", ex.Message);
    }
}
