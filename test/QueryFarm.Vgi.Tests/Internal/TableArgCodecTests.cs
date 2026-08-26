using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>Exercises <see cref="TableArgCodec"/> against the exact wire shape
/// <c>vgi_arrow_utils.cpp</c>'s <c>BuildArgumentsFromValues</c> produces: a single <c>args</c>
/// struct column with <c>positional_&lt;i&gt;</c>/<c>named_&lt;key&gt;</c> fields.</summary>
public class TableArgCodecTests
{
    private static byte[] BuildArgsBytes(StructArray argsStruct)
    {
        var schema = new Schema([new Field("args", argsStruct.Data.DataType, nullable: false)], metadata: null);
        var batch = new RecordBatch(schema, [argsStruct], 1);

        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true))
        {
            writer.WriteStart();
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }

        return stream.ToArray();
    }

    [Fact]
    public void Decode_ReadsPositionalAndNamedFields()
    {
        var countBuilder = new Int64Array.Builder();
        countBuilder.Append(5);
        var batchSizeBuilder = new Int64Array.Builder();
        batchSizeBuilder.Append(1000);

        var structType = new StructType(
        [
            new Field("positional_0", Int64Type.Default, nullable: true),
            new Field("named_batch_size", Int64Type.Default, nullable: true),
        ]);
        var structArray = new StructArray(structType, 1, [countBuilder.Build(), batchSizeBuilder.Build()], ArrowBuffer.Empty, nullCount: 0);

        var args = TableArgCodec.Decode(BuildArgsBytes(structArray));

        Assert.Equal(1, args.PositionalCount);
        Assert.Equal(5L, args.Int64(0));
        Assert.Equal(1000L, args.Int64Named("batch_size", -1));
        Assert.Equal(42L, args.Int64Named("missing", 42));
    }

    [Fact]
    public void Decode_DistinguishesOmittedFromExplicitNull()
    {
        var countBuilder = new Int64Array.Builder();
        countBuilder.AppendNull();

        var structType = new StructType([new Field("positional_0", Int64Type.Default, nullable: true)]);
        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(true); // the struct row itself is valid; the FIELD inside it is null
        var structArray = new StructArray(structType, 1, [countBuilder.Build()], validity.Build(), nullCount: 0);

        var args = TableArgCodec.Decode(BuildArgsBytes(structArray));

        // The field is present but its value is SQL NULL.
        Assert.NotNull(args.PositionalArray(0));
        Assert.True(args.PositionalArray(0)!.IsNull(0));
        Assert.Null(args.Positional(0));

        // A field never declared at all is simply absent.
        Assert.Null(args.PositionalArray(1));
        Assert.Null(args.NamedArray("never_declared"));
    }

    [Fact]
    public void Decode_NullOrEmptyBytes_ReturnsEmptyArguments()
    {
        var args = TableArgCodec.Decode(null);
        Assert.Equal(0, args.PositionalCount);
        Assert.Empty(args.NamedArrays);

        var args2 = TableArgCodec.Decode([]);
        Assert.Equal(0, args2.PositionalCount);
    }

    /// <summary>Regression for a real bug found via <c>table/constant_columns_types.test</c>'s
    /// HUGEINT case: DuckDB's <c>arrow_lossless_conversion</c> represents an exotic constant
    /// (HUGEINT, UUID, ...) as a plain physical Arrow type PLUS an <c>ARROW:extension:name</c>
    /// field-metadata annotation — dropping that annotation on decode meant a dynamic-output
    /// ANY-typed function (e.g. <c>constant_columns</c>) could only echo the physical storage type
    /// back (fixed_size_binary(16) — displayed as a raw BLOB), never the original HUGEINT identity.
    /// <see cref="TableArgCodec"/> must preserve the wire struct field's own metadata alongside its
    /// value so a caller can copy it onto its own output field.</summary>
    [Fact]
    public void Decode_PreservesPerArgumentFieldMetadata_ForBothPositionalAndNamed()
    {
        var hugeintMetadata = new Dictionary<string, string>
        {
            ["ARROW:extension:name"] = "DuckDB.hugeint",
            ["ARROW:extension:metadata"] = "",
        };
        var plainBuilder = new Int64Array.Builder();
        plainBuilder.Append(42);
        var hugeintBuilder = new BinaryArray.Builder();
        hugeintBuilder.Append(new byte[16]);

        var structType = new StructType(
        [
            new Field("positional_0", Int64Type.Default, nullable: true),
            new Field("named_seed", BinaryType.Default, nullable: true, hugeintMetadata),
        ]);
        var structArray = new StructArray(
            structType, 1, [plainBuilder.Build(), hugeintBuilder.Build()], ArrowBuffer.Empty, nullCount: 0);

        var args = TableArgCodec.Decode(BuildArgsBytes(structArray));

        Assert.Null(args.PositionalMetadata(0));
        Assert.NotNull(args.NamedMetadata("seed"));
        Assert.Equal("DuckDB.hugeint", args.NamedMetadata("seed")!["ARROW:extension:name"]);

        // A field/index with no metadata at all (or that doesn't exist) reports null, not throw.
        Assert.Null(args.PositionalMetadata(99));
        Assert.Null(args.NamedMetadata("never_declared"));
    }
}
