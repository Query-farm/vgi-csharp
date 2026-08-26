using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>Exercises <see cref="PartitionValuesCodec"/> against the exact wire shape
/// <c>vgi_table_function_impl.cpp</c>'s <c>InstallBatch</c> decodes: a standalone Arrow IPC stream
/// carrying exactly one 2-row RecordBatch (row 0 = min, row 1 = max — identical for
/// SINGLE_VALUE_PARTITIONS) over ONLY the partition-annotated columns, in declared order.</summary>
public class PartitionValuesCodecTests
{
    private static RecordBatch DecodeBatch(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new ArrowStreamReader(stream);
        return reader.ReadNextRecordBatch() ?? throw new InvalidOperationException("no batch");
    }

    [Fact]
    public void EncodeSingleValue_StringColumn_ProducesTwoIdenticalRows()
    {
        var schema = new Schema([new Field("country", StringType.Default, nullable: true)], metadata: null);

        var bytes = PartitionValuesCodec.EncodeSingleValue(schema, ["US"]);
        var batch = DecodeBatch(bytes);

        Assert.Equal(2, batch.Length);
        Assert.Single(batch.Schema.FieldsList);
        Assert.Equal("country", batch.Schema.GetFieldByIndex(0).Name);
        Assert.True(batch.Schema.GetFieldByIndex(0).DataType.Equals(StringType.Default));

        var column = (StringArray)batch.Column(0);
        Assert.Equal("US", column.GetString(0));
        Assert.Equal("US", column.GetString(1));
    }

    [Fact]
    public void EncodeSingleValue_NullValue_RoundTripsAsNull()
    {
        var schema = new Schema([new Field("country", StringType.Default, nullable: true)], metadata: null);

        var bytes = PartitionValuesCodec.EncodeSingleValue(schema, [null]);
        var batch = DecodeBatch(bytes);

        var column = (StringArray)batch.Column(0);
        Assert.True(column.IsNull(0));
        Assert.True(column.IsNull(1));
    }

    [Fact]
    public void EncodeSingleValue_MultiColumn_PreservesDeclaredOrderAndTypes()
    {
        var schema = new Schema(
            [
                new Field("region", StringType.Default, nullable: true),
                new Field("year", Int32Type.Default, nullable: true),
            ],
            metadata: null);

        var bytes = PartitionValuesCodec.EncodeSingleValue(schema, ["US", 2020]);
        var batch = DecodeBatch(bytes);

        Assert.Equal(2, batch.Schema.FieldsList.Count);
        var region = (StringArray)batch.Column(0);
        var year = (Int32Array)batch.Column(1);
        Assert.Equal("US", region.GetString(0));
        Assert.Equal("US", region.GetString(1));
        Assert.Equal(2020, year.GetValue(0));
        Assert.Equal(2020, year.GetValue(1));
    }

    [Fact]
    public void EncodeSingleValueBase64_IsValidBase64OfTheSameBytes()
    {
        var schema = new Schema([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

        var bytes = PartitionValuesCodec.EncodeSingleValue(schema, [42L]);
        var base64 = PartitionValuesCodec.EncodeSingleValueBase64(schema, [42L]);

        Assert.Equal(bytes, Convert.FromBase64String(base64));
    }

    [Fact]
    public void EncodeSingleValue_MismatchedValueCount_Throws()
    {
        var schema = new Schema([new Field("country", StringType.Default, nullable: true)], metadata: null);

        Assert.Throws<ArgumentException>(() => PartitionValuesCodec.EncodeSingleValue(schema, ["US", "extra"]));
    }

    private static Dictionary<string, string> PartitionColumnMetadata() =>
        new() { [VgiWireMetadata.PartitionColumnKey] = VgiWireMetadata.PartitionColumnTrueValue };

    [Fact]
    public void PartitionValues_AutoExtractsMinMaxFromBatch()
    {
        var declared = new Schema(
            [
                new Field("country", StringType.Default, nullable: true, PartitionColumnMetadata()),
                new Field("sales", Int64Type.Default, nullable: false),
            ],
            metadata: null);

        var countryBuilder = new StringArray.Builder();
        countryBuilder.Append("US");
        countryBuilder.Append("US");
        var salesBuilder = new Int64Array.Builder();
        salesBuilder.Append(1L);
        salesBuilder.Append(2L);
        var batch = new RecordBatch(declared, [countryBuilder.Build(), salesBuilder.Build()], 2);

        var metadata = PartitionValuesCodec.PartitionValues(declared, batch);

        Assert.NotNull(metadata);
        var partitionBatch = DecodeBatch(Convert.FromBase64String(metadata!["vgi_partition_values#b64"]));
        Assert.Single(partitionBatch.Schema.FieldsList);
        Assert.Equal("country", partitionBatch.Schema.GetFieldByIndex(0).Name);
        var country = (StringArray)partitionBatch.Column(0);
        Assert.Equal("US", country.GetString(0));
        Assert.Equal("US", country.GetString(1));
    }

    [Fact]
    public void PartitionValues_NoAnnotatedFields_ReturnsNullWhenNoOverride()
    {
        var declared = new Schema([new Field("country", StringType.Default, nullable: true)], metadata: null);
        var builder = new StringArray.Builder();
        builder.Append("US");
        var batch = new RecordBatch(declared, [builder.Build()], 1);

        Assert.Null(PartitionValuesCodec.PartitionValues(declared, batch));
    }

    [Fact]
    public void PartitionValues_NoAnnotatedFields_ExplicitOverride_Throws()
    {
        var declared = new Schema([new Field("country", StringType.Default, nullable: true)], metadata: null);
        var builder = new StringArray.Builder();
        builder.Append("US");
        var batch = new RecordBatch(declared, [builder.Build()], 1);

        var overrides = new Dictionary<string, PartitionValuesCodec.Range> { ["country"] = new("US", "US") };
        var ex = Assert.Throws<ArgumentException>(() => PartitionValuesCodec.PartitionValues(declared, batch, overrides));
        Assert.Contains("partition-annotated fields", ex.Message);
    }

    [Fact]
    public void PartitionValues_AnnotatedColumnAbsentFromBatch_NoOverride_Throws()
    {
        var declared = new Schema(
            [
                new Field("category", StringType.Default, nullable: true, PartitionColumnMetadata()),
                new Field("revenue", Int64Type.Default, nullable: false),
            ],
            metadata: null);
        var batchOnlySchema = new Schema([new Field("revenue", Int64Type.Default, nullable: false)], metadata: null);
        var builder = new Int64Array.Builder();
        builder.Append(1L);
        var batch = new RecordBatch(batchOnlySchema, [builder.Build()], 1);

        var ex = Assert.Throws<ArgumentException>(() => PartitionValuesCodec.PartitionValues(declared, batch));
        Assert.Contains("absent from emitted batch", ex.Message);
    }

    [Fact]
    public void PartitionValues_ExplicitOverride_AllowsMinNotEqualMax()
    {
        // The framework doesn't itself enforce SINGLE_VALUE_PARTITIONS' min==max contract — that's
        // the C++ side's defense-in-depth (see broken_partition_min_neq_max.test).
        var declared = new Schema([new Field("country", StringType.Default, nullable: true, PartitionColumnMetadata())], metadata: null);
        var builder = new StringArray.Builder();
        builder.Append("US");
        var batch = new RecordBatch(declared, [builder.Build()], 1);

        var overrides = new Dictionary<string, PartitionValuesCodec.Range> { ["country"] = new("US", "BR") };
        var metadata = PartitionValuesCodec.PartitionValues(declared, batch, overrides);

        var partitionBatch = DecodeBatch(Convert.FromBase64String(metadata!["vgi_partition_values#b64"]));
        var country = (StringArray)partitionBatch.Column(0);
        Assert.Equal("US", country.GetString(0));
        Assert.Equal("BR", country.GetString(1));
    }

    [Fact]
    public void PartitionValues_EmptyBatch_ReturnsNull()
    {
        var declared = new Schema([new Field("country", StringType.Default, nullable: true, PartitionColumnMetadata())], metadata: null);
        var batch = new RecordBatch(declared, [new StringArray.Builder().Build()], 0);

        Assert.Null(PartitionValuesCodec.PartitionValues(declared, batch));
    }
}
