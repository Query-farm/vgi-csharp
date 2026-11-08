using Apache.Arrow;
using Apache.Arrow.Types;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Encodes a set of per-column statistics as the RAW (embedded-IPC, NOT strictly schema-validated —
/// see <c>vgi_schema_registry.cpp</c>'s "dynamic" response entries) wire batch both
/// <c>catalog_table_column_statistics_get</c> and <c>table_function_statistics</c> return, and
/// <see cref="Protocol.TableInfo.ColumnStatistics"/> inlines: one row per column,
/// <c>(column_name, min, max, has_null, has_not_null, distinct_count, contains_unicode,
/// max_string_length)</c> — <c>min</c>/<c>max</c> are a small closed SPARSE UNION of scalar Arrow
/// types (covers every value type this worker's fixtures need) so a single batch can carry stats
/// for columns of different types. Mirrors <c>vgi_catalog_api.cpp</c>'s <c>ParseColumnStatisticsBatch</c>
/// (the C++ reader), which only requires the six non-optional fields by NAME — the exact union
/// member layout is this worker's own choice, never cross-checked by the C++ side.
/// </summary>
public static class ColumnStatisticsCodec
{
    private const byte Int64TypeId = 0;
    private const byte DoubleTypeId = 1;
    private const byte StringTypeId = 2;
    private const byte BoolTypeId = 3;
    private const byte BinaryTypeId = 4;
    private const byte Date32TypeId = 5;

    private static readonly UnionType ValueUnionType = new(
        [
            new Field("i", Int64Type.Default, nullable: true),
            new Field("f", DoubleType.Default, nullable: true),
            new Field("s", StringType.Default, nullable: true),
            new Field("b", BooleanType.Default, nullable: true),
            new Field("bin", BinaryType.Default, nullable: true),
            new Field("d", Date32Type.Default, nullable: true),
        ],
        [Int64TypeId, DoubleTypeId, StringTypeId, BoolTypeId, BinaryTypeId, Date32TypeId],
        UnionMode.Sparse);

    private static readonly Schema BatchSchema = new(
        [
            new Field("column_name", StringType.Default, nullable: false),
            new Field("min", ValueUnionType, nullable: true),
            new Field("max", ValueUnionType, nullable: true),
            new Field("has_null", BooleanType.Default, nullable: false),
            new Field("has_not_null", BooleanType.Default, nullable: false),
            new Field("distinct_count", Int64Type.Default, nullable: true),
            new Field("contains_unicode", BooleanType.Default, nullable: true),
            new Field("max_string_length", UInt64Type.Default, nullable: true),
        ],
        metadata: null);

    /// <summary>Encodes <paramref name="rows"/> (in the given order — callers typically pass a
    /// table/function's own column order) as an embedded-IPC batch. An empty <paramref name="rows"/>
    /// encodes as an empty (zero-row) batch — the C++ side treats that the same as "no stats".</summary>
    public static byte[] Encode(
        IReadOnlyList<(string ColumnName, Catalog.ColumnStatisticsInput Stats)> rows,
        long? cacheMaxAgeSeconds = null)
    {
        var nameBuilder = new StringArray.Builder();
        var hasNullBuilder = new BooleanArray.Builder();
        var hasNotNullBuilder = new BooleanArray.Builder();
        var distinctBuilder = new Int64Array.Builder();
        var unicodeBuilder = new BooleanArray.Builder();
        var maxLenBuilder = new UInt64Array.Builder();

        var minTypeIds = new byte[rows.Count];
        var maxTypeIds = new byte[rows.Count];
        var minChildren = new UnionValueBuilders();
        var maxChildren = new UnionValueBuilders();

        for (var i = 0; i < rows.Count; i++)
        {
            var (columnName, stats) = rows[i];
            nameBuilder.Append(columnName);
            hasNullBuilder.Append(stats.HasNull);
            hasNotNullBuilder.Append(stats.HasNotNull);
            if (stats.DistinctCount is { } dc)
            {
                distinctBuilder.Append(dc);
            }
            else
            {
                distinctBuilder.AppendNull();
            }

            if (stats.ContainsUnicode is { } cu)
            {
                unicodeBuilder.Append(cu);
            }
            else
            {
                unicodeBuilder.AppendNull();
            }

            if (stats.MaxStringLength is { } msl)
            {
                maxLenBuilder.Append((ulong)msl);
            }
            else
            {
                maxLenBuilder.AppendNull();
            }

            minTypeIds[i] = AppendUnionValue(minChildren, stats.Min);
            maxTypeIds[i] = AppendUnionValue(maxChildren, stats.Max);
        }

        var minArray = new SparseUnionArray(
            ValueUnionType, rows.Count, minChildren.Build(), new ArrowBuffer.Builder<byte>().AppendRange(minTypeIds).Build());
        var maxArray = new SparseUnionArray(
            ValueUnionType, rows.Count, maxChildren.Build(), new ArrowBuffer.Builder<byte>().AppendRange(maxTypeIds).Build());

        using var batch = new RecordBatch(
            BatchSchema,
            [
                nameBuilder.Build(),
                minArray,
                maxArray,
                hasNullBuilder.Build(),
                hasNotNullBuilder.Build(),
                distinctBuilder.Build(),
                unicodeBuilder.Build(),
                maxLenBuilder.Build(),
            ],
            rows.Count);

        using var stream = new MemoryStream();
        RecordBatchIpc.Write(
            stream,
            batch,
            cacheMaxAgeSeconds is { } ttl
                ? new Dictionary<string, string> { ["cache_max_age_seconds"] = ttl.ToString(System.Globalization.CultureInfo.InvariantCulture) }
                : null);
        return stream.ToArray();
    }

    /// <summary>
    /// Decodes a column-statistics batch — the inverse of <see cref="Encode"/>, and also the layout
    /// vgi-python's <c>serialize_column_statistics</c> emits (a sparse union with one member per
    /// distinct value type, in any order). Returns rows in wire order; <c>Min</c>/<c>Max</c> are
    /// boxed CLR scalars (<see cref="long"/>, <see cref="double"/>, <see cref="string"/>,
    /// <see cref="bool"/>, <see cref="byte"/>[], <see cref="DateOnly"/>, ...), <see langword="null"/>
    /// when unknown. Empty/null bytes decode to no rows.
    /// </summary>
    public static IReadOnlyList<(string ColumnName, Catalog.ColumnStatisticsInput Stats)> Decode(byte[]? bytes)
    {
        var rows = new List<(string, Catalog.ColumnStatisticsInput)>();
        if (bytes is null || bytes.Length == 0)
        {
            return rows;
        }

        using var stream = new MemoryStream(bytes, writable: false);
        using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(stream);
        while (reader.ReadNextRecordBatch() is { } batch)
        {
            using (batch)
            {
                var names = (StringArray)Column(batch, "column_name")!;
                var mins = Column(batch, "min");
                var maxs = Column(batch, "max");
                var hasNull = (BooleanArray)Column(batch, "has_null")!;
                var hasNotNull = (BooleanArray)Column(batch, "has_not_null")!;
                var distinct = Column(batch, "distinct_count") as Int64Array;
                var unicode = Column(batch, "contains_unicode") as BooleanArray;
                var maxLength = Column(batch, "max_string_length");
                for (var i = 0; i < batch.Length; i++)
                {
                    rows.Add((names.GetString(i)!, new Catalog.ColumnStatisticsInput
                    {
                        Min = UnionValue(mins, i),
                        Max = UnionValue(maxs, i),
                        HasNull = hasNull.GetValue(i) ?? true,
                        HasNotNull = hasNotNull.GetValue(i) ?? true,
                        DistinctCount = distinct?.GetValue(i),
                        ContainsUnicode = unicode?.GetValue(i),
                        MaxStringLength = maxLength is null || maxLength.IsNull(i)
                            ? null
                            : Convert.ToInt64(ScalarArgCodec.ReadScalar(maxLength, i), System.Globalization.CultureInfo.InvariantCulture),
                    }));
                }
            }
        }

        return rows;
    }

    private static IArrowArray? Column(RecordBatch batch, string name)
    {
        var index = batch.Schema.GetFieldIndex(name);
        return index < 0 ? null : batch.Column(index);
    }

    private static object? UnionValue(IArrowArray? array, int index)
    {
        if (array is not UnionArray union || union.IsNull(index))
        {
            return null;
        }

        var type = (UnionType)union.Data.DataType;
        var typeId = union.TypeIds[index];
        var child = System.Array.IndexOf(type.TypeIds, (int)typeId);
        if (child < 0)
        {
            return null;
        }

        var childIndex = union.Mode == UnionMode.Sparse ? index : ((DenseUnionArray)union).ValueOffsets[index];
        var values = union.Fields[child];
        return values is NullArray ? null : ScanPlanningEvaluator.ReadValue(values, childIndex);
    }

    /// <summary>Appends <paramref name="value"/> to whichever union-member builder matches its CLR
    /// type (NULL values are still placed in the int64 member — the value itself carries no
    /// validity bit for a NULL min/max, but the caller conditions on <see cref="Catalog.ColumnStatisticsInput.Min"/>/
    /// <see cref="Catalog.ColumnStatisticsInput.Max"/> being <see langword="null"/> so which member
    /// is "active" doesn't matter) and returns the matching type id — every other member gets a null
    /// appended too, since a sparse union requires every child array to have the parent's length.</summary>
    private static byte AppendUnionValue(UnionValueBuilders builders, object? value)
    {
        byte typeId;
        switch (value)
        {
            case null:
                typeId = Int64TypeId;
                builders.Int64.AppendNull();
                break;
            case long l:
                typeId = Int64TypeId;
                builders.Int64.Append(l);
                break;
            case int iv:
                typeId = Int64TypeId;
                builders.Int64.Append(iv);
                break;
            case double d:
                typeId = DoubleTypeId;
                builders.Double.Append(d);
                break;
            case float f:
                typeId = DoubleTypeId;
                builders.Double.Append(f);
                break;
            case string s:
                typeId = StringTypeId;
                builders.String.Append(s);
                break;
            case bool b:
                typeId = BoolTypeId;
                builders.Bool.Append(b);
                break;
            case byte[] bin:
                typeId = BinaryTypeId;
                builders.Binary.Append(bin);
                break;
            case DateOnly date:
                typeId = Date32TypeId;
                builders.Date32.Append(date);
                break;
            default:
                throw new ArgumentException($"Unsupported column-statistics value type '{value.GetType()}'.", nameof(value));
        }

        if (typeId != Int64TypeId)
        {
            builders.Int64.AppendNull();
        }

        if (typeId != DoubleTypeId)
        {
            builders.Double.AppendNull();
        }

        if (typeId != StringTypeId)
        {
            builders.String.AppendNull();
        }

        if (typeId != BoolTypeId)
        {
            builders.Bool.AppendNull();
        }

        if (typeId != BinaryTypeId)
        {
            builders.Binary.AppendNull();
        }

        if (typeId != Date32TypeId)
        {
            builders.Date32.AppendNull();
        }

        return typeId;
    }

    private sealed class UnionValueBuilders
    {
        public Int64Array.Builder Int64 { get; } = new();

        public DoubleArray.Builder Double { get; } = new();

        public StringArray.Builder String { get; } = new();

        public BooleanArray.Builder Bool { get; } = new();

        public BinaryArray.Builder Binary { get; } = new();

        public Date32Array.Builder Date32 { get; } = new();

        public IArrowArray[] Build() =>
            [Int64.Build(), Double.Build(), String.Build(), Bool.Build(), Binary.Build(), Date32.Build()];
    }
}
