using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Builds the <c>vgi_partition_values#b64</c> custom_metadata value a <c>SINGLE_VALUE_PARTITIONS</c>
/// (or other <see cref="Protocol.VgiPartitionKind"/>) table function must attach to every non-empty
/// data batch it emits — see <c>vgi_table_function_impl.cpp</c>'s <c>InstallBatch</c>, which THROWS a
/// typed <c>IOException</c> if this is missing/malformed on any batch once
/// <c>FunctionInfo.PartitionKind != NotPartitioned</c>.
///
/// Wire shape: a standalone Arrow IPC stream (schema message + exactly one 2-row RecordBatch + EOS)
/// over ONLY the partition-annotated columns (<see cref="VgiWireMetadata.PartitionColumnKey"/>), in
/// their declared order — row 0 = the partition tuple's MIN value per column, row 1 = MAX (identical
/// for <c>SINGLE_VALUE_PARTITIONS</c>, where a whole batch carries exactly one partition value per
/// column). The C++ side cross-checks column NAME + TYPE (not nullability/metadata) against the
/// declared bind-schema fields at those indices.
/// </summary>
public static class PartitionValuesCodec
{
    /// <summary>A resolved (min, max) pair for one partition column — see <see cref="PartitionValues"/>.
    /// For <c>SINGLE_VALUE_PARTITIONS</c> a correct pair always has <c>Min == Max</c>; the framework
    /// does NOT enforce that itself (see <c>broken_partition_min_neq_max.test</c>'s deliberate
    /// violation) — only the C++ side's <c>InstallBatch</c> defense-in-depth check does, mirroring
    /// vgi-java's <c>EmitMetadata.Range</c>/vgi-python's <c>partition_values=</c> kwarg contract.</summary>
    public readonly record struct Range(object? Min, object? Max);

    /// <summary>Encodes a SINGLE_VALUE_PARTITIONS tuple (row 0 == row 1) for the given partition
    /// columns' schema (a <see cref="Schema"/> containing ONLY the partition-annotated fields, in
    /// declared order) and one value per column (nullable — a partition column may legitimately be
    /// NULL, see <c>cache_partition_parallel</c>'s NULL-country case).</summary>
    public static byte[] EncodeSingleValue(Schema partitionSchema, IReadOnlyList<object?> values)
    {
        if (partitionSchema.FieldsList.Count != values.Count)
        {
            throw new ArgumentException(
                $"partitionSchema has {partitionSchema.FieldsList.Count} field(s) but {values.Count} value(s) were given.",
                nameof(values));
        }

        var arrays = new IArrowArray[partitionSchema.FieldsList.Count];
        for (var i = 0; i < arrays.Length; i++)
        {
            var field = partitionSchema.GetFieldByIndex(i);
            arrays[i] = BuildRangeArray(field, values[i], values[i]);
        }

        return EncodeBatch(partitionSchema, arrays);
    }

    /// <summary>Convenience: <see cref="EncodeSingleValue"/> then base64-encodes for direct use as
    /// the <c>vgi_partition_values#b64</c> metadata dictionary value.</summary>
    public static string EncodeSingleValueBase64(Schema partitionSchema, IReadOnlyList<object?> values) =>
        Convert.ToBase64String(EncodeSingleValue(partitionSchema, values));

    /// <summary>Framework-level <c>out.emit(partition_values=...)</c> helper — ports vgi-java's
    /// <c>EmitMetadata.partitionValues</c>/vgi-python's <c>_merge_partition_values</c>. Given the
    /// function's DECLARED schema (whose <see cref="VgiWireMetadata.PartitionColumnKey"/>-annotated
    /// fields identify the partition columns) and the batch about to be emitted, returns the
    /// <c>vgi_partition_values#b64</c> metadata entry to attach — auto-extracting each partition
    /// column's (min, max) from <paramref name="batch"/> unless <paramref name="explicitValues"/>
    /// overrides it by column name. Returns <see langword="null"/> when there is nothing to attach
    /// (no partition-annotated fields, or a 0-row batch — the C++ side exempts both). Throws
    /// <see cref="ArgumentException"/> — caught client-side, before the wire — for two contract
    /// violations: <paramref name="explicitValues"/> supplied on a non-partitioned schema, or an
    /// annotated column missing from <paramref name="batch"/> with no override for it. Does NOT
    /// itself enforce <c>min == max</c> for <c>SINGLE_VALUE_PARTITIONS</c> — that check is the C++
    /// side's defense-in-depth (see <see cref="Range"/>'s doc comment).</summary>
    public static IReadOnlyDictionary<string, string>? PartitionValues(
        Schema declaredSchema, RecordBatch batch, IReadOnlyDictionary<string, Range>? explicitValues = null)
    {
        var partitionFields = declaredSchema.FieldsList
            .Where(f => f.Metadata is not null
                && f.Metadata.TryGetValue(VgiWireMetadata.PartitionColumnKey, out var v)
                && v == VgiWireMetadata.PartitionColumnTrueValue)
            .ToList();

        if (partitionFields.Count == 0)
        {
            if (explicitValues is not null)
            {
                throw new ArgumentException(
                    "out.emit(partition_values=...) requires partition-annotated fields in the bind schema.");
            }

            return null;
        }

        if (batch.Length == 0)
        {
            return null;
        }

        var partitionSchema = new Schema(partitionFields, metadata: null);
        var arrays = new IArrowArray[partitionFields.Count];
        for (var i = 0; i < partitionFields.Count; i++)
        {
            var field = partitionFields[i];
            var range = ResolveRange(field, batch, explicitValues);
            arrays[i] = BuildRangeArray(field, range.Min, range.Max);
        }

        var bytes = EncodeBatch(partitionSchema, arrays);
        return new Dictionary<string, string> { ["vgi_partition_values#b64"] = Convert.ToBase64String(bytes) };
    }

    private static Range ResolveRange(Field field, RecordBatch batch, IReadOnlyDictionary<string, Range>? explicitValues)
    {
        if (explicitValues is not null && explicitValues.TryGetValue(field.Name, out var overridden))
        {
            return overridden;
        }

        var index = batch.Schema.GetFieldIndex(field.Name);
        if (index < 0)
        {
            throw new ArgumentException(
                $"column '{field.Name}' is partition-annotated but absent from emitted batch; pass an explicit partition_values override.");
        }

        return ScanMinMax(batch.Column(index));
    }

    private static Range ScanMinMax(IArrowArray array)
    {
        object? min = null;
        object? max = null;
        switch (array)
        {
            case StringArray s:
                for (var i = 0; i < s.Length; i++)
                {
                    if (s.IsNull(i))
                    {
                        continue;
                    }

                    var v = s.GetString(i);
                    if (min is null || string.CompareOrdinal(v, (string)min) < 0)
                    {
                        min = v;
                    }

                    if (max is null || string.CompareOrdinal(v, (string)max) > 0)
                    {
                        max = v;
                    }
                }

                break;
            case Int64Array l:
                for (var i = 0; i < l.Length; i++)
                {
                    if (l.IsNull(i))
                    {
                        continue;
                    }

                    var v = l.GetValue(i)!.Value;
                    if (min is null || v < (long)min)
                    {
                        min = v;
                    }

                    if (max is null || v > (long)max)
                    {
                        max = v;
                    }
                }

                break;
            case Int32Array ia:
                for (var i = 0; i < ia.Length; i++)
                {
                    if (ia.IsNull(i))
                    {
                        continue;
                    }

                    var v = ia.GetValue(i)!.Value;
                    if (min is null || v < (int)min)
                    {
                        min = v;
                    }

                    if (max is null || v > (int)max)
                    {
                        max = v;
                    }
                }

                break;
            default:
                throw new NotSupportedException(
                    $"PartitionValuesCodec.ScanMinMax: unsupported partition column array type '{array.GetType().Name}'.");
        }

        return new Range(min, max);
    }

    private static byte[] EncodeBatch(Schema partitionSchema, IArrowArray[] arrays)
    {
        var batch = new RecordBatch(partitionSchema, arrays, 2);

        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, partitionSchema, leaveOpen: true))
        {
            writer.WriteStart();
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }

        return stream.ToArray();
    }

    private static IArrowArray BuildRangeArray(Field field, object? min, object? max)
    {
        switch (field.DataType)
        {
            case Apache.Arrow.Types.StringType:
                var sb = new StringArray.Builder();
                AppendOrNull(sb, min as string);
                AppendOrNull(sb, max as string);
                return sb.Build();
            case Apache.Arrow.Types.Int64Type:
                var lb = new Int64Array.Builder();
                AppendOrNull(lb, min is long lMin ? lMin : (long?)null);
                AppendOrNull(lb, max is long lMax ? lMax : (long?)null);
                return lb.Build();
            case Apache.Arrow.Types.Int32Type:
                var ib = new Int32Array.Builder();
                AppendOrNull(ib, min is int iMin ? iMin : (int?)null);
                AppendOrNull(ib, max is int iMax ? iMax : (int?)null);
                return ib.Build();
            default:
                throw new NotSupportedException(
                    $"PartitionValuesCodec.BuildRangeArray: unsupported partition column type '{field.DataType}' for field '{field.Name}'.");
        }
    }

    private static void AppendOrNull(StringArray.Builder builder, string? value)
    {
        if (value is null)
        {
            builder.AppendNull();
        }
        else
        {
            builder.Append(value);
        }
    }

    private static void AppendOrNull(Int64Array.Builder builder, long? value)
    {
        if (value is null)
        {
            builder.AppendNull();
        }
        else
        {
            builder.Append(value.Value);
        }
    }

    private static void AppendOrNull(Int32Array.Builder builder, int? value)
    {
        if (value is null)
        {
            builder.AppendNull();
        }
        else
        {
            builder.Append(value.Value);
        }
    }
}
