using Apache.Arrow;
using Apache.Arrow.Types;

namespace QueryFarm.Vgi.ExampleWorker.AttachOptions;

/// <summary>One declared ATTACH-time option: its wire type plus the single-element default array
/// <see cref="AttachOptionSpecBuilder"/> embeds AND <see cref="EchoAttachOptionsFunction"/> falls
/// back to for an option the caller didn't supply.</summary>
internal readonly record struct AttachOptionEntry(string Name, string Description, IArrowType Type, IArrowArray Default);

/// <summary>
/// The 19 typed ATTACH-time options <c>attach/attach_options_echo.test</c> round-trips — one per
/// supported Arrow/DuckDB scalar/nested type. Declaration order is discovery/echo-schema order.
/// Values mirror vgi-python's own <c>AttachOptions</c> fixture class defaults exactly (this is a
/// cross-SDK conformance fixture — the same test file runs against every language's worker).
/// </summary>
internal static class AttachOptionEntries
{
    public static readonly IReadOnlyList<AttachOptionEntry> All = Build();

    private static List<AttachOptionEntry> Build()
    {
        var listItemField = new Field("item", Int64Type.Default, nullable: true);
        var listType = new ListType(listItemField);
        var listBuilder = new ListArray.Builder(listItemField);
        listBuilder.Append();
        ((Int64Array.Builder)listBuilder.ValueBuilder).Append(1).Append(2).Append(3);
        var listDefault = listBuilder.Build();

        var structType = new StructType(
        [
            new Field("a", Int64Type.Default, nullable: true),
            new Field("b", StringType.Default, nullable: true),
        ]);
        var structDefault = new StructArray(
            structType,
            length: 1,
            [new Int64Array.Builder().Append(1).Build(), new StringArray.Builder().Append("x").Build()],
            new ArrowBuffer.BitmapBuilder().Append(true).Build());

        var decimalType = new Decimal128Type(precision: 18, scale: 4);

        return
        [
            new("opt_bool", "Boolean option", BooleanType.Default, new BooleanArray.Builder().Append(true).Build()),
            new("opt_int8", "int8", Int8Type.Default, new Int8Array.Builder().Append(-8).Build()),
            new("opt_int16", "int16", Int16Type.Default, new Int16Array.Builder().Append(-16).Build()),
            new("opt_int32", "int32", Int32Type.Default, new Int32Array.Builder().Append(-32).Build()),
            new("opt_int64", "int64", Int64Type.Default, new Int64Array.Builder().Append(-64).Build()),
            new("opt_uint8", "uint8", UInt8Type.Default, new UInt8Array.Builder().Append(8).Build()),
            new("opt_uint16", "uint16", UInt16Type.Default, new UInt16Array.Builder().Append(16).Build()),
            new("opt_uint32", "uint32", UInt32Type.Default, new UInt32Array.Builder().Append(32).Build()),
            new("opt_uint64", "uint64", UInt64Type.Default, new UInt64Array.Builder().Append(64).Build()),
            new("opt_float32", "float32", FloatType.Default, new FloatArray.Builder().Append(1.5f).Build()),
            new("opt_float64", "float64", DoubleType.Default, new DoubleArray.Builder().Append(2.5).Build()),
            new("opt_string", "UTF-8 string", StringType.Default, new StringArray.Builder().Append("hello").Build()),
            new("opt_blob", "Binary blob", BinaryType.Default, new BinaryArray.Builder().Append([0x00, 0x01, 0x02]).Build()),
            new("opt_date", "Date", Date32Type.Default, new Date32Array.Builder().Append(new DateOnly(2026, 4, 24)).Build()),
            new("opt_time", "Time of day", new Time64Type(TimeUnit.Microsecond), new Time64Array.Builder(TimeUnit.Microsecond).Append(new TimeOnly(12, 34, 56)).Build()),
            new("opt_timestamp", "Naive timestamp", new TimestampType(TimeUnit.Microsecond, (string?)null), new TimestampArray.Builder(TimeUnit.Microsecond).Append(new DateTimeOffset(2026, 4, 24, 12, 34, 56, TimeSpan.Zero)).Build()),
            new("opt_timestamp_tz", "Timestamp with UTC tz", new TimestampType(TimeUnit.Microsecond, (string?)"UTC"), new TimestampArray.Builder(TimeUnit.Microsecond, "UTC").Append(new DateTimeOffset(2026, 4, 24, 12, 34, 56, TimeSpan.Zero)).Build()),
            new("opt_decimal", "Decimal(18,4)", decimalType, new Decimal128Array.Builder(decimalType).Append(123.4500m).Build()),
            new("opt_list", "List of int64", listType, listDefault),
            new("opt_struct", "Struct", structType, structDefault),
        ];
    }
}
