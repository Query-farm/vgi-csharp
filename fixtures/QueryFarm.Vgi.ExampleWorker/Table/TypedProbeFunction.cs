using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>typed_probe(n, ts:=, iv:=, blob:=, ub:=, f:=)</c> — binds less-common scalar const argument
/// types (TIMESTAMPTZ, INTERVAL, BLOB, UBIGINT), each with a default, and echoes them into
/// uint64/int64/blob/double output columns; <c>f</c> increases by 1.0 per row so a projection-only
/// query can still tell rows apart. Backs <c>table/typed_probe.test</c>. <c>iv</c> always arrives as
/// Arrow's <c>interval_monthdaynano</c> (DuckDB's own native INTERVAL wire shape — declaring a
/// different target Arrow type on the argument field does NOT make DuckDB cast an INTERVAL literal
/// into it, unlike TIMESTAMPTZ/BLOB/UBIGINT/DOUBLE); this fixture only ever uses pure-time
/// intervals (no months/days component) so <c>iv_ms</c> is just its nanoseconds field / 1e6.
/// </summary>
public sealed class TypedProbeFunction : ITableFunction
{
    private static readonly TimestampType TsArgType = new(TimeUnit.Microsecond, "UTC");

    private static readonly DateTime DefaultTsUtc = new(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
    private const long DefaultIvNanos = 1_500_000_000; // 1500 ms
    private static readonly byte[] DefaultBlob = "vgi"u8.ToArray();
    private const ulong DefaultUb = 9;
    private const double DefaultF = 2.5;

    public string Name => "typed_probe";

    public string Description => "Binds less-common scalar const argument types and echoes them per row";

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("n", Int64Type.Default),
            TableArgFields.Named("ts", TsArgType),
            TableArgFields.Named("iv", IntervalType.MonthDayNanosecond),
            TableArgFields.Named("blob", BinaryType.Default),
            TableArgFields.Named("ub", UInt64Type.Default),
            TableArgFields.Named("f", DoubleType.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("idx", UInt64Type.Default, nullable: true),
            new Field("ts_us", Int64Type.Default, nullable: true),
            new Field("iv_ms", Int64Type.Default, nullable: true),
            new Field("payload", BinaryType.Default, nullable: true),
            new Field("ub", UInt64Type.Default, nullable: true),
            new Field("f", DoubleType.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var n = initParams.Arguments.Int64(0);
        var tsUs = ReadTimestampMicros(initParams.Arguments.NamedArray("ts"));
        var ivMs = ReadDurationMillis(initParams.Arguments.NamedArray("iv"));
        var blob = ReadBlob(initParams.Arguments.NamedArray("blob"));
        var ub = ReadUInt64(initParams.Arguments.NamedArray("ub"));
        var f = ReadDouble(initParams.Arguments.NamedArray("f"));
        return new Producer(n, tsUs, ivMs, blob, ub, f, initParams.OutputSchema);
    }

    private static long ReadTimestampMicros(IArrowArray? array) =>
        array is TimestampArray ts && !ts.IsNull(0) ? ts.Values[0] : DefaultTsMicros();

    private static long DefaultTsMicros() =>
        (long)(DefaultTsUtc - DateTime.UnixEpoch).TotalMicroseconds;

    private static long ReadDurationMillis(IArrowArray? array) =>
        (array is MonthDayNanosecondIntervalArray iv && !iv.IsNull(0) ? iv.Values[0].Nanoseconds : DefaultIvNanos) / 1_000_000;

    private static byte[] ReadBlob(IArrowArray? array) =>
        array is BinaryArray b && !b.IsNull(0) ? b.GetBytes(0).ToArray() : DefaultBlob;

    private static ulong ReadUInt64(IArrowArray? array) =>
        array is UInt64Array u && !u.IsNull(0) ? u.GetValue(0)!.Value : DefaultUb;

    private static double ReadDouble(IArrowArray? array) =>
        array is DoubleArray d && !d.IsNull(0) ? d.GetValue(0)!.Value : DefaultF;

    private sealed class Producer(long n, long tsUs, long ivMs, byte[] blob, ulong ub, double f, Schema outputSchema)
        : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= n)
            {
                output.Finish();
                return;
            }

            var rows = (int)n; // typed_probe is only ever called with small n in this fixture's tests.
            var idxBuilder = new UInt64Array.Builder();
            var tsBuilder = new Int64Array.Builder();
            var ivBuilder = new Int64Array.Builder();
            var payloadBuilder = new BinaryArray.Builder();
            var ubBuilder = new UInt64Array.Builder();
            var fBuilder = new DoubleArray.Builder();

            for (var i = 0L; i < rows; i++)
            {
                idxBuilder.Append((ulong)i);
                tsBuilder.Append(tsUs);
                ivBuilder.Append(ivMs);
                payloadBuilder.Append(blob);
                ubBuilder.Append(ub);
                fBuilder.Append(f + i);
            }

            _next = n;
            output.Emit(new RecordBatch(
                outputSchema,
                [idxBuilder.Build(), tsBuilder.Build(), ivBuilder.Build(), payloadBuilder.Build(), ubBuilder.Build(), fBuilder.Build()],
                rows));
            output.Finish();
        }
    }
}
