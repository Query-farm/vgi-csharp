using Apache.Arrow;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Accumulate;

/// <summary>
/// <c>accumulate(name VARCHAR, data TABLE, ttl := NULL, max_row_size := 0, result := 'all')</c> —
/// appends <c>data</c>'s rows to a named, ATTACH-session-scoped persistent collection
/// (<see cref="AccumulateStore"/>) and returns rows per <c>result</c>: <c>'all'</c> (default) the
/// whole collection, <c>'new'</c> only the rows this call added, <c>'none'</c> nothing. Every row
/// gets a <c>_timestamp</c> column (one value per call, TIMESTAMP typed). Mirrors vgi-python/
/// vgi-java's example-fixture <c>accumulate</c> (test/sql/integration/accumulate/*.test) — a
/// table-buffering function purely because that's the ONE function kind whose Sink phase sees
/// every input row before its Source phase must decide what to emit back.
/// </summary>
public sealed class AccumulateFunction : ITableBufferingFunction
{
    private const string TimestampColumn = "_timestamp";
    private const string CallNamespace = "accumulate";
    private const string CallKey = "call_timestamp";

    public string Name => "accumulate";

    public string Description => "Appends rows to a named persistent collection and returns it";

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("name", StringType.Default, nullable: false),
            TableArgFields.Table("data"),
            TableArgFields.Named("ttl", new IntervalType(IntervalUnit.MonthDayNanosecond)),
            TableArgFields.Named("max_row_size", Int64Type.Default),
            TableArgFields.Named("result", StringType.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public void Bind(TableInOutBindParams bindParams)
    {
        var name = RequireName(bindParams.Arguments);

        if (bindParams.InputSchema.GetFieldIndex(TimestampColumn) >= 0)
        {
            throw new InvalidOperationException(
                $"accumulate('{name}', ...): input may not contain a reserved '{TimestampColumn}' column; " +
                "accumulate adds this column to its output.");
        }

        var store = new AccumulateStore(bindParams.AttachOpaqueData, name);
        var pinned = store.ReadPinnedSchema();
        if (pinned is not null)
        {
            var withTimestamp = WithTimestampColumn(bindParams.InputSchema);
            if (!SchemasEqual(pinned, withTimestamp))
            {
                throw new InvalidOperationException(
                    $"accumulate('{name}', ...): input schema for accumulate('{name}', ...) does not match " +
                    "the schema already accumulated under that name.\n" +
                    $"  accumulated: {DescribeSchema(pinned)}\n  received: {DescribeSchema(withTimestamp)}");
            }
        }
    }

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams)
    {
        var name = RequireName(bindParams.Arguments);
        return new AccumulateStore(bindParams.AttachOpaqueData, name).ReadPinnedSchema()
            ?? WithTimestampColumn(bindParams.InputSchema);
    }

    public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams)
    {
        var name = RequireName(processParams.Arguments);
        var store = new AccumulateStore(processParams.AttachOpaqueData ?? [], name);
        var callTimestampUtc = GetOrCreateCallTimestamp(processParams.Storage);
        var stamped = WithTimestampValues(batch, callTimestampUtc);

        store.EnsurePinnedSchema(stamped.Schema);
        store.AppendSegment(stamped, callTimestampUtc);
        return processParams.ExecutionId;
    }

    public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams)
    {
        var name = RequireName(combineParams.Arguments);
        var store = new AccumulateStore(combineParams.AttachOpaqueData ?? [], name);
        var callTimestampUtc = GetOrCreateCallTimestamp(combineParams.Storage);

        if (ReadTtl(combineParams.Arguments) is { } ttl)
        {
            store.EvictOlderThan(callTimestampUtc - ttl);
        }

        var maxRowSize = combineParams.Arguments.Int64Named("max_row_size", 0);
        if (maxRowSize > 0)
        {
            store.TrimToNewest(maxRowSize);
        }

        return [combineParams.ExecutionId];
    }

    public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams)
    {
        var name = RequireName(finalizeParams.Arguments);
        var result = finalizeParams.Arguments.StringNamed("result", "all");
        var store = new AccumulateStore(finalizeParams.AttachOpaqueData ?? [], name);

        IReadOnlyList<RecordBatch> rows = result switch
        {
            "none" => [],
            "new" => store.ReadSegmentsAt(GetOrCreateCallTimestamp(finalizeParams.Storage)),
            _ => store.ReadAllSegments(),
        };

        return new BatchListProducer(rows);
    }

    private static string RequireName(TableArguments arguments)
    {
        var name = arguments.StringPositional(0);
        if (string.IsNullOrEmpty(name))
        {
            throw new InvalidOperationException("accumulate: 'name' must be a non-empty string.");
        }

        return name;
    }

    /// <summary>Recovers (or, on the FIRST Process/Combine/finalize call for this execution, mints
    /// and persists) the ONE timestamp every row this <c>accumulate()</c> call ingests shares —
    /// stored via the generic append-log primitive (single entry) since
    /// <see cref="Buffering.IFunctionStorage"/> exposes no single-value read/write of its own.</summary>
    private static DateTime GetOrCreateCallTimestamp(IFunctionStorage storage)
    {
        var existing = storage.ScanLog(CallNamespace, CallKey).FirstOrDefault();
        if (existing is { Length: 8 } bytes)
        {
            return DateTime.FromBinary(BitConverter.ToInt64(bytes));
        }

        var now = DateTime.UtcNow;
        storage.Append(CallNamespace, CallKey, BitConverter.GetBytes(now.ToBinary()));
        return now;
    }

    private static MonthDayNanosecondInterval? ReadTtl(TableArguments arguments) =>
        arguments.NamedArray("ttl") is MonthDayNanosecondIntervalArray array && !array.IsNull(0)
            ? array.GetValue(0)
            : null;

    private static Schema WithTimestampColumn(Schema schema)
    {
        var fields = schema.FieldsList.ToList();
        fields.Add(new Field(TimestampColumn, new TimestampType(TimeUnit.Microsecond, (string?)null), nullable: false));
        return new Schema(fields, metadata: null);
    }

    private static RecordBatch WithTimestampValues(RecordBatch batch, DateTime timestampUtc)
    {
        var timestampOffset = new DateTimeOffset(timestampUtc);
        var tsBuilder = new TimestampArray.Builder(TimeUnit.Microsecond);
        for (var i = 0; i < batch.Length; i++)
        {
            tsBuilder.Append(timestampOffset);
        }

        var arrays = new List<IArrowArray>();
        for (var i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            arrays.Add(batch.Column(i));
        }

        arrays.Add(tsBuilder.Build());

        return new RecordBatch(WithTimestampColumn(batch.Schema), arrays, batch.Length);
    }

    private static bool SchemasEqual(Schema a, Schema b)
    {
        if (a.FieldsList.Count != b.FieldsList.Count)
        {
            return false;
        }

        for (var i = 0; i < a.FieldsList.Count; i++)
        {
            var fa = a.FieldsList[i];
            var fb = b.FieldsList[i];
            // IArrowType has no value-equality override on most concrete types (TimestampType
            // included) — two separately-constructed instances of the "same" type are never
            // Equals() to each other by reference. ToString() renders each type's full structure
            // (unit/timezone/precision/etc.) so a text comparison is the reliable structural check.
            if (!string.Equals(fa.Name, fb.Name, StringComparison.Ordinal) ||
                !string.Equals(fa.DataType.ToString(), fb.DataType.ToString(), StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static string DescribeSchema(Schema schema) =>
        string.Join(", ", schema.FieldsList.Select(f => $"{f.Name}: {f.DataType}"));
}
