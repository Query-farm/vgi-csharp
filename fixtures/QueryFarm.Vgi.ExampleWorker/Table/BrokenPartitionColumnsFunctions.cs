using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// Deliberately-broken v2 PartitionColumns fixtures — backs <c>partition_columns_contract.test</c>.
/// Two violations are caught worker-side by <see cref="PartitionValuesCodec.PartitionValues"/>
/// (before the wire); two reach the C++ extension's <c>InstallBatch</c> defense-in-depth check.
/// </summary>
public static class BrokenPartitionColumnsFunctions
{
    internal static Dictionary<string, string> PartitionColumnMetadata() =>
        new() { [VgiWireMetadata.PartitionColumnKey] = VgiWireMetadata.PartitionColumnTrueValue };
}

/// <summary><c>ex.broken_missing_partition_values(count)</c> — declares partition_kind + a
/// partition-annotated field but emits a data batch with NO <c>vgi_partition_values#b64</c>
/// metadata. The C++ extension's contract check raises.</summary>
public sealed class BrokenMissingPartitionValuesFunction : ITableFunction
{
    public string Name => "broken_missing_partition_values";

    public string SchemaName => "main";

    public IReadOnlyList<string> Categories => ["testing", "broken"];

    public VgiPartitionKind PartitionKind => VgiPartitionKind.SingleValuePartitions;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("country", StringType.Default, nullable: true, BrokenPartitionColumnsFunctions.PartitionColumnMetadata()),
            new Field("sales", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(initParams.Arguments.Int64(0), initParams.OutputSchema);

    private sealed class Producer(long count, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted)
            {
                output.Finish();
                return;
            }

            var countryBuilder = new StringArray.Builder();
            var salesBuilder = new Int64Array.Builder();
            for (var i = 0L; i < count; i++)
            {
                countryBuilder.Append("US");
                salesBuilder.Append(i);
            }

            _emitted = true;

            // No vgi_partition_values#b64 metadata attached — the C++ side must raise.
            output.Emit(new RecordBatch(outputSchema, [countryBuilder.Build(), salesBuilder.Build()], (int)count));
        }
    }
}

/// <summary><c>ex.broken_partition_min_neq_max(count)</c> — declares SINGLE_VALUE_PARTITIONS but
/// supplies an explicit override with <c>min != max</c>. The framework's helper doesn't compare
/// min vs max for SINGLE_VALUE_PARTITIONS itself (see <see cref="PartitionValuesCodec.Range"/>'s doc
/// comment); the C++ extension's <c>InstallBatch</c> defense-in-depth check raises instead.</summary>
public sealed class BrokenPartitionMinNeqMaxFunction : ITableFunction
{
    public string Name => "broken_partition_min_neq_max";

    public string SchemaName => "main";

    public IReadOnlyList<string> Categories => ["testing", "broken"];

    public VgiPartitionKind PartitionKind => VgiPartitionKind.SingleValuePartitions;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("country", StringType.Default, nullable: true, BrokenPartitionColumnsFunctions.PartitionColumnMetadata()),
            new Field("sales", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(initParams.Arguments.Int64(0), initParams.OutputSchema);

    private sealed class Producer(long count, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted)
            {
                output.Finish();
                return;
            }

            var countryBuilder = new StringArray.Builder();
            var salesBuilder = new Int64Array.Builder();
            for (var i = 0L; i < count; i++)
            {
                countryBuilder.Append("US");
                salesBuilder.Append(i);
            }

            _emitted = true;

            var batch = new RecordBatch(outputSchema, [countryBuilder.Build(), salesBuilder.Build()], (int)count);

            // min != max defeats a SINGLE_VALUE_PARTITIONS batch's own contract; the C++
            // defense-in-depth check must raise (not this worker-side helper).
            var overrides = new Dictionary<string, PartitionValuesCodec.Range> { ["country"] = new("US", "BR") };
            output.Emit(batch, PartitionValuesCodec.PartitionValues(outputSchema, batch, overrides));
        }
    }
}

/// <summary><c>ex.broken_partition_values_no_annotation(count)</c> — NO field carries
/// <c>vgi.partition_column</c> metadata (partition_kind defaults to NOT_PARTITIONED), but the worker
/// still supplies an explicit <c>partition_values</c> override. Rejected worker-side before the
/// wire is ever reached.</summary>
public sealed class BrokenPartitionValuesNoAnnotationFunction : ITableFunction
{
    public string Name => "broken_partition_values_no_annotation";

    public string SchemaName => "main";

    public IReadOnlyList<string> Categories => ["testing", "broken"];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    // No partition annotation — plain schema.
    public Schema OutputSchema { get; } = new(
        [
            new Field("country", StringType.Default, nullable: true),
            new Field("sales", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(initParams.Arguments.Int64(0), initParams.OutputSchema);

    private sealed class Producer(long count, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted)
            {
                output.Finish();
                return;
            }

            var countryBuilder = new StringArray.Builder();
            var salesBuilder = new Int64Array.Builder();
            for (var i = 0L; i < count; i++)
            {
                countryBuilder.Append("US");
                salesBuilder.Append(i);
            }

            _emitted = true;

            var batch = new RecordBatch(outputSchema, [countryBuilder.Build(), salesBuilder.Build()], (int)count);

            // OUTPUT has no partition-annotated field — helper raises before the wire.
            var overrides = new Dictionary<string, PartitionValuesCodec.Range> { ["country"] = new("US", "US") };
            PartitionValuesCodec.PartitionValues(outputSchema, batch, overrides);
            output.Emit(batch); // unreached
        }
    }
}

/// <summary><c>ex.broken_partition_column_absent_from_batch(count)</c> — declares partition_kind on
/// <c>category</c> but the batch actually emitted omits that column entirely and no explicit
/// override is supplied. The framework's auto-extract fails worker-side before the wire.</summary>
public sealed class BrokenPartitionColumnAbsentFromBatchFunction : ITableFunction
{
    private static readonly Schema BatchOnlySchema = new([new Field("revenue", Int64Type.Default, nullable: false)], metadata: null);

    public string Name => "broken_partition_column_absent_from_batch";

    public string SchemaName => "main";

    public IReadOnlyList<string> Categories => ["testing", "broken"];

    public VgiPartitionKind PartitionKind => VgiPartitionKind.SingleValuePartitions;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("category", StringType.Default, nullable: true, BrokenPartitionColumnsFunctions.PartitionColumnMetadata()),
            new Field("revenue", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(initParams.Arguments.Int64(0), initParams.OutputSchema);

    private sealed class Producer(long count, Schema declaredSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted)
            {
                output.Finish();
                return;
            }

            var revenueBuilder = new Int64Array.Builder();
            for (var i = 0L; i < count; i++)
            {
                revenueBuilder.Append(i);
            }

            _emitted = true;

            // 'category' is partition-annotated in the DECLARED schema but absent from the
            // actually-emitted batch, and no override is supplied — helper raises.
            var batch = new RecordBatch(BatchOnlySchema, [revenueBuilder.Build()], (int)count);
            PartitionValuesCodec.PartitionValues(declaredSchema, batch, explicitValues: null);
            output.Emit(batch); // unreached
        }
    }
}
