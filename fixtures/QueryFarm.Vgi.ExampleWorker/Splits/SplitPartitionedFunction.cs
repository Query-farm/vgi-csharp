using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Splits;

/// <summary>
/// <c>split_partitioned(rows_per_country)</c> — one split per country (four, always, regardless
/// of <c>rows_per_country</c> — including 0, which still plans four splits that each emit
/// nothing), proving partition-value association survives greedy claiming, re-init on a reused
/// connection, and readers moving between partitions (<c>partition_values.test</c>).
///
/// Each country's <c>sales</c> values are <c>offset + 1 .. offset + rows_per_country</c> with a
/// distinct <c>offset</c> per country (<c>US</c>=0, <c>DE</c>=100, <c>JP</c>=200, <c>BR</c>=300) —
/// deliberately NOT identical across countries, so a mislabeled or swapped partition moves the
/// per-country sums the test asserts, rather than being invisible in a total.
/// </summary>
public sealed class SplitPartitionedFunction : ITableFunction
{
    private static readonly string[] Countries = ["US", "DE", "JP", "BR"];

    public string Name => "split_partitioned";

    public string Description => "One split per partition value, proving the association survives greedy claiming";

    public bool SupportsSplits => true;

    public VgiPartitionKind PartitionKind => VgiPartitionKind.SingleValuePartitions;

    public int? MaxWorkers => 8;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Named("rows_per_country", Int64Type.Default),
            TableArgFields.Named("key_mode", StringType.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("country", StringType.Default, nullable: true,
                new Dictionary<string, string>
                {
                    [VgiWireMetadata.PartitionColumnKey] = VgiWireMetadata.PartitionColumnTrueValue,
                }),
            new Field("sales", Int64Type.Default, nullable: true),
        ],
        metadata: null);

    public PlanResult Plan(TableBindParams bindParams, PlanRequest request)
    {
        var rowsPerCountry = bindParams.Arguments.Int64Named("rows_per_country", 0);
        var keyMode = bindParams.Arguments.StringNamed("key_mode", "valid");
        var scanSplits = Enumerable.Range(0, Countries.Length)
            .Select(i => new ScanSplit
            {
                Payload = Encode(i, rowsPerCountry),
                KeyValue = KeyValue(Countries[i], keyMode),
            })
            .ToList();
        var path = EmbeddedIpc.Encode(new FieldPath { Components = ["country"] });
        var transform = EmbeddedIpc.Encode(new TransformSpec
        {
            Id = "vgi.identity/1",
            Inputs = [path],
        });
        return new PlanResult
        {
            Splits = scanSplits,
            EstimatedTotalSplits = scanSplits.Count,
            KeyGrouping = new KeyGrouping { Keys = [transform] },
        };
    }

    private static byte[]? KeyValue(string country, string mode)
    {
        if (mode == "missing") return null;
        using var batch = mode switch
        {
            "valid" => new RecordBatch(
                new Schema([new Field("country", StringType.Default, nullable: true)], null),
                [new StringArray.Builder().Append(country).Build()], 1),
            "two_rows" => new RecordBatch(
                new Schema([new Field("country", StringType.Default, nullable: true)], null),
                [new StringArray.Builder().Append(country).Append(country).Build()], 2),
            "wrong_type" => new RecordBatch(
                new Schema([new Field("country", Int64Type.Default, nullable: true)], null),
                [new Int64Array.Builder().Append(1).Build()], 1),
            _ => throw new ArgumentException($"unknown split_partitioned key_mode: {mode}"),
        };
        return RecordBatchIpc.Write(batch);
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var payloads = SplitOnlyGuard.RequireSingle(initParams, Name);
        var (countryIndex, rowsPerCountry) = Decode(payloads[0]);
        return new Producer(Countries[countryIndex], countryIndex * 100, rowsPerCountry, initParams.OutputSchema);
    }

    private static byte[] Encode(long countryIndex, long rowsPerCountry)
    {
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(0, 8), countryIndex);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(8, 8), rowsPerCountry);
        return bytes;
    }

    private static (long CountryIndex, long RowsPerCountry) Decode(byte[] payload) => (
        BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(0, 8)),
        BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(8, 8)));

    private sealed class Producer(string country, long offset, long rowsPerCountry, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (_emitted || rowsPerCountry <= 0)
            {
                output.Finish();
                return;
            }

            _emitted = true;
            var rows = (int)rowsPerCountry;
            var countryBuilder = new StringArray.Builder();
            var salesBuilder = new Int64Array.Builder();
            for (var i = 1; i <= rows; i++)
            {
                countryBuilder.Append(country);
                salesBuilder.Append(offset + i);
            }

            var batch = new RecordBatch(outputSchema, [countryBuilder.Build(), salesBuilder.Build()], rows);
            output.Emit(batch, PartitionValuesCodec.PartitionValues(outputSchema, batch));
            output.Finish();
        }
    }
}
