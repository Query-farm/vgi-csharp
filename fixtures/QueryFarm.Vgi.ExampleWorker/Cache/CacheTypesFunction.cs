using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary><c>ex.main.cache_types(n)</c> — a wide, nested, interleaved-NULL generator: STRUCT
/// (<c>attrs</c>), LIST&lt;INT64&gt; (<c>tags</c>), DECIMAL (<c>amt</c>), TIMESTAMP (<c>ts</c>), and
/// VARCHAR (<c>label</c>, exactly 20% NULL — every 5th row) columns, each with its OWN (different
/// modulus) null pattern so a validity-bitmap bug in one column's spill/serve round-trip can't hide
/// behind another's. Genuinely multi-batch (1000 rows/tick). Backs <c>spill_types.test</c>'s proof
/// that nested/wide/NULL columns survive a disk spill + streaming serve BYTE-IDENTICAL, not merely
/// matching a COUNT/SUM aggregate the way every flat-int64 cache fixture does.</summary>
public sealed class CacheTypesFunction : ITableFunction
{
    private const long BatchSize = 1000;

    private static readonly StructType AttrsType = new(
        [
            new Field("a", Int64Type.Default, nullable: true),
            new Field("b", StringType.Default, nullable: true),
        ]);

    private static readonly ListType TagsType = new(new Field("item", Int64Type.Default, nullable: true));

    private static readonly Decimal128Type AmtType = new(precision: 18, scale: 2);

    private static readonly TimestampType TsType = TimestampType.Default;

    private static readonly DateTimeOffset Epoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public string Name => "cache_types";

    public string SchemaName => "main";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("n", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("tags", TagsType, nullable: true),
            new Field("attrs", AttrsType, nullable: true),
            new Field("amt", AmtType, nullable: true),
            new Field("ts", TsType, nullable: true),
            new Field("label", StringType.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var n = initParams.Arguments.Int64(0);
        return new Producer(n, initParams.OutputSchema);
    }

    private sealed class Producer(long n, Schema outputSchema) : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= n)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, n - _next);
            var start = _next;
            _next += rows;

            var idBuilder = new Int64Array.Builder();
            var tagsBuilder = new ListArray.Builder(TagsType.ValueField);
            var tagsValues = (Int64Array.Builder)tagsBuilder.ValueBuilder;
            var attrsABuilder = new Int64Array.Builder();
            var attrsBBuilder = new StringArray.Builder();
            var attrsValidity = new ArrowBuffer.BitmapBuilder();
            var amtBuilder = new Decimal128Array.Builder(AmtType);
            var tsBuilder = new TimestampArray.Builder(TsType.Unit, TsType.Timezone);
            var labelBuilder = new StringArray.Builder();
            var attrsNullCount = 0;

            for (var i = 0L; i < rows; i++)
            {
                var id = start + i;
                idBuilder.Append(id);

                if (id % 7 == 0)
                {
                    tagsBuilder.AppendNull();
                }
                else
                {
                    tagsBuilder.Append();
                    tagsValues.Append(id);
                    tagsValues.Append(id + 1);
                }

                if (id % 11 == 0)
                {
                    attrsABuilder.AppendNull();
                    attrsBBuilder.AppendNull();
                    attrsValidity.Append(false);
                    attrsNullCount++;
                }
                else
                {
                    attrsABuilder.Append(id);
                    attrsBBuilder.Append($"s{id}");
                    attrsValidity.Append(true);
                }

                if (id % 13 == 0)
                {
                    amtBuilder.AppendNull();
                }
                else
                {
                    amtBuilder.Append((decimal)id / 100m);
                }

                if (id % 17 == 0)
                {
                    tsBuilder.AppendNull();
                }
                else
                {
                    tsBuilder.Append(Epoch.AddSeconds(id));
                }

                // Exactly 20% NULL over any multiple-of-5 total row count (every 5th row).
                if (id % 5 == 0)
                {
                    labelBuilder.AppendNull();
                }
                else
                {
                    labelBuilder.Append($"label_{id}");
                }
            }

            var attrs = new StructArray(
                AttrsType, rows, [attrsABuilder.Build(), attrsBBuilder.Build()], attrsValidity.Build(), attrsNullCount);

            var metadata = start == 0 ? CacheMetadata.Ttl(300) : null;
            output.Emit(
                new RecordBatch(
                    outputSchema,
                    [idBuilder.Build(), tagsBuilder.Build(), attrs, amtBuilder.Build(), tsBuilder.Build(), labelBuilder.Build()],
                    rows),
                metadata);

            if (_next >= n)
            {
                output.Finish();
            }
        }
    }
}
