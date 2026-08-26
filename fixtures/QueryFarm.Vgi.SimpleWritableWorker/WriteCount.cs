using Apache.Arrow;
using Apache.Arrow.Types;

namespace QueryFarm.Vgi.SimpleWritableWorker;

/// <summary>The <c>(count BIGINT)</c> shape a writable-table delegate function emits when the
/// caller did NOT request RETURNING (<c>write_options.return_chunks == false</c>) — see
/// <c>ReadCountFromBatch</c> in <c>vgi_physical_write.cpp</c>.</summary>
public static class WriteCount
{
    public static Schema Schema { get; } = new([new Field("count", Int64Type.Default, nullable: false)], metadata: null);

    public static RecordBatch Batch(long count)
    {
        var builder = new Int64Array.Builder();
        builder.Append(count);
        return new RecordBatch(Schema, [builder.Build()], 1);
    }
}
