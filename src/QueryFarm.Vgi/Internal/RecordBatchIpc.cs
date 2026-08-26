using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Serializes/deserializes a whole <see cref="RecordBatch"/> (its own schema + data) as a
/// self-contained Arrow IPC stream — the shape a table-buffering function's
/// <see cref="Buffering.IFunctionStorage"/> entries commonly use to persist an input batch verbatim
/// between the Sink phase (<see cref="Buffering.ITableBufferingFunction.Process"/>) and the Source
/// phase (its FINALIZE producer). Same write/read pattern as <see cref="EmbeddedIpc"/>, just without
/// the property-reflection step (a <see cref="RecordBatch"/> already IS the row-shaped data).
/// </summary>
public static class RecordBatchIpc
{
    public static byte[] Write(RecordBatch batch)
    {
        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, batch.Schema, leaveOpen: true))
        {
            writer.WriteStart();
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }

        return stream.ToArray();
    }

    public static RecordBatch Read(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new ArrowStreamReader(stream);
        return reader.ReadNextRecordBatch()
            ?? throw new InvalidOperationException("Embedded RecordBatch had no data batch.");
    }
}
