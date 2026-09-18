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
        Write(stream, batch);
        return stream.ToArray();
    }

    /// <summary>
    /// Writes <paramref name="batch"/> onto <paramref name="destination"/> as one self-contained
    /// IPC stream: schema message, the batch (with <paramref name="customMetadata"/> on its message,
    /// if any), EOS. <paramref name="destination"/> is left open.
    ///
    /// <para>Every single-batch IPC write in this package goes through here, because of a
    /// use-after-free in Apache.Arrow .NET (23.0.0, and the QueryFarm.Arrow fork built from it)
    /// that the caller has to guard against. A builder-made <see cref="ArrowBuffer"/> is owned by a
    /// <c>SharedMemoryHandle</c> whose finalizer frees the native memory, while the
    /// <c>ReadOnlyMemory&lt;byte&gt;</c> <see cref="ArrowStreamWriter"/> records for each buffer
    /// references only the inner memory manager — so it does not keep the handle alive.
    /// <c>WriteRecordBatch</c> stops referencing the batch before it copies the buffer bodies, so
    /// for a batch nobody else references (a row built only to be written, the usual case here) a
    /// collection in that window finalizes the handles mid-write. The copy then either dereferences
    /// a zeroed pointer (a <see cref="NullReferenceException"/> in
    /// <c>ArrowStreamWriter.WriteBufferData</c> — seen intermittently from
    /// <c>catalog_schema_contents_functions</c> under load) or, when the finalizer lands just after
    /// the pointer was read, copies memory the native pool has already handed to another buffer.
    /// Keeping the batch reachable until the stream is complete closes the window.</para>
    /// </summary>
    public static void Write(Stream destination, RecordBatch batch, IReadOnlyDictionary<string, string>? customMetadata = null)
    {
        using (var writer = new ArrowStreamWriter(destination, batch.Schema, leaveOpen: true))
        {
            writer.WriteStart();
            if (customMetadata is null)
            {
                writer.WriteRecordBatch(batch);
            }
            else
            {
                writer.WriteRecordBatch(batch, customMetadata);
            }

            writer.WriteEnd();
        }

        // Not redundant: see the summary. Without it the JIT may treat `batch` as dead as soon as
        // it has been handed to WriteRecordBatch.
        GC.KeepAlive(batch);
    }

    public static RecordBatch Read(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new ArrowStreamReader(stream);
        return reader.ReadNextRecordBatch()
            ?? throw new InvalidOperationException("Embedded RecordBatch had no data batch.");
    }
}
