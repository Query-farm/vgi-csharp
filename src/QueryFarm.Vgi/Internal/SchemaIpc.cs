using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Writes a BARE Arrow schema (no row, no EOS) as IPC bytes — the shape
/// <c>BindResponse.OutputSchema</c>/<c>FunctionInfo.Arguments</c>/<c>FunctionInfo.OutputSchema</c>
/// need: the C++ extension deserializes them with <c>arrow::ipc::ReadSchema</c> (a schema-only
/// read, distinct from a full record-batch-stream read), matching what C++'s own
/// <c>arrow::ipc::SerializeSchema</c> produces on the write side of the exact same fields
/// (<c>SerializeSchemaToIpcBytes</c> in <c>vgi_rpc_types.cpp</c>). Calling
/// <see cref="ArrowStreamWriter.WriteStart"/> without ever calling <c>WriteRecordBatch</c>/
/// <c>WriteEnd</c> writes exactly the schema message and nothing else — <c>Dispose</c> doesn't
/// append anything of its own (verified against the vendored writer's source).
/// </summary>
public static class SchemaIpc
{
    public static byte[] WriteSchemaOnly(Schema schema)
    {
        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true))
        {
            writer.WriteStart();
        }

        return stream.ToArray();
    }

    /// <summary>Reads back a schema written by <see cref="WriteSchemaOnly"/> (or a full IPC stream
    /// — only the schema message is consulted either way).</summary>
    public static Schema ReadSchemaOnly(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new ArrowStreamReader(stream);
        return reader.Schema;
    }
}
