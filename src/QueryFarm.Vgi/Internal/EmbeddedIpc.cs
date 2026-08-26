using Apache.Arrow;
using Apache.Arrow.Ipc;
using QueryFarm.VgiRpc.Reflection;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Encodes/decodes a plain C# dataclass-equivalent as a self-contained Arrow IPC stream (schema
/// message + one row + EOS) — exactly what <c>QueryFarm.VgiRpc.Reflection.ValueCodec</c>'s
/// PRIVATE <c>BuildEmbeddedRecordArray</c>/<c>ExtractEmbeddedRecord</c> helpers do internally for
/// a top-level RPC parameter/result, reimplemented here (from the same public building blocks —
/// <see cref="SchemaDerivation.InnerSchemaFor"/>, <see cref="ValueCodec.BuildRow"/>,
/// <see cref="ValueCodec.ExtractRow"/>, <see cref="ValueCodec.FindClrPropertyName"/> — all public)
/// for two cases <c>ValueCodec</c> itself doesn't cover:
/// <list type="bullet">
/// <item>a "binary containing an embedded IPC stream" value that sits NESTED inside another
/// already-embedded IPC stream (<see cref="Protocol.InitRequest.BindCall"/> — see its doc
/// comment) rather than being a service method's own top-level parameter/result;</item>
/// <item>an <see cref="Protocol.ItemsResponse.Items"/> element — each one independently
/// serialized as its own embedded IPC stream, not part of any method's own top-level schema.</item>
/// </list>
/// </summary>
public static class EmbeddedIpc
{
    public static byte[] Encode<T>(T value) where T : class, new()
    {
        var clrType = typeof(T);
        var innerSchema = SchemaDerivation.InnerSchemaFor(clrType);
        var rowValues = new object?[innerSchema.FieldsList.Count];
        for (var i = 0; i < rowValues.Length; i++)
        {
            var field = innerSchema.GetFieldByIndex(i);
            var property = clrType.GetProperty(ValueCodec.FindClrPropertyName(clrType, field))!;
            rowValues[i] = property.GetValue(value);
        }

        var row = ValueCodec.BuildRow(innerSchema, rowValues);

        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, innerSchema, leaveOpen: true))
        {
            writer.WriteStart();
            writer.WriteRecordBatch(row);
            writer.WriteEnd();
        }

        return stream.ToArray();
    }

    public static T Decode<T>(byte[] bytes) where T : class, new()
    {
        var clrType = typeof(T);
        var innerSchema = SchemaDerivation.InnerSchemaFor(clrType);
        using var stream = new MemoryStream(bytes);
        using var reader = new ArrowStreamReader(stream);
        var row = reader.ReadNextRecordBatch()
            ?? throw new InvalidOperationException($"Embedded record for '{clrType}' had no data batch.");

        var properties = new System.Reflection.PropertyInfo[innerSchema.FieldsList.Count];
        var clrTypes = new Type[properties.Length];
        for (var i = 0; i < properties.Length; i++)
        {
            var field = innerSchema.GetFieldByIndex(i);
            properties[i] = clrType.GetProperty(ValueCodec.FindClrPropertyName(clrType, field))!;
            clrTypes[i] = properties[i].PropertyType;
        }

        var values = ValueCodec.ExtractRow(row, clrTypes);

        var instance = new T();
        for (var i = 0; i < properties.Length; i++)
        {
            properties[i].SetValue(instance, values[i]);
        }

        return instance;
    }
}
