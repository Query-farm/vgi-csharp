using System.Collections.Concurrent;
using System.Reflection;
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
    /// <summary>A type's inner schema and the CLR property behind each of its fields, in field
    /// order. Resolving a field's property scans every property of the type
    /// (<see cref="ValueCodec.FindClrPropertyName"/>), so doing it per field per call made an
    /// encode quadratic in the type's width — a <see cref="Protocol.FunctionInfo"/> has 41 fields,
    /// and a function listing encodes one per function.</summary>
    private sealed class Layout
    {
        public Layout(Type clrType)
        {
            Schema = SchemaDerivation.InnerSchemaFor(clrType);
            Properties = new PropertyInfo[Schema.FieldsList.Count];
            ClrTypes = new Type[Properties.Length];
            for (var i = 0; i < Properties.Length; i++)
            {
                Properties[i] = clrType.GetProperty(ValueCodec.FindClrPropertyName(clrType, Schema.GetFieldByIndex(i)))!;
                ClrTypes[i] = Properties[i].PropertyType;
            }
        }

        public Schema Schema { get; }

        public PropertyInfo[] Properties { get; }

        public Type[] ClrTypes { get; }
    }

    private static readonly ConcurrentDictionary<Type, Layout> s_layouts = new();

    private static Layout LayoutFor(Type clrType) => s_layouts.GetOrAdd(clrType, static type => new Layout(type));

    public static byte[] Encode<T>(T value) where T : class, new()
    {
        var layout = LayoutFor(typeof(T));
        var rowValues = new object?[layout.Properties.Length];
        for (var i = 0; i < rowValues.Length; i++)
        {
            rowValues[i] = layout.Properties[i].GetValue(value);
        }

        // Disposed (so reachable) until the write is complete — see RecordBatchIpc.Write — and
        // its native buffers go straight back to the pool rather than waiting for a finalizer.
        using var row = ValueCodec.BuildRow(layout.Schema, rowValues);
        return RecordBatchIpc.Write(row);
    }

    public static T Decode<T>(byte[] bytes) where T : class, new()
    {
        var layout = LayoutFor(typeof(T));
        using var stream = new MemoryStream(bytes);
        using var reader = new ArrowStreamReader(stream);
        // ExtractRow copies every value out, so the batch can be released as soon as it returns;
        // until then it must stay reachable (see RecordBatchIpc.Write).
        using var row = reader.ReadNextRecordBatch()
            ?? throw new InvalidOperationException($"Embedded record for '{typeof(T)}' had no data batch.");

        var values = ValueCodec.ExtractRow(row, layout.ClrTypes);

        var instance = new T();
        for (var i = 0; i < layout.Properties.Length; i++)
        {
            layout.Properties[i].SetValue(instance, values[i]);
        }

        return instance;
    }
}
