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
            var schema = SchemaDerivation.InnerSchemaFor(clrType);
            Properties = new PropertyInfo[schema.FieldsList.Count];
            ClrTypes = new Type[Properties.Length];
            var fields = new List<Field>(Properties.Length);
            var positional = 0;
            for (var i = 0; i < Properties.Length; i++)
            {
                var field = schema.GetFieldByIndex(i);
                Properties[i] = clrType.GetProperty(ValueCodec.FindClrPropertyName(clrType, field))!;
                ClrTypes[i] = Properties[i].PropertyType;
                if (Properties[i].IsDefined(typeof(WireOptionalAttribute)))
                {
                    field = new Field(field.Name, field.DataType, nullable: true, field.Metadata);
                }
                else
                {
                    if (positional != i)
                    {
                        throw new InvalidOperationException(
                            $"'{clrType}.{Properties[i].Name}' follows a [WireOptional] property; appended optional columns must come last.");
                    }

                    positional++;
                }

                fields.Add(field);
            }

            PositionalCount = positional;
            Schema = positional == Properties.Length ? schema : new Schema(fields, schema.Metadata);
        }

        /// <summary>How many leading fields decode positionally; the rest are
        /// <see cref="WireOptionalAttribute"/> columns, decoded by name.</summary>
        public int PositionalCount { get; }

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

        var values = ValueCodec.ExtractRow(row, layout.PositionalCount == layout.ClrTypes.Length
            ? layout.ClrTypes
            : layout.ClrTypes[..layout.PositionalCount]);

        var instance = new T();
        for (var i = 0; i < layout.PositionalCount; i++)
        {
            layout.Properties[i].SetValue(instance, values[i]);
        }

        // Appended optional columns: by name, so a batch from an older peer (no such column)
        // or one carrying a null leaves the property at its default.
        for (var i = layout.PositionalCount; i < layout.Properties.Length; i++)
        {
            var name = layout.Schema.GetFieldByIndex(i).Name;
            var index = row.Schema.GetFieldIndex(name);
            if (index < 0)
            {
                continue;
            }

            var clrType = layout.ClrTypes[i];
            var readType = clrType.IsValueType && Nullable.GetUnderlyingType(clrType) is null
                ? typeof(Nullable<>).MakeGenericType(clrType)
                : clrType;
            // Not disposed: it shares row's column, which row's own disposal releases.
            var single = new RecordBatch(
                new Schema([row.Schema.GetFieldByIndex(index)], metadata: null), [row.Column(index)], row.Length);
            var value = ValueCodec.ExtractRow(single, [readType])[0];
            if (value is not null)
            {
                layout.Properties[i].SetValue(instance, value);
            }
        }

        return instance;
    }
}
