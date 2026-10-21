using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.VgiRpc.Reflection;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

/// <summary>Collection codecs for wire 2.2 catalog metadata. Unary codecs remain unchanged.</summary>
public static class MetadataCollections
{
    public static byte[]? Encode<T>(IReadOnlyList<T> values, int batchSize = 1024) where T : class, new()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        if (values.Count == 0) return null;
        var schema = SchemaDerivation.InnerSchemaFor(typeof(T));
        var properties = schema.FieldsList.Select(f => typeof(T).GetProperty(ValueCodec.FindClrPropertyName(typeof(T), f))!).ToArray();
        var rows = new List<RecordBatch>(values.Count);
        try
        {
            foreach (var value in values) rows.Add(ValueCodec.BuildRow(schema, properties.Select(p => p.GetValue(value)).ToArray()));
            var columns = Enumerable.Range(0, schema.FieldsList.Count).Select(i => Concatenate(rows.Select(r => r.Column(i)).ToArray())).ToArray();
            using var all = new RecordBatch(schema, columns, values.Count);
            using var output = new MemoryStream();
            using (var writer = new ArrowStreamWriter(output, schema, leaveOpen: true))
            {
                writer.WriteStart();
                for (var offset = 0; offset < values.Count; offset += batchSize)
                {
                    // Slices share all's buffers; all owns and releases them.
                    var size = Math.Min(batchSize, values.Count - offset);
                    var batch = new RecordBatch(schema, columns.Select(c => ArrowArrayFactory.BuildArray(c.Data.Slice(offset, size))).ToArray(), size);
                    writer.WriteRecordBatch(batch);
                    GC.KeepAlive(batch);
                }
                writer.WriteEnd();
            }
            GC.KeepAlive(all);
            return output.ToArray();
        }
        finally { foreach (var row in rows) row.Dispose(); }
    }

    public static List<T> Decode<T>(byte[]? bytes) where T : class, new()
    {
        if (bytes is null) return [];
        var expected = SchemaDerivation.InnerSchemaFor(typeof(T));
        var properties = expected.FieldsList.Select(f => typeof(T).GetProperty(ValueCodec.FindClrPropertyName(typeof(T), f))!).ToArray();
        var types = properties.Select(p => p.PropertyType).ToArray();
        using var input = new MemoryStream(bytes);
        using var reader = new ArrowStreamReader(input);
        if (!SameFields(reader.Schema.FieldsList, expected.FieldsList)) throw new InvalidOperationException($"Catalog collection schema does not match {typeof(T).Name}.");
        var values = new List<T>();
        var batches = new List<RecordBatch>();
        var slices = new List<RecordBatch>();
        RecordBatch? batch;
        try
        {
            while ((batch = reader.ReadNextRecordBatch()) is not null)
            {
                {
                    batches.Add(batch);
                    if (reader.LastBatchCustomMetadata?.Keys.Any(key => key.StartsWith("vgi_rpc.location", StringComparison.Ordinal) || key.StartsWith("vgi_rpc.log_", StringComparison.Ordinal) || key.StartsWith("vgi_rpc.error_", StringComparison.Ordinal)) == true) throw new InvalidOperationException("Nested metadata cannot contain external pointers.");
                    for (var column = 0; column < expected.FieldsList.Count; column++)
                        for (var row = 0; row < batch.Length; row++) ValidateValue(batch.Column(column), expected.FieldsList[column], row);
                    for (var row = 0; row < batch.Length; row++)
                    {
                        var slice = new RecordBatch(batch.Schema, batch.Arrays.Select(a => ArrowArrayFactory.BuildArray(a.Data.Slice(row, 1))).ToArray(), 1);
                        slices.Add(slice);
                        var fields = ValueCodec.ExtractRow(slice, types);
                        var value = new T();
                        for (var i = 0; i < fields.Length; i++) properties[i].SetValue(value, fields[i]);
                        values.Add(value);
                    }
                }
            }
        }
        finally
        {
            // Dictionary arrays share the reader's dictionary buffers between batches.
            // Keep every owner until all batches have been consumed.
            GC.KeepAlive(slices);
            foreach (var owned in batches) owned.Dispose();
        }
        if (input.Position != input.Length) throw new InvalidOperationException("Trailing catalog metadata bytes.");
        return values;
    }

    public static byte[]? Merge<T>(IEnumerable<byte[]> items) where T : class, new() => Encode(items.SelectMany(Decode<T>).ToList());

    /// <summary>Decode and validate every catalog category, preserving raw schema paths.</summary>
    public static List<SchemaContents> DecodeCatalog(CatalogContentsResponse response)
    {
        if (response.NotModified)
        {
            if (response.Etag is null || new[] { response.Schemas, response.Tables, response.Views, response.Functions, response.Macros, response.Indexes }.Any(v => v is not null))
                throw new InvalidOperationException("not_modified requires an ETag and six null streams.");
            return [];
        }
        var schemas = new List<SchemaContents>();
        var paths = new Dictionary<string, SchemaContents>(StringComparer.Ordinal);
        foreach (var info in Decode<SchemaInfo>(response.Schemas))
        {
            if (info.Path.Any(string.IsNullOrEmpty)) throw new InvalidOperationException("Catalog schema path components must be nonempty strings.");
            var key = CatalogRegistry.PathKey(info.Path);
            if (info.Path.Count == 0 || paths.ContainsKey(key)) throw new InvalidOperationException("Empty or duplicate catalog schema path.");
            if (info.Path.Count > 1 && !paths.ContainsKey(CatalogRegistry.PathKey(info.Path.Take(info.Path.Count - 1).ToList())))
                throw new InvalidOperationException("Catalog schema parent is missing or out of order.");
            var entry = new SchemaContents { Path = info.Path, Schema = EmbeddedIpc.Encode(info) };
            schemas.Add(entry);
            paths.Add(key, entry);
        }
        SchemaContents Owner(List<string> path) => paths.TryGetValue(CatalogRegistry.PathKey(path), out var schema)
            ? schema : throw new InvalidOperationException("Catalog object refers to an absent schema.");
        foreach (var row in Decode<TableInfo>(response.Tables)) Owner(row.SchemaPath).Tables.Add(EmbeddedIpc.Encode(row));
        foreach (var row in Decode<ViewInfo>(response.Views)) Owner(row.SchemaPath).Views.Add(EmbeddedIpc.Encode(row));
        foreach (var row in Decode<FunctionInfo>(response.Functions))
        {
            var schema = Owner(row.SchemaPath);
            var items = row.FunctionType switch
            {
                FunctionType.Scalar => schema.ScalarFunctions,
                FunctionType.Aggregate => schema.AggregateFunctions,
                FunctionType.Table or FunctionType.TableBuffering => schema.TableFunctions,
                _ => throw new InvalidOperationException("Unknown catalog function type."),
            };
            items.Add(EmbeddedIpc.Encode(row));
        }
        foreach (var row in Decode<MacroInfo>(response.Macros))
        {
            var schema = Owner(row.SchemaPath);
            var items = row.MacroType switch
            {
                MacroType.Scalar => schema.ScalarMacros,
                MacroType.Table => schema.TableMacros,
                _ => throw new InvalidOperationException("Unknown catalog macro type."),
            };
            items.Add(EmbeddedIpc.Encode(row));
        }
        foreach (var row in Decode<IndexInfo>(response.Indexes)) Owner(row.SchemaPath).Indexes.Add(EmbeddedIpc.Encode(row));
        return schemas;
    }

    private static void ValidateValue(IArrowArray array, Field field, int row)
    {
        if (array.IsNull(row))
        {
            if (!field.IsNullable) throw new InvalidOperationException($"Required catalog field {field.Name} is null.");
            return;
        }
        if (array is StructArray structure && field.DataType is StructType structType)
        {
            for (var i = 0; i < structType.Fields.Count; i++) ValidateValue(structure.Fields[i], structType.Fields[i], row + array.Offset);
        }
        else if (field.DataType is ListType or MapType)
        {
            var offsets = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span);
            var child = ArrowArrayFactory.BuildArray(array.Data.Children[0]);
            var childField = ((NestedType)field.DataType).Fields[0];
            for (var i = offsets[array.Offset + row]; i < offsets[array.Offset + row + 1]; i++) ValidateValue(child, childField, i);
            GC.KeepAlive(child);
        }
        else if (array is DictionaryArray dictionary)
        {
            var index = dictionary.Indices switch { Int16Array a => (int)a.GetValue(row)!.Value, Int32Array a => a.GetValue(row)!.Value, _ => throw new InvalidOperationException("Unsupported catalog dictionary index type.") };
            if (index < 0 || index >= dictionary.Dictionary.Length || (dictionary.Dictionary.IsNull(index) && !field.IsNullable)) throw new InvalidOperationException($"Invalid catalog dictionary value in {field.Name}.");
        }
    }

    private static bool SameFields(IReadOnlyList<Field> actual, IReadOnlyList<Field> expected) =>
        actual.Count == expected.Count && actual.Zip(expected).All(pair => pair.First.Name == pair.Second.Name && SameType(pair.First.DataType, pair.Second.DataType));

    private static bool SameType(IArrowType a, IArrowType b)
    {
        if (a.TypeId != b.TypeId) return false;
        return (a, b) switch
        {
            (DictionaryType x, DictionaryType y) => SameType(x.IndexType, y.IndexType) && SameType(x.ValueType, y.ValueType),
            (NestedType x, NestedType y) => SameFields(x.Fields, y.Fields),
            _ => a.Equals(b),
        };
    }

    private sealed class BinaryComparer : IEqualityComparer<byte[]>
    {
        public static readonly BinaryComparer Instance = new();
        public bool Equals(byte[]? left, byte[]? right) => ReferenceEquals(left, right) || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
        public int GetHashCode(byte[] bytes) { var hash = new HashCode(); hash.AddBytes(bytes); return hash.ToHashCode(); }
    }

    // Apache.Arrow's generic concatenator drops dictionary sidecars. Recurse
    // through containers and unify dictionary values before joining indices.
    private static IArrowArray Concatenate(IReadOnlyList<IArrowArray> arrays)
    {
        var type = arrays[0].Data.DataType;
        if (type is DictionaryType dictionaryType)
        {
            if (dictionaryType.IndexType is Int32Type && dictionaryType.ValueType is BinaryType)
            {
                var binaryPositions = new Dictionary<byte[], int>(BinaryComparer.Instance);
                var binaryDictionary = new BinaryArray.Builder();
                var binaryIndices = new Int32Array.Builder();
                foreach (var value in arrays.Cast<DictionaryArray>())
                {
                    var old = (Int32Array)value.Indices;
                    var binary = (BinaryArray)value.Dictionary;
                    for (var row = 0; row < value.Length; row++)
                    {
                        var index = old.GetValue(row);
                        if (index is null) { binaryIndices.AppendNull(); continue; }
                        var bytes = binary.GetBytes(index.Value).ToArray();
                        if (!binaryPositions.TryGetValue(bytes, out var mapped))
                        {
                            mapped = binaryPositions.Count;
                            binaryPositions.Add(bytes, mapped);
                            binaryDictionary.Append(bytes);
                        }
                        binaryIndices.Append(mapped);
                    }
                }
                return new DictionaryArray(dictionaryType, binaryIndices.Build(), binaryDictionary.Build());
            }
            if (dictionaryType.IndexType is not Int16Type || dictionaryType.ValueType is not StringType)
                throw new NotSupportedException("Catalog dictionaries require int16 UTF8 or int32 binary values.");
            var dictionary = new List<string>();
            var positions = new Dictionary<string, short>(StringComparer.Ordinal);
            var indices = new Int16Array.Builder();
            foreach (var value in arrays.Cast<DictionaryArray>())
            {
                var old = (Int16Array)value.Indices;
                var strings = (StringArray)value.Dictionary;
                for (var i = 0; i < value.Length; i++)
                {
                    var index = old.GetValue(i);
                    if (index is null) { indices.AppendNull(); continue; }
                    var text = strings.GetString(index.Value);
                    if (!positions.TryGetValue(text, out var mapped))
                    {
                        mapped = checked((short)dictionary.Count);
                        positions.Add(text, mapped);
                        dictionary.Add(text);
                    }
                    indices.Append(mapped);
                }
            }
            return new DictionaryArray(dictionaryType, indices.Build(), new StringArray.Builder().AppendRange(dictionary).Build());
        }
        if (type is StructType or ListType or MapType)
        {
            var length = arrays.Sum(a => a.Length);
            var valid = new ArrowBuffer.BitmapBuilder();
            var nulls = 0;
            foreach (var array in arrays) for (var i = 0; i < array.Length; i++) { var present = !array.IsNull(i); valid.Append(present); if (!present) nulls++; }
            if (type is StructType structure)
            {
                var children = Enumerable.Range(0, structure.Fields.Count).Select(i => Concatenate(arrays.Select(a => ArrowArrayFactory.BuildArray(a.Data.Children[i].Slice(a.Offset, a.Length))).ToArray()).Data).ToArray();
                return new StructArray(new ArrayData(type, length, nulls, 0, [valid.Build()], children));
            }
            var offsets = new ArrowBuffer.Builder<int>().Append(0);
            var total = 0;
            var values = new List<IArrowArray>();
            foreach (var array in arrays)
            {
                var sourceOffsets = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span);
                var start = sourceOffsets[array.Offset];
                for (var row = 0; row < array.Length; row++) offsets.Append(total + sourceOffsets[array.Offset + row + 1] - start);
                var size = sourceOffsets[array.Offset + array.Length] - start;
                values.Add(ArrowArrayFactory.BuildArray(array.Data.Children[0].Slice(start, size)));
                total += size;
            }
            var child = Concatenate(values);
            return ArrowArrayFactory.BuildArray(new ArrayData(type, length, nulls, 0, [valid.Build(), offsets.Build()], [child.Data]));
        }
        return ArrowArrayConcatenator.Concatenate(arrays);
    }
}
