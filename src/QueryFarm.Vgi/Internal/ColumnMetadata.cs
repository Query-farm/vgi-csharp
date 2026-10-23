// Copyright 2025, 2026 Query Farm LLC - https://query.farm
using Apache.Arrow;
using Apache.Arrow.Types;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace QueryFarm.Vgi.Internal;

public static class ColumnMetadata
{
    private static readonly UTF8Encoding Utf8 = new(false, true);
    private static void Entry(string key, string value)
    {
        if (key != "comment" && key != "tags")
            return;
        Utf8.GetByteCount(value); // Reject author-supplied unpaired UTF-16 surrogates.
        if (key != "tags")
            return;
        using var document = JsonDocument.Parse(value);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
            throw new ArgumentException("Column tags must be a JSON object of strings.");
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in document.RootElement.EnumerateObject())
        {
            Utf8.GetByteCount(property.Name);
            if (!names.Add(property.Name))
                throw new ArgumentException("Duplicate column tag key.");
            if (property.Value.ValueKind != JsonValueKind.String)
                throw new ArgumentException("Column tag values must be strings.");
            Utf8.GetByteCount(property.Value.GetString()!);
        }
    }
    public static void Validate(Schema schema)
    {
        foreach (var field in schema.FieldsList)
            ValidateField(field);
    }
    private static void ValidateField(Field field)
    {
        if (field.Metadata is { } metadata)
            foreach (var pair in metadata)
                Entry(pair.Key, pair.Value);
        ValidateType(field.DataType);
    }
    private static void ValidateType(IArrowType type)
    {
        if (type is NestedType nested)
            foreach (var child in nested.Fields)
                ValidateField(child);
        if (type is DictionaryType dictionary)
            ValidateType(dictionary.ValueType);
    }
    // Arrow's public reader normalizes malformed UTF-8. Inspect reserved values
    // in the raw schema FlatBuffer first (Message/Schema/Field/KeyValue slots).
    public static void ValidateIpc(byte[] bytes)
    {
        int prefix = BinaryPrimitives.ReadInt32LittleEndian(bytes) == -1 ? 8 : 4;
        int length = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(prefix - 4));
        if (length <= 0 || length > bytes.Length - prefix)
            throw new ArgumentException("Invalid column schema IPC.");
        byte[] raw = bytes.AsSpan(prefix, length).ToArray();
        int I32(int p) => BinaryPrimitives.ReadInt32LittleEndian(raw.AsSpan(p));
        int U16(int p) => BinaryPrimitives.ReadUInt16LittleEndian(raw.AsSpan(p));
        int Slot(int p, int id)
        {
            int v = checked(p - I32(p)), o = 4 + id * 2;
            return o < U16(v) ? U16(v + o) : 0;
        }
        int Ref(int p, int id)
        {
            int o = Slot(p, id);
            return o == 0 ? 0 : checked(p + o + I32(p + o));
        }
        void Vector(int p, int id, Action<int> visit)
        {
            int v = Ref(p, id);
            if (v == 0)
                return;
            int n = I32(v);
            if (n < 0 || n > (raw.Length - v - 4) / 4)
                throw new ArgumentException("Invalid column metadata vector.");
            for (int i = 0; i < n; i++)
            {
                int q = v + 4 + i * 4;
                visit(checked(q + I32(q)));
            }
        }
        string Text(int p, int id)
        {
            int v = Ref(p, id);
            return v == 0 ? "" : Utf8.GetString(raw.AsSpan(v + 4, I32(v)));
        }
        void Field(int p, int depth)
        {
            if (depth > 64)
                throw new ArgumentException("Column metadata nesting too deep.");
            var seen = new HashSet<string>(StringComparer.Ordinal);
            Vector(p, 6, m => {
                string key = Text(m, 0);
                if (!seen.Add(key))
                    throw new ArgumentException("Duplicate column metadata key.");
                if (key == "comment" || key == "tags")
                    Entry(key, Text(m, 1));
            });
            Vector(p, 5, c => Field(c, depth + 1));
        }
        int message = I32(0), kind = Slot(message, 1);
        if (kind == 0 || raw[message + kind] != 1)
            throw new ArgumentException("Expected column schema message.");
        int schema = Ref(message, 2);
        if (schema == 0)
            throw new ArgumentException("Missing column schema.");
        Vector(schema, 1, f => Field(f, 0));
    }
}
