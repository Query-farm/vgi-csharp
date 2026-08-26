using Apache.Arrow;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.SimpleWritableWorker;

/// <summary>Small row-shaped read/write helpers shared by the writable-table fixture functions —
/// generic across column type via <see cref="AnyArrayBuilder"/>/<see cref="ScalarArgCodec"/> (both
/// already handle every scalar Arrow type this fixture's tables use).</summary>
public static class RowCodec
{
    /// <summary>Every column value of one row, by field NAME.</summary>
    public static Dictionary<string, object?> ReadRow(RecordBatch batch, int rowIndex)
    {
        var values = new Dictionary<string, object?>(StringComparer.Ordinal);
        for (var i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            values[batch.Schema.GetFieldByIndex(i).Name] = ScalarArgCodec.ReadScalar(batch.Column(i), rowIndex);
        }

        return values;
    }

    /// <summary>Builds a single-row batch matching <paramref name="schema"/>, reading each field's
    /// value from <paramref name="values"/> by name (missing keys become NULL).</summary>
    public static RecordBatch BuildRow(Schema schema, IReadOnlyDictionary<string, object?> values)
    {
        var arrays = schema.FieldsList
            .Select(field => AnyArrayBuilder.Build(field.DataType, [values.GetValueOrDefault(field.Name)]))
            .ToArray();
        return new RecordBatch(schema, arrays, 1);
    }

    /// <summary>Builds an N-row batch matching <paramref name="schema"/> from N independently-read
    /// row dictionaries (see <see cref="ReadRow"/>) — the RETURNING-batch assembly path.</summary>
    public static RecordBatch BuildBatch(Schema schema, IReadOnlyList<IReadOnlyDictionary<string, object?>> rows)
    {
        var arrays = schema.FieldsList
            .Select(field => AnyArrayBuilder.Build(field.DataType, rows.Select(row => row.GetValueOrDefault(field.Name)).ToList()))
            .ToArray();
        return new RecordBatch(schema, arrays, rows.Count);
    }
}
