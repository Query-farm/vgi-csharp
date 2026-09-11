using Apache.Arrow;
using Apache.Arrow.Types;

namespace QueryFarm.Vgi.SimpleWritableWorker;

/// <summary>Builds the exact VGI 2.0 count, rows, and OLD/NEW result shapes.</summary>
public static class WriteResults
{
    public static Schema Schema(string mode, Schema visibleSchema)
    {
        if (mode == "count") return WriteCount.Schema;
        if (mode == "rows") return visibleSchema;
        if (mode != "changes") throw new InvalidOperationException($"Unknown write result mode '{mode}'.");
        var rowType = new StructType(visibleSchema.FieldsList);
        return new Schema(
            [new Field("old", rowType, nullable: true), new Field("new", rowType, nullable: true)],
            metadata: null);
    }

    public static RecordBatch Batch(
        string mode,
        Schema visibleSchema,
        IReadOnlyList<IReadOnlyDictionary<string, object?>?> oldRows,
        IReadOnlyList<IReadOnlyDictionary<string, object?>?> newRows)
    {
        if (oldRows.Count != newRows.Count)
            throw new InvalidOperationException("OLD and NEW result counts must match.");
        if (mode == "count") return WriteCount.Batch(oldRows.Count);
        if (mode == "rows")
        {
            var rows = newRows.Any(row => row is not null) ? newRows : oldRows;
            return RowCodec.BuildBatch(visibleSchema, rows.Select(row => row!).ToList());
        }
        if (mode != "changes") throw new InvalidOperationException($"Unknown write result mode '{mode}'.");
        var oldArray = BuildStruct(visibleSchema, oldRows);
        var newArray = BuildStruct(visibleSchema, newRows);
        return new RecordBatch(Schema(mode, visibleSchema), [oldArray, newArray], oldRows.Count);
    }

    private static StructArray BuildStruct(
        Schema visibleSchema,
        IReadOnlyList<IReadOnlyDictionary<string, object?>?> rows)
    {
        var empty = visibleSchema.FieldsList.ToDictionary(field => field.Name, _ => (object?)null);
        var normalized = rows.Select(row => row ?? empty).ToList();
        var values = RowCodec.BuildBatch(visibleSchema, normalized);
        var validity = new ArrowBuffer.BitmapBuilder();
        var nullCount = 0;
        foreach (var row in rows)
        {
            var valid = row is not null;
            validity.Append(valid);
            if (!valid) nullCount++;
        }
        var children = Enumerable.Range(0, visibleSchema.FieldsList.Count).Select(values.Column).ToArray();
        return new StructArray(new StructType(visibleSchema.FieldsList), rows.Count, children, validity.Build(), nullCount);
    }
}
