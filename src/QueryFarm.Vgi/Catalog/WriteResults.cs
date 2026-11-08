using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Catalog;

/// <summary>The write operations a <see cref="CatalogTable"/> can declare in
/// <see cref="CatalogTable.WriteResultModes"/>.</summary>
public static class WriteOperation
{
    public const string Insert = "insert";
    public const string Update = "update";
    public const string Delete = "delete";

    /// <summary>All three, in protocol order.</summary>
    public static IReadOnlyList<string> All { get; } = [Insert, Update, Delete];
}

/// <summary>
/// Write result modes, ordered <c>count &lt; rows &lt; changes</c>: a table that supports one mode
/// supports every lower one. DuckDB asks for one per statement in the <c>write_options</c> named
/// argument of the write function's bind (<see cref="WriteRequest"/>).
/// </summary>
public static class WriteResultMode
{
    /// <summary>Plain INSERT/UPDATE/DELETE: one row, <c>count BIGINT NOT NULL</c>.</summary>
    public const string Count = "count";

    /// <summary><c>... RETURNING</c>: the written rows, in the table's user columns.</summary>
    public const string Rows = "rows";

    /// <summary>OLD/NEW pairs: <c>old STRUCT(&lt;table&gt;)</c>, <c>new STRUCT(&lt;table&gt;)</c>.</summary>
    public const string Changes = "changes";

    /// <summary>All three, lowest first.</summary>
    public static IReadOnlyList<string> All { get; } = [Count, Rows, Changes];

    /// <summary>Whether a table whose maximum is <paramref name="maximum"/> supports <paramref name="requested"/>.</summary>
    public static bool Supports(string maximum, string requested) =>
        Rank(maximum) is { } max && Rank(requested) is { } req && max >= req;

    /// <summary>Validates an operation → maximum-mode map (as assigned to
    /// <see cref="CatalogTable.WriteResultModes"/>) and returns a copy.</summary>
    /// <exception cref="ArgumentException">An unknown operation or mode.</exception>
    public static IReadOnlyDictionary<string, string> Validate(IReadOnlyDictionary<string, string> modes)
    {
        ArgumentNullException.ThrowIfNull(modes);
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (operation, mode) in modes)
        {
            if (!WriteOperation.All.Contains(operation))
            {
                throw new ArgumentException($"Unknown write operation '{operation}' (expected insert, update or delete).", nameof(modes));
            }

            if (Rank(mode) is null)
            {
                throw new ArgumentException($"Unknown write result mode '{mode}' for '{operation}' (expected count, rows or changes).", nameof(modes));
            }

            result[operation] = mode;
        }

        return result;
    }

    internal static int? Rank(string? mode) => mode switch
    {
        Count => 0,
        Rows => 1,
        Changes => 2,
        _ => null,
    };
}

/// <summary>
/// What DuckDB asked a write function for: the <c>write_options</c> named argument of an
/// INSERT/UPDATE/DELETE function's bind, decoded — the C# counterpart of vgi-typescript's
/// <c>writeOptions(bindCall)</c>.
/// </summary>
public sealed class WriteRequest
{
    /// <summary>One of <see cref="WriteResultMode"/>; <see cref="WriteResultMode.Count"/> when absent.</summary>
    public string ResultMode { get; init; } = WriteResultMode.Count;

    /// <summary><c>throw</c> (the default), or the <c>INSERT ... ON CONFLICT</c> action requested.</summary>
    public string OnConflict { get; init; } = "throw";

    /// <summary>The <c>ON CONFLICT (...)</c> target columns; empty when none.</summary>
    public IReadOnlyList<string> OnConflictColumns { get; init; } = [];

    /// <summary>Decodes the write function's arguments (<see cref="TableInOut.TableInOutBindParams.Arguments"/>
    /// or <see cref="TableInOut.TableInOutInitParams.Arguments"/>). Missing <c>write_options</c>
    /// means a plain <see cref="WriteResultMode.Count"/> write.</summary>
    /// <exception cref="InvalidOperationException">The result mode is not one of <see cref="WriteResultMode"/>.</exception>
    public static WriteRequest From(Arguments.TableArguments arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        var decoded = WriteOptions.Decode(arguments);
        if (WriteResultMode.Rank(decoded.ResultMode) is null)
        {
            throw new InvalidOperationException($"Unknown write result mode '{decoded.ResultMode}'.");
        }

        return new WriteRequest
        {
            ResultMode = decoded.ResultMode,
            OnConflict = decoded.OnConflict,
            OnConflictColumns = decoded.OnConflictColumns.ToList(),
        };
    }
}

/// <summary>
/// Writable-table helpers: the row-id column, the result schema a write function must bind for a
/// <see cref="WriteResultMode"/>, and the result batches — the C# counterparts of vgi-typescript's
/// <c>rowIdField</c>, <c>userColumns</c>, <c>writeResultSchema</c> and <c>writeResultBatch</c>.
/// </summary>
/// <remarks>
/// A write function is an ordinary <see cref="TableInOut.ITableInOutFunction"/> named on the
/// <see cref="CatalogTable"/> (<see cref="CatalogTable.InsertFunction"/>,
/// <see cref="CatalogTable.UpdateFunction"/>, <see cref="CatalogTable.DeleteFunction"/>). Declare its
/// arguments as <c>[TableArgFields.Table("data"), TableArgFields.Named("write_options", BinaryType.Default)]</c>,
/// bind <c>WriteResults.Schema(WriteRequest.From(bind.Arguments).ResultMode, userColumns)</c>, and
/// emit <c>WriteResults.Batch(...)</c> per input batch. UPDATE's input carries the SET columns plus
/// the row-id column last; DELETE's carries the row-id column.
/// </remarks>
public static class WriteResults
{
    /// <summary>A row-id field: DuckDB's conventional <c>rowid BIGINT</c> by default, carrying the
    /// <c>is_row_id</c> metadata. Add it to the table's <see cref="CatalogTable.Columns"/> and the
    /// scan function's output, and name it in <see cref="CatalogTable.RowIdColumn"/> (which also
    /// applies the metadata, so a plain field works there too).
    /// <para>DuckDB reads the row id as a <i>virtual</i> column, which it only requests from a scan
    /// that supports projection pushdown: the table's <see cref="CatalogTable.ScanFunction"/> must
    /// declare <see cref="Table.ITableFunction.ProjectionPushdown"/> <see langword="true"/> and emit
    /// <see cref="Table.TableInitParams.ProjectedSchema"/>. Registration refuses a table that names a
    /// row-id column over a scan without it. Row ids must be unique and stable for the life of a
    /// statement; a row id must be a column of its own (not a user column such as <c>id</c>), since
    /// DuckDB hides it from <c>SELECT *</c> and from INSERT's column list.</para></summary>
    public static Field RowIdField(string name = "rowid", IArrowType? type = null) =>
        new(name, type ?? Int64Type.Default, nullable: false,
            new Dictionary<string, string> { [VgiRowIdMetadata.Key] = VgiRowIdMetadata.Value });

    /// <summary>Whether <paramref name="field"/> carries the row-id metadata.</summary>
    public static bool IsRowId(Field field) => field.Metadata?.ContainsKey(VgiRowIdMetadata.Key) == true;

    /// <summary>The table's user columns: <paramref name="columns"/> without the row-id field (by
    /// metadata, or by <paramref name="rowIdColumn"/> name). Write results — <c>rows</c>, and both
    /// halves of <c>changes</c> — use this schema.</summary>
    public static Schema UserColumns(Schema columns, string? rowIdColumn = null)
    {
        ArgumentNullException.ThrowIfNull(columns);
        return new Schema(
            columns.FieldsList.Where(f => !IsRowId(f) && !string.Equals(f.Name, rowIdColumn, StringComparison.Ordinal)),
            metadata: null);
    }

    /// <summary>The <c>count</c> result schema: <c>count BIGINT NOT NULL</c>.</summary>
    public static Schema CountSchema { get; } = new([new Field("count", Int64Type.Default, nullable: false)], metadata: null);

    /// <summary>The exact output schema a write function must bind for <paramref name="mode"/>.</summary>
    /// <param name="mode">One of <see cref="WriteResultMode"/>.</param>
    /// <param name="userColumns">The table's user columns (<see cref="UserColumns"/>).</param>
    public static Schema Schema(string mode, Schema userColumns)
    {
        ArgumentNullException.ThrowIfNull(userColumns);
        switch (mode)
        {
            case WriteResultMode.Count:
                return CountSchema;
            case WriteResultMode.Rows:
                return userColumns;
            case WriteResultMode.Changes:
                var rowType = new StructType(userColumns.FieldsList);
                return new Schema([new Field("old", rowType, nullable: true), new Field("new", rowType, nullable: true)], metadata: null);
            default:
                throw new ArgumentException($"Unknown write result mode '{mode}'.", nameof(mode));
        }
    }

    /// <summary>A <c>count</c> result batch.</summary>
    public static RecordBatch CountBatch(long count) =>
        new(CountSchema, [new Int64Array.Builder().Append(count).Build()], 1);

    /// <summary>A <c>rows</c> result batch: the written rows, values keyed by column name (a
    /// missing key is NULL). Column types must be ones <see cref="BuildBatch"/> supports.</summary>
    public static RecordBatch RowsBatch(Schema userColumns, IReadOnlyList<IReadOnlyDictionary<string, object?>> rows) =>
        BuildBatch(userColumns, rows);

    /// <summary>A <c>changes</c> result batch, one OLD/NEW pair per written row: an
    /// <paramref name="oldRows"/> entry is <see langword="null"/> for an insert, a
    /// <paramref name="newRows"/> entry for a delete.</summary>
    public static RecordBatch ChangesBatch(
        Schema userColumns,
        IReadOnlyList<IReadOnlyDictionary<string, object?>?> oldRows,
        IReadOnlyList<IReadOnlyDictionary<string, object?>?> newRows)
    {
        if (oldRows.Count != newRows.Count)
        {
            throw new ArgumentException("oldRows and newRows must have equal length.", nameof(newRows));
        }

        return new RecordBatch(
            Schema(WriteResultMode.Changes, userColumns),
            [StructOf(userColumns, oldRows), StructOf(userColumns, newRows)],
            oldRows.Count);
    }

    /// <summary>The result batch for <paramref name="mode"/>, from the written rows' before/after
    /// images (pass <see langword="null"/> for the missing half: old for an insert, new for a
    /// delete). <c>rows</c> returns each row as written — the new image, or the old one for a delete.</summary>
    public static RecordBatch Batch(
        string mode,
        Schema userColumns,
        IReadOnlyList<IReadOnlyDictionary<string, object?>?> oldRows,
        IReadOnlyList<IReadOnlyDictionary<string, object?>?> newRows)
    {
        if (oldRows.Count != newRows.Count)
        {
            throw new ArgumentException("oldRows and newRows must have equal length.", nameof(newRows));
        }

        return mode switch
        {
            WriteResultMode.Count => CountBatch(newRows.Count),
            WriteResultMode.Changes => ChangesBatch(userColumns, oldRows, newRows),
            WriteResultMode.Rows => RowsBatch(userColumns, newRows.Select((row, i) => row ?? oldRows[i]
                ?? throw new ArgumentException($"Row {i} has neither an old nor a new image.", nameof(newRows))).ToList()),
            _ => throw new ArgumentException($"Unknown write result mode '{mode}'.", nameof(mode)),
        };
    }

    /// <summary>Every row of <paramref name="batch"/> as a column-name → value map (boxed CLR
    /// values, <see langword="null"/> for SQL NULL) — e.g. to read a write function's input rows.</summary>
    public static IReadOnlyList<Dictionary<string, object?>> ReadRows(RecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        var rows = new List<Dictionary<string, object?>>(batch.Length);
        for (var r = 0; r < batch.Length; r++)
        {
            var row = new Dictionary<string, object?>(StringComparer.Ordinal);
            for (var c = 0; c < batch.ColumnCount; c++)
            {
                row[batch.Schema.GetFieldByIndex(c).Name] = ScalarArgCodec.ReadScalar(batch.Column(c), r);
            }

            rows.Add(row);
        }

        return rows;
    }

    /// <summary>Builds a batch of <paramref name="schema"/> from row maps keyed by column name (a
    /// missing key is NULL) — the inverse of <see cref="ReadRows"/>. Supports the primitive types:
    /// signed/unsigned integers, float, double, VARCHAR, BOOLEAN and BLOB.</summary>
    public static RecordBatch BuildBatch(Schema schema, IReadOnlyList<IReadOnlyDictionary<string, object?>> rows)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(rows);
        var arrays = schema.FieldsList
            .Select(field => AnyArrayBuilder.Build(field.DataType, rows.Select(row => row.GetValueOrDefault(field.Name)).ToList()))
            .ToArray();
        return new RecordBatch(schema, arrays, rows.Count);
    }

    private static StructArray StructOf(Schema userColumns, IReadOnlyList<IReadOnlyDictionary<string, object?>?> rows)
    {
        IReadOnlyDictionary<string, object?> empty = new Dictionary<string, object?>();
        var values = BuildBatch(userColumns, rows.Select(row => row ?? empty).ToList());
        var validity = new ArrowBuffer.BitmapBuilder();
        var nullCount = 0;
        foreach (var row in rows)
        {
            validity.Append(row is not null);
            if (row is null) nullCount++;
        }

        var children = Enumerable.Range(0, userColumns.FieldsList.Count).Select(values.Column).ToArray();
        return new StructArray(new StructType(userColumns.FieldsList), rows.Count, children, validity.Build(), nullCount);
    }
}
