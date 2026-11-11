using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.SimpleWritableWorker;

/// <summary>
/// Created tables (<c>CREATE TABLE</c>, <c>CREATE TABLE ... AS SELECT</c>, <c>DROP TABLE</c>) in
/// schema <c>main</c>, next to the fixed tables — the vgi <c>docs/writable_tx_fixtures.md</c>
/// "simple_writable: created tables" contract, through the SDK's <see cref="AttachTables"/>.
/// A created table has the request's columns, is scanned by <c>simple_writable_created_scan</c>
/// and written by <c>simple_writable_created_insert</c> (both table_only, both told the table by
/// their first argument), takes INSERTs with result modes up to <c>rows</c>, and has no row id,
/// update or delete function.
/// </summary>
/// <remarks>
/// Storage is per attach (its <see cref="AttachData.AttachId"/>), shared by every worker process:
/// <c>&lt;temp&gt;/vgi-csharp-simple-writable/&lt;attach id&gt;/&lt;hex table name&gt;/</c> holds the
/// table's column schema and one file per inserted batch. A table directory appears with one
/// rename, so a half-created table is never listed.
/// </remarks>
public sealed class CreatedTables : AttachTables
{
    private const string SchemaName = "main";
    private const string SchemaFile = "schema.arrow";
    private const string RowsDir = "rows";

    private readonly CreatedScanFunction _scan;
    private readonly CreatedInsertFunction _insert;

    public CreatedTables()
    {
        _scan = new CreatedScanFunction(this);
        _insert = new CreatedInsertFunction(this);
    }

    public override IReadOnlyList<ITableFunction> ScanFunctions => [_scan];

    public override IReadOnlyList<ITableInOutFunction> WriteFunctions => [_insert];

    public override IEnumerable<CatalogTable> Tables(AttachTablesContext context)
    {
        if (context.SchemaPath is { } path && !IsMain(path))
        {
            return [];
        }

        var root = AttachRoot(context.AttachOpaqueData);
        if (!Directory.Exists(root))
        {
            return [];
        }

        var tables = new List<CatalogTable>();
        foreach (var dir in Directory.GetDirectories(root).Order(StringComparer.Ordinal))
        {
            var schemaPath = Path.Combine(dir, SchemaFile);
            if (!File.Exists(schemaPath) || DecodeName(Path.GetFileName(dir)) is not { } name)
            {
                continue;
            }

            tables.Add(Describe(name, SchemaIpc.ReadSchemaOnly(File.ReadAllBytes(schemaPath))));
        }

        return tables;
    }

    public override bool CreateTable(TableCreateContext context)
    {
        if (!IsMain(context.SchemaPath))
        {
            throw new InvalidOperationException($"simple_writable: tables can only be created in schema '{SchemaName}'.");
        }

        if (context.Request.ConstraintDefinitions.Count > 0)
        {
            throw new NotSupportedException("simple_writable: created tables do not support constraints.");
        }

        var columns = context.Columns;
        if (columns.FieldsList.Count == 0)
        {
            throw new InvalidOperationException("simple_writable: a created table needs at least one column.");
        }

        var dir = TableDir(context.AttachOpaqueData, context.Name);
        if (Directory.Exists(dir))
        {
            switch (context.OnConflict)
            {
                case OnConflict.Ignore:
                    return false;
                case OnConflict.Replace:
                    Remove(dir);
                    break;
                default:
                    throw new InvalidOperationException($"Table with name {context.Name} already exists!");
            }
        }

        var staging = $"{dir}.{Guid.NewGuid():N}.tmp";
        Directory.CreateDirectory(Path.Combine(staging, RowsDir));
        File.WriteAllBytes(Path.Combine(staging, SchemaFile), SchemaIpc.WriteSchemaOnly(columns));
        try
        {
            Directory.Move(staging, dir);
        }
        catch (IOException) when (Directory.Exists(dir))
        {
            // Another process created it meanwhile.
            Remove(staging);
            return context.OnConflict == OnConflict.Ignore
                ? false
                : throw new InvalidOperationException($"Table with name {context.Name} already exists!");
        }

        return true;
    }

    public override void DropTable(TableDropContext context)
    {
        var dir = IsMain(context.SchemaPath) ? TableDir(context.AttachOpaqueData, context.Name) : null;
        if (dir is null || !Directory.Exists(dir))
        {
            if (context.IgnoreNotFound)
            {
                return;
            }

            throw new VgiNotFoundException($"Table with name {context.Name} does not exist!");
        }

        Remove(dir);
    }

    /// <summary>The created table <paramref name="name"/>'s columns, or an error naming it.</summary>
    internal Schema ColumnsOf(byte[] attachOpaqueData, string name)
    {
        var schemaPath = Path.Combine(TableDir(attachOpaqueData, name), SchemaFile);
        return File.Exists(schemaPath)
            ? SchemaIpc.ReadSchemaOnly(File.ReadAllBytes(schemaPath))
            : throw new InvalidOperationException($"simple_writable: unknown table '{name}'.");
    }

    /// <summary>Every batch stored for <paramref name="name"/>, in insertion order.</summary>
    internal IReadOnlyList<RecordBatch> Batches(byte[] attachOpaqueData, string name)
    {
        var dir = Path.Combine(TableDir(attachOpaqueData, name), RowsDir);
        return Directory.Exists(dir)
            ? Directory.GetFiles(dir, "*.bin").Order(StringComparer.Ordinal).Select(f => RecordBatchIpc.Read(File.ReadAllBytes(f))).ToList()
            : throw new InvalidOperationException($"simple_writable: unknown table '{name}'.");
    }

    /// <summary>Stores one inserted batch of <paramref name="name"/>.</summary>
    internal void Append(byte[] attachOpaqueData, string name, RecordBatch batch)
    {
        var dir = Path.Combine(TableDir(attachOpaqueData, name), RowsDir);
        if (!Directory.Exists(dir))
        {
            throw new InvalidOperationException($"simple_writable: unknown table '{name}'.");
        }

        var file = Path.Combine(dir, $"{DateTime.UtcNow.Ticks:D20}_{Guid.NewGuid():N}.bin");
        File.WriteAllBytes(file + ".tmp", RecordBatchIpc.Write(batch));
        File.Move(file + ".tmp", file);
    }

    private CatalogTable Describe(string name, Schema columns) => new()
    {
        Name = name,
        SchemaName = SchemaName,
        Columns = columns,
        ScanFunction = _scan,
        ScanArguments = [name],
        InsertFunction = _insert,
        WriteArguments = [name],
        WriteResultModes = new Dictionary<string, string> { [WriteOperation.Insert] = WriteResultMode.Rows },
    };

    private static bool IsMain(IReadOnlyList<string> path) => path is [SchemaName];

    private static string AttachRoot(byte[] attachOpaqueData)
    {
        var attach = AttachData.Parse(attachOpaqueData);
        var id = attach.AttachId is { Length: > 0 } attachId
            ? Convert.ToHexStringLower(attachId)
            : Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(attachOpaqueData));
        return Path.Combine(Path.GetTempPath(), "vgi-csharp-simple-writable", id);
    }

    private static string TableDir(byte[] attachOpaqueData, string name) =>
        Path.Combine(AttachRoot(attachOpaqueData), Convert.ToHexStringLower(Encoding.UTF8.GetBytes(name)));

    private static string? DecodeName(string hex)
    {
        try
        {
            return Encoding.UTF8.GetString(Convert.FromHexString(hex));
        }
        catch (FormatException)
        {
            return null;
        }
    }

    private static void Remove(string dir)
    {
        // Rename away first so no reader sees a half-deleted table.
        var doomed = $"{dir}.{Guid.NewGuid():N}.drop";
        Directory.Move(dir, doomed);
        Directory.Delete(doomed, recursive: true);
    }
}

/// <summary><c>simple_writable_created_scan(table_name)</c>: every stored batch of a created table,
/// read once when the scan starts.</summary>
public sealed class CreatedScanFunction(CreatedTables tables) : ITableFunction
{
    public string Name => "simple_writable_created_scan";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("table_name", StringType.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public FunctionStability? Stability => FunctionStability.Volatile;

    public Schema ResolveOutputSchema(TableBindParams bindParams) =>
        Plain(tables.ColumnsOf(bindParams.AttachOpaqueData, TableName(bindParams.Arguments)));

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(initParams.OutputSchema, tables.Batches(initParams.AttachOpaqueData, TableName(initParams.Arguments)));

    internal static string TableName(Arguments.TableArguments arguments) =>
        arguments.Positional(0) as string ?? throw new InvalidOperationException("simple_writable: table_name argument is required.");

    /// <summary>The columns without field metadata (defaults, comments), as a result schema.</summary>
    internal static Schema Plain(Schema columns) =>
        new(columns.FieldsList.Select(f => new Field(f.Name, f.DataType, f.IsNullable)), metadata: null);

    private sealed class Producer(Schema outputSchema, IReadOnlyList<RecordBatch> batches) : ITableFunctionProducer
    {
        private int _next;

        public void Produce(OutputCollector output)
        {
            if (_next < batches.Count)
            {
                var batch = batches[_next++];
                output.Emit(new RecordBatch(outputSchema, Enumerable.Range(0, batch.ColumnCount).Select(batch.Column), batch.Length));
                return;
            }

            output.Finish();
        }
    }
}

/// <summary><c>simple_writable_created_insert(table_name, data)</c>: appends each batch DuckDB
/// sends (every column of the created table, in order) and answers <c>count</c> or <c>rows</c>.</summary>
public sealed class CreatedInsertFunction(CreatedTables tables) : ITableInOutFunction
{
    public string Name => "simple_writable_created_insert";

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("table_name", StringType.Default),
            TableArgFields.Table("data"),
            TableArgFields.Named("write_options", BinaryType.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public FunctionStability? Stability => FunctionStability.Volatile;

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) =>
        WriteResults.Schema(
            WriteRequest.From(bindParams.Arguments).ResultMode,
            CreatedScanFunction.Plain(tables.ColumnsOf(bindParams.AttachOpaqueData, CreatedScanFunction.TableName(bindParams.Arguments))));

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams)
    {
        var name = CreatedScanFunction.TableName(initParams.Arguments);
        var columns = CreatedScanFunction.Plain(tables.ColumnsOf(initParams.AttachOpaqueData, name));
        return new Processor(tables, initParams.AttachOpaqueData, name, columns, WriteRequest.From(initParams.Arguments).ResultMode);
    }

    private sealed class Processor(CreatedTables tables, byte[] attach, string name, Schema columns, string mode) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            if (input.ColumnCount != columns.FieldsList.Count)
            {
                throw new InvalidOperationException(
                    $"simple_writable: insert into '{name}' sent {input.ColumnCount} columns, want {columns.FieldsList.Count}.");
            }

            for (var i = 0; i < input.ColumnCount; i++)
            {
                var want = columns.GetFieldByIndex(i).DataType;
                var got = input.Column(i).Data.DataType;
                if (want.TypeId != got.TypeId)
                {
                    throw new InvalidOperationException(
                        $"simple_writable: insert into '{name}' column '{columns.GetFieldByIndex(i).Name}' is {got.Name}, want {want.Name}.");
                }
            }

            var stored = new RecordBatch(columns, Enumerable.Range(0, input.ColumnCount).Select(input.Column), input.Length);
            tables.Append(attach, name, stored);
            output.Emit(mode == WriteResultMode.Count ? WriteResults.CountBatch(input.Length) : stored);
        }
    }
}
