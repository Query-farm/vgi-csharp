using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.ExampleWorker.Aggregate;
using QueryFarm.Vgi.ExampleWorker.Buffering;
using QueryFarm.Vgi.ExampleWorker.Scalar;
using QueryFarm.Vgi.ExampleWorker.Table;
using QueryFarm.Vgi.ExampleWorker.TableInOut;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

/// <summary>
/// What this port writes, value by value, against the reference's rules -- the schema-level checks
/// in <see cref="GeneratedProtocolConformanceTests"/> cannot see either of these.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>A dictionary-encoded enum column carries the Python enum member <em>name</em>
/// (<c>function_type</c> is one of <c>SCALAR</c>/<c>TABLE</c>/<c>AGGREGATE</c>/<c>TABLE_BUFFERING</c>,
/// <c>macro_type</c> one of <c>SCALAR</c>/<c>TABLE</c>), never the lowercase enum value.</item>
/// <item>No null is written into a non-nullable column at any depth: a non-nullable list or map
/// is never null, and a list whose item field is non-nullable holds no null item. A null under a
/// null parent is not a value and is not counted.</item>
/// </list>
/// Every catalog item a registry with one of every kind produces is decoded from the exact bytes
/// the worker would send and walked.
/// </remarks>
public class WireValueConformanceTests
{
    private static readonly Schema OneColumn = new([new Field("x", Int64Type.Default, nullable: true)], metadata: null);

    private static readonly Dictionary<string, string[]> s_enumNames = new(StringComparer.Ordinal)
    {
        ["function_type"] = ["SCALAR", "TABLE", "AGGREGATE", "TABLE_BUFFERING"],
        ["macro_type"] = ["SCALAR", "TABLE"],
    };

    private static CatalogRegistry Registry()
    {
        var registry = new CatalogRegistry();
        registry.RegisterScalar(new AddValuesFunction());
        registry.RegisterTable(new SequenceFunction());
        registry.RegisterAggregate(new SumFunction("wire_sum"));
        registry.RegisterTableInOut(new EchoFunction());
        registry.RegisterTableBuffering(new BufferInputFunction("wire_buffer", "buffers its input"));
        registry.RegisterSchema("data", "Data schema", new Dictionary<string, string> { ["k"] = "v" });
        registry.RegisterCatalogTable(new CatalogTable { Name = "plain", SchemaPath = ["data"], Columns = OneColumn });
        registry.RegisterView(new CatalogView { Name = "data_view", SchemaName = "data", Definition = "SELECT 2 AS two" });
        registry.RegisterMacro(new CatalogMacro
        {
            Name = "twice",
            SchemaName = "data",
            MacroType = MacroType.Scalar,
            Parameters = ["v"],
            Definition = "v * 2",
        });
        registry.RegisterMacro(new CatalogMacro
        {
            Name = "rows_of",
            SchemaName = "data",
            MacroType = MacroType.Table,
            Definition = "SELECT 42 AS answer",
        });
        return registry;
    }

    private static async Task<List<(string Kind, byte[] Item)>> AllItemsAsync()
    {
        IVgiService service = new VgiServiceImpl(Registry());
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData;
        var contents = await service.CatalogContentsAsync(attach);
        var items = new List<(string, byte[])>();
        foreach (var entry in MetadataCollections.DecodeCatalog(contents))
        {
            items.Add(("schema", entry.Schema));
            items.AddRange(entry.Tables.Select(i => ("table", i)));
            items.AddRange(entry.Views.Select(i => ("view", i)));
            items.AddRange(entry.ScalarFunctions.Select(i => ("scalar_function", i)));
            items.AddRange(entry.AggregateFunctions.Select(i => ("aggregate_function", i)));
            items.AddRange(entry.TableFunctions.Select(i => ("table_function", i)));
            items.AddRange(entry.ScalarMacros.Select(i => ("scalar_macro", i)));
            items.AddRange(entry.TableMacros.Select(i => ("table_macro", i)));
            items.AddRange(entry.Indexes.Select(i => ("index", i)));
        }

        return items;
    }

    [Fact]
    public async Task EnumColumns_CarryThePythonMemberName()
    {
        var seen = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        foreach (var (kind, item) in await AllItemsAsync())
        {
            using var batch = RecordBatchIpc.Read(item);
            for (var i = 0; i < batch.ColumnCount; i++)
            {
                var field = batch.Schema.GetFieldByIndex(i);
                if (!s_enumNames.TryGetValue(field.Name, out var allowed))
                {
                    continue;
                }

                var dictionary = Assert.IsType<DictionaryArray>(batch.Column(i));
                var values = Assert.IsType<StringArray>(dictionary.Dictionary);
                var indices = (Int16Array)dictionary.Indices;
                for (var row = 0; row < batch.Length; row++)
                {
                    var name = values.GetString(indices.Values[row]);
                    Assert.True(allowed.Contains(name), $"{kind}.{field.Name} = '{name}', expected one of {string.Join("/", allowed)}");
                    (seen.TryGetValue(field.Name, out var set) ? set : seen[field.Name] = []).Add(name);
                }
            }
        }

        Assert.Equal(["AGGREGATE", "SCALAR", "TABLE", "TABLE_BUFFERING"], seen["function_type"].Order(StringComparer.Ordinal));
        Assert.Equal(["SCALAR", "TABLE"], seen["macro_type"].Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task NoNullIsWrittenIntoANonNullableColumn()
    {
        var violations = new List<string>();
        foreach (var (kind, item) in await AllItemsAsync())
        {
            using var batch = RecordBatchIpc.Read(item);
            for (var i = 0; i < batch.ColumnCount; i++)
            {
                var field = batch.Schema.GetFieldByIndex(i);
                Walk($"{kind}.{field.Name}", field, batch.Column(i), parentValid: null, violations);
            }
        }

        Assert.Empty(violations);
    }

    /// <summary>The walker itself: a null where the field forbids one is caught at the top, inside
    /// a list's items, and under a valid struct parent.</summary>
    [Fact]
    public void Walker_CatchesEachKindOfViolation()
    {
        var violations = new List<string>();
        var nonNullItems = new ListType(new Field("item", StringType.Default, nullable: false));
        var items = new StringArray.Builder().Append("a").AppendNull().Build();
        var list = new ListArray(nonNullItems, 1,
            new ArrowBuffer.Builder<int>().Append(0).Append(2).Build(), items, ArrowBuffer.Empty, nullCount: 0);
        Walk("l", new Field("l", nonNullItems, nullable: false), list, null, violations);
        Assert.Equal(["l[] row 1"], violations);

        violations.Clear();
        var nullList = new ListArray.Builder(StringType.Default).AppendNull().Build();
        Walk("n", new Field("n", nullList.Data.DataType, nullable: false), nullList, null, violations);
        Assert.Equal(["n row 0"], violations);
    }

    private static void Walk(string path, Field field, IArrowArray array, bool[]? parentValid, List<string> violations)
    {
        var valid = new bool[array.Length];
        for (var row = 0; row < array.Length; row++)
        {
            var parentOk = parentValid is null || parentValid[row];
            valid[row] = parentOk && !array.IsNull(row);
            if (parentOk && array.IsNull(row) && !field.IsNullable)
            {
                violations.Add($"{path} row {row}");
            }
        }

        if (array is MapArray map)
        {
            var mapType = (MapType)field.DataType;
            var keyValid = ChildValidity(map, valid);
            Walk(path + "{key}", mapType.KeyField, map.Keys, keyValid, violations);
            Walk(path + "{value}", mapType.ValueField, map.Values, keyValid, violations);
        }
        else if (array is ListArray list)
        {
            Walk(path + "[]", ((ListType)field.DataType).ValueField, list.Values, ChildValidity(list, valid), violations);
        }
        else if (array is StructArray structArray)
        {
            var structType = (StructType)field.DataType;
            for (var i = 0; i < structType.Fields.Count; i++)
            {
                Walk(path + "." + structType.Fields[i].Name, structType.Fields[i], structArray.Fields[i], valid, violations);
            }
        }
    }

    /// <summary>Per child element: whether the list slot owning it is itself a value.</summary>
    private static bool[] ChildValidity(ListArray list, bool[] valid)
    {
        var child = new bool[list.Values.Length];
        for (var row = 0; row < list.Length; row++)
        {
            for (var j = list.ValueOffsets[row]; j < list.ValueOffsets[row + 1]; j++)
            {
                child[j] = valid[row];
            }
        }

        return child;
    }
}
