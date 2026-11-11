using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.SimpleWritableWorker;
using QueryFarm.Vgi.WritableTxWorker;
using Xunit;

namespace QueryFarm.Vgi.Tests.Catalog;

/// <summary>The fixture side of vgi <c>docs/writable_tx_fixtures.md</c> (protocol 3.3 round): the
/// txfix write functions' per-batch rules and staging, and the simple_writable created tables.</summary>
public sealed class FixtureWriteContractTests
{
    private static readonly Schema User = new(
        [
            new Field("id", Int32Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: false),
            new Field("qty", Int64Type.Default, nullable: true),
        ],
        metadata: null);

    private static Dictionary<string, object?> Item(int? id, string? name, long? qty = 0) =>
        new() { ["id"] = id, ["name"] = name, ["qty"] = qty };

    private static Dictionary<string, object?> RowId(long id) => new() { ["rowid"] = id };

    private static List<int> Ids(Partition state, string? tx = null) => state.ItemsFor(tx).Select(row => row.Id).ToList();

    [Fact]
    public void InsertChecksTheWholeBatchBeforeApplyingAny()
    {
        var insert = new ItemsInsertFunction(User);
        var state = Partition.Initial();

        var duplicate = Assert.Throws<InvalidOperationException>(() => insert.Apply(state, null, [Item(4, "d"), Item(1, "dup")]));
        Assert.Equal("txfix: duplicate id 1", duplicate.Message);
        var withinBatch = Assert.Throws<InvalidOperationException>(() => insert.Apply(state, null, [Item(5, "e"), Item(5, "e")]));
        Assert.Equal("txfix: duplicate id 5", withinBatch.Message);
        Assert.Equal("txfix: name required", Assert.Throws<InvalidOperationException>(() => insert.Apply(state, null, [Item(6, null)])).Message);
        Assert.Equal("txfix: id required", Assert.Throws<InvalidOperationException>(() => insert.Apply(state, null, [Item(null, "x")])).Message);
        Assert.Equal("txfix: qty must be >= 0", Assert.Throws<InvalidOperationException>(() => insert.Apply(state, null, [Item(7, "g", -1)])).Message);
        Assert.Equal([1, 2, 3], Ids(state));

        var outcome = insert.Apply(state, null, [Item(4, "d"), Item(5, "e", null)]);
        Assert.Equal(2, outcome.NewRows.Count);
        Assert.All(outcome.OldRows, Assert.Null);
        Assert.Equal([1, 2, 3, 4, 5], Ids(state));
    }

    [Fact]
    public void DeleteCountsEachVisibleRowOnce()
    {
        var delete = new ItemsDeleteFunction(User);
        var state = Partition.Initial();

        var outcome = delete.Apply(state, null, [RowId(2), RowId(2), RowId(99)]);
        Assert.Single(outcome.OldRows);
        Assert.Equal(2, outcome.OldRows[0]!["id"]);
        Assert.Empty(delete.Apply(state, null, [RowId(2)]).OldRows);
        Assert.Equal([1, 3], Ids(state));
    }

    [Fact]
    public void TransactionsStageInsertsUpdatesAndDeletesWithReadYourWrites()
    {
        var (insert, update, delete) = (new ItemsInsertFunction(User), new ItemsUpdateFunction(User), new ItemsDeleteFunction(User));
        var state = Partition.Initial();
        const string tx = "00ff";

        insert.Apply(state, tx, [Item(4, "d", -5)]); // a negative qty is accepted at write time on ttx
        delete.Apply(state, tx, [RowId(1)]);
        update.Apply(state, tx, [new() { ["qty"] = 40L, ["rowid"] = 4L }]); // edits the staged insert
        Assert.Equal([2, 3, 4], Ids(state, tx));
        Assert.Equal(40, state.ItemsFor(tx).Single(row => row.Id == 4).Qty);
        Assert.Equal([1, 2, 3], Ids(state)); // other attaches see committed state

        insert.Apply(state, tx, [Item(1, "again")]); // re-insert of an id the transaction deleted
        Assert.Throws<InvalidOperationException>(() => insert.Apply(state, tx, [Item(4, "dup")]));

        state.Apply(state.Pending[tx]);
        Assert.Equal([1, 2, 3, 4], Ids(state));
        Assert.Equal("again", state.ItemsFor(null).Single(row => row.Id == 1).Name);
    }

    [Fact]
    public async Task SimpleWritableCreatesListsAndDropsTables()
    {
        var registry = new CatalogRegistry();
        registry.RegisterAttachTables(new CreatedTables());
        var service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "simple_writable" })).AttachOpaqueData;
        var columns = SchemaIpc.WriteSchemaOnly(new Schema(
            [new Field("id", Int64Type.Default, true), new Field("label", StringType.Default, true)], metadata: null));
        TableCreateRequest Create(OnConflict onConflict) => new()
        {
            AttachOpaqueData = attach,
            SchemaPath = ["main"],
            Name = "ctas_unit",
            Columns = columns,
            OnConflict = onConflict,
            ConstraintDefinitions = [],
        };

        Assert.True((await service.CatalogTableCreateAsync(Create(OnConflict.Error))).Created);
        try
        {
            Assert.False((await service.CatalogTableCreateAsync(Create(OnConflict.Ignore))).Created);
            Assert.Contains("already exists", (await Assert.ThrowsAsync<InvalidOperationException>(() =>
                service.CatalogTableCreateAsync(Create(OnConflict.Error)))).Message);
            Assert.True((await service.CatalogTableCreateAsync(Create(OnConflict.Replace))).Created);

            var info = EmbeddedIpc.Decode<TableInfo>(Assert.Single(
                (await service.CatalogTableGetAsync(attach, ["main"], "ctas_unit", null, null, null)).Items));
            Assert.Equal(["id", "label"], SchemaIpc.ReadSchemaOnly(info.Columns).FieldsList.Select(f => f.Name));
            Assert.Equal("simple_writable_created_insert", EmbeddedIpc.Decode<ScanFunctionResult>(info.InsertFunction!).FunctionName);
            Assert.Equal(new Dictionary<string, string> { ["insert"] = "rows" }, info.WriteResultModes);
        }
        finally
        {
            await service.CatalogTableDropAsync(attach, ["main"], "ctas_unit", ignoreNotFound: false, cascade: false, null);
        }

        Assert.Empty((await service.CatalogSchemaContentsTablesAsync(attach, ["main"], null)).Items);
    }
}
