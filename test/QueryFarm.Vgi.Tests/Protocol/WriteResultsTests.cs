using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.SimpleWritableWorker;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

public class WriteResultsTests
{
    private static readonly Schema TableSchema = new(
        [new Field("id", Int64Type.Default, nullable: false), new Field("name", StringType.Default, nullable: true)],
        metadata: null);

    [Fact]
    public void ChangesCarriesNullableOldAndNewStructs()
    {
        var before = new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "before" };
        var after = new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "after" };

        var batch = WriteResults.Batch("changes", TableSchema, [before], [after]);

        Assert.Equal(["old", "new"], batch.Schema.FieldsList.Select(field => field.Name));
        var oldRows = Assert.IsType<StructArray>(batch.Column(0));
        var newRows = Assert.IsType<StructArray>(batch.Column(1));
        Assert.False(oldRows.IsNull(0));
        Assert.False(newRows.IsNull(0));
        Assert.Equal("before", Assert.IsType<StringArray>(oldRows.Fields[1]).GetString(0));
        Assert.Equal("after", Assert.IsType<StringArray>(newRows.Fields[1]).GetString(0));
    }

    [Fact]
    public void InsertAndDeleteNullTheMissingImage()
    {
        var row = new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "row" };

        var inserted = WriteResults.Batch("changes", TableSchema, [null], [row]);
        Assert.True(Assert.IsType<StructArray>(inserted.Column(0)).IsNull(0));
        Assert.False(Assert.IsType<StructArray>(inserted.Column(1)).IsNull(0));

        var deleted = WriteResults.Batch("changes", TableSchema, [row], [null]);
        Assert.False(Assert.IsType<StructArray>(deleted.Column(0)).IsNull(0));
        Assert.True(Assert.IsType<StructArray>(deleted.Column(1)).IsNull(0));
    }
}
