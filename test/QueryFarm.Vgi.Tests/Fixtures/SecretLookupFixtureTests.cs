using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Arguments;
using QueryFarm.Vgi.ExampleWorker.Table;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;
using Xunit;
using TableArguments = QueryFarm.Vgi.Arguments.TableArguments;

namespace QueryFarm.Vgi.Tests.Fixtures;

/// <summary>The example worker's secret-lookup conformance fixtures (vgi
/// <c>docs/secret_lookup_fixtures.md</c>), driven directly with a delivered secrets batch.</summary>
public class SecretLookupFixtureTests
{
    private static IArrowArray Str(string value) => new StringArray.Builder().Append(value).Build();

    private static (string, StructArray) Delivered(string name, string type, string scope, params (string, IArrowArray)[] custom)
    {
        (string Name, IArrowArray Value)[] fields = [("type", Str(type)), ("provider", Str("config")), ("name", Str(name)), .. custom, ("scope", Str(scope))];
        var structType = new StructType(fields.Select(f => new Field(f.Name, f.Value.Data.DataType, nullable: true)).ToList());
        return (name, new StructArray(structType, 1, fields.Select(f => f.Value).ToList(), ArrowBuffer.Empty, nullCount: 0));
    }

    private static byte[] Batch(params (string Column, StructArray Secret)[] columns)
    {
        var schema = new Schema(columns.Select(c => new Field(c.Column, c.Secret.Data.DataType, nullable: true)).ToList(), metadata: null);
        return RecordBatchIpc.Write(new RecordBatch(schema, columns.Select(c => (IArrowArray)c.Secret).ToList(), 1));
    }

    private static List<string> Run(ITableFunction function, byte[]? secrets, params string[] args)
    {
        var producer = function.CreateProducer(new TableInitParams
        {
            FunctionName = function.Name,
            Arguments = new TableArguments([.. args.Select(Str)], new Dictionary<string, IArrowArray>()),
            OutputSchema = function.OutputSchema,
            Secrets = secrets,
        });
        var rows = new List<string>();
        for (var finished = false; !finished;)
        {
            using var output = new OutputCollector(function.OutputSchema);
            producer.Produce(output);
            if (output.EmittedBatch is { } batch)
            {
                for (var r = 0; r < batch.Length; r++)
                {
                    rows.Add(string.Join("|", Enumerable.Range(0, batch.ColumnCount).Select(c => ScalarArgCodec.ReadScalar(batch.Column(c), r) switch
                    {
                        null => "NULL",
                        bool b => b ? "true" : "false",
                        var v => v.ToString(),
                    })));
                }
            }

            finished = output.Finished;
        }

        return rows;
    }

    [Fact]
    public void ByType_FindsTheUnnamedDefaultSecret_AndRendersNumbersAndMissingFields()
    {
        var secrets = Batch(Delivered("__default_vgi_example", "vgi_example", "", ("api_key", Str("unnamed-key")), ("port", new Int32Array.Builder().Append(1).Build())));
        Assert.Equal("true|__default_vgi_example|unnamed-key", Assert.Single(Run(new SecretValueByTypeFunction(), secrets, "api_key")));
        Assert.Equal("true|__default_vgi_example|1", Assert.Single(Run(new SecretValueByTypeFunction(), secrets, "port")));
        Assert.Equal("true|__default_vgi_example|NULL", Assert.Single(Run(new SecretValueByTypeFunction(), secrets, "no_such_field")));
        Assert.Equal("false|NULL|NULL", Assert.Single(Run(new SecretValueByTypeFunction(), null, "api_key")));
    }

    [Fact]
    public void ByName_RejectsAnotherType()
    {
        var secrets = Batch(Delivered("conformance_s3", "s3", "", ("key_id", Str("AKIA"))));
        Assert.Equal("false|NULL|NULL", Assert.Single(Run(new SecretValueByNameFunction(), secrets, "conformance_s3", "key_id")));
    }

    [Fact]
    public void ForScope_LongestPrefix()
    {
        var secrets = Batch(
            Delivered("conformance_a", "vgi_example", "vgi-conformance://a/", ("api_key", Str("key-a"))),
            Delivered("conformance_ab", "vgi_example", "vgi-conformance://a/b/", ("api_key", Str("key-ab"))));
        Assert.Equal("true|conformance_ab|key-ab", Assert.Single(Run(new SecretValueForScopeFunction(), secrets, "vgi-conformance://a/b/f", "api_key")));
        Assert.Equal("false|NULL|NULL", Assert.Single(Run(new SecretValueForScopeFunction(), secrets, "vgi-conformance://z/f", "api_key")));
    }

    [Fact]
    public void TypesSeen_OneRowPerDeliveredSecret()
    {
        Assert.Empty(Run(new SecretTypesSeenFunction(), null));
        var secrets = Batch(
            Delivered("conformance_s3", "s3", "s3://vgi-conformance/"),
            Delivered("__default_vgi_example", "vgi_example", ""));
        Assert.Equal(
            ["__default_vgi_example|vgi_example|", "conformance_s3|s3|s3://vgi-conformance/"],
            Run(new SecretTypesSeenFunction(), secrets));
    }
}
