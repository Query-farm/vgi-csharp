using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.ExampleWorker.Aggregate;
using QueryFarm.Vgi.ExampleWorker.Scalar;
using QueryFarm.Vgi.ExampleWorker.Table;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Client;
using QueryFarm.VgiRpc.Transport;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary><c>catalog_contents</c> (protocol 2.1.0): the whole catalog in one RPC, each item
/// byte-for-byte what the per-schema RPC returns, and the attach flag that advertises it.</summary>
public class CatalogContentsTests
{
    private static readonly Schema OneColumn = new([new Field("x", Int64Type.Default, nullable: true)], metadata: null);

    /// <summary>One of every kind this port serves, spread over a parent schema, a child schema
    /// holding the only table (the registry lists table schemas before view/macro/declared ones,
    /// so <c>catalog_schemas</c> lists the child first and the composer has to reorder), and a
    /// schema with nothing but functions.</summary>
    private static CatalogRegistry PopulatedRegistry()
    {
        var registry = new CatalogRegistry();
        registry.RegisterView(new CatalogView
        {
            Name = "nested_view",
            SchemaPath = ["data", "nested"],
            Definition = "SELECT 1 AS one",
        });
        registry.RegisterScalar(new AddValuesFunction());
        registry.RegisterTable(new SequenceFunction());
        registry.RegisterAggregate(new SumFunction("contents_sum"));
        registry.RegisterSchema("data", "Data schema", new Dictionary<string, string> { ["k"] = "v" });
        registry.RegisterCatalogTable(new CatalogTable { Name = "plain", SchemaPath = ["data", "nested"], Columns = OneColumn });
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

    private static async Task<byte[]> AttachAsync(IVgiService service, string name = "example") =>
        (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = name })).AttachOpaqueData;

    private static string Key(List<string> path) => string.Join(".", path);

    /// <summary>The response's schemas, after checking each inline row's <c>path</c> matches the
    /// <see cref="SchemaInfo.Path"/> inside it (what the client verifies).</summary>
    private static List<SchemaContents> Decode(CatalogContentsResponse response)
    {
        Assert.False(response.NotModified);
        foreach (var entry in response.Schemas)
        {
            Assert.Equal(EmbeddedIpc.Decode<SchemaInfo>(entry.Schema).Path, entry.Path);
        }

        return response.Schemas;
    }

    /// <summary>A snapshot's identity for comparisons: the content-hash digest covers every path and
    /// item byte.</summary>
    private static string Fingerprint(CatalogContentsResponse response) => CatalogContentsDigest.Compute(response.Schemas);

    [Fact]
    public async Task OneEntryPerSchema_ParentsBeforeChildren()
    {
        IVgiService service = new VgiServiceImpl(PopulatedRegistry());
        var attach = await AttachAsync(service);

        var listed = (await service.CatalogSchemasAsync(attach, null)).Items
            .Select(item => EmbeddedIpc.Decode<SchemaInfo>(item).Path).ToList();
        var contents = Decode(await service.CatalogContentsAsync(attach));
        var paths = contents.Select(c => EmbeddedIpc.Decode<SchemaInfo>(c.Schema).Path).ToList();

        // Same set as catalog_schemas, one entry each...
        Assert.Equal(listed.Count, paths.Count);
        Assert.Equal(
            listed.Select(Key).Order(StringComparer.Ordinal),
            paths.Select(Key).Order(StringComparer.Ordinal));
        // ...the fixture really does list the child first, and the answer puts it after its parent.
        Assert.True(
            listed.FindIndex(p => p.SequenceEqual(["data", "nested"])) < listed.FindIndex(p => p.SequenceEqual(["data"])),
            "fixture should make catalog_schemas list data.nested before data");
        Assert.True(
            paths.FindIndex(p => p.SequenceEqual(["data"])) < paths.FindIndex(p => p.SequenceEqual(["data", "nested"])),
            "catalog_contents must list a parent schema before its child");
        Assert.Equal(paths.Select(p => p.Count).Order(), paths.Select(p => p.Count));
    }

    [Fact]
    public async Task EveryItem_IsByteIdenticalToThePerSchemaRpc_ForEveryKind()
    {
        IVgiService service = new VgiServiceImpl(PopulatedRegistry());
        var attach = await AttachAsync(service);

        var response = await service.CatalogContentsAsync(attach);
        Assert.Equal((await service.CatalogVersionAsync(attach, null)).Version, response.CatalogVersion);

        var schemaItems = (await service.CatalogSchemasAsync(attach, null)).Items;
        var contents = Decode(response);
        var kindsSeen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in contents)
        {
            Assert.Contains(schemaItems, item => item.SequenceEqual(entry.Schema));
            var path = EmbeddedIpc.Decode<SchemaInfo>(entry.Schema).Path;

            void Same(string kind, List<byte[]> expected, List<byte[]> actual)
            {
                Assert.Equal(expected.Count, actual.Count);
                for (var i = 0; i < expected.Count; i++)
                {
                    Assert.True(expected[i].SequenceEqual(actual[i]), $"{Key(path)} {kind}[{i}] differs");
                }

                if (actual.Count > 0)
                {
                    kindsSeen.Add(kind);
                }
            }

            Same("tables", (await service.CatalogSchemaContentsTablesAsync(attach, path, null)).Items, entry.Tables);
            Same("views", (await service.CatalogSchemaContentsViewsAsync(attach, path, null)).Items, entry.Views);
            Same("scalar_functions",
                (await service.CatalogSchemaContentsFunctionsAsync(attach, path, SchemaObjectType.ScalarFunction, null)).Items,
                entry.ScalarFunctions);
            Same("aggregate_functions",
                (await service.CatalogSchemaContentsFunctionsAsync(attach, path, SchemaObjectType.AggregateFunction, null)).Items,
                entry.AggregateFunctions);
            Same("table_functions",
                (await service.CatalogSchemaContentsFunctionsAsync(attach, path, SchemaObjectType.TableFunction, null)).Items,
                entry.TableFunctions);
            Same("scalar_macros",
                (await service.CatalogSchemaContentsMacrosAsync(attach, path, SchemaObjectType.ScalarMacro, null)).Items,
                entry.ScalarMacros);
            Same("table_macros",
                (await service.CatalogSchemaContentsMacrosAsync(attach, path, SchemaObjectType.TableMacro, null)).Items,
                entry.TableMacros);
            Assert.Empty(entry.Indexes);
        }

        // The comparison is only as good as its coverage: every kind must actually have items.
        Assert.Equal(
            ["aggregate_functions", "scalar_functions", "scalar_macros", "table_functions", "table_macros", "tables", "views"],
            kindsSeen.Order(StringComparer.Ordinal));
    }

    /// <summary>An item's bytes depend only on its contents: map fields (tags, column comments,
    /// estimated_object_count) are written in key order, whatever order the caller built them in.
    /// Otherwise one object could encode two ways and the "byte-identical" guarantee — and any
    /// content-hash cache built on it — would not hold.</summary>
    [Fact]
    public async Task MapFields_EncodeInKeyOrder_RegardlessOfInsertionOrder()
    {
        static CatalogRegistry With(params (string Key, string Value)[] tags)
        {
            var dict = new Dictionary<string, string>();
            foreach (var (key, value) in tags)
            {
                dict[key] = value;
            }

            var registry = new CatalogRegistry();
            registry.RegisterSchema("data", "Data", new Dictionary<string, string>(dict));
            registry.RegisterView(new CatalogView
            {
                Name = "v",
                SchemaName = "data",
                Definition = "SELECT 1 AS one",
                Tags = new Dictionary<string, string>(dict),
                ColumnComments = new Dictionary<string, string>(dict),
            });
            registry.RegisterMacro(new CatalogMacro
            {
                Name = "m",
                SchemaName = "data",
                MacroType = MacroType.Scalar,
                Definition = "1",
                Tags = new Dictionary<string, string>(dict),
            });
            registry.RegisterCatalogTable(new CatalogTable
            {
                Name = "t",
                SchemaName = "data",
                Columns = OneColumn,
                Tags = new Dictionary<string, string>(dict),
            });
            return registry;
        }

        IVgiService forward = new VgiServiceImpl(With(("a", "1"), ("b", "2"), ("c", "3")));
        IVgiService reverse = new VgiServiceImpl(With(("c", "3"), ("b", "2"), ("a", "1")));
        var forwardResponse = await forward.CatalogContentsAsync(await AttachAsync(forward));
        var reverseResponse = await reverse.CatalogContentsAsync(await AttachAsync(reverse));
        Assert.Equal(Fingerprint(forwardResponse), Fingerprint(reverseResponse));

        var data = Decode(reverseResponse)
            .Single(c => EmbeddedIpc.Decode<SchemaInfo>(c.Schema).Path.SequenceEqual(["data"]));
        Assert.Equal(["a", "b", "c"], EmbeddedIpc.Decode<SchemaInfo>(data.Schema).Tags.Keys);
        Assert.Equal(
            EmbeddedIpc.Decode<SchemaInfo>(data.Schema).EstimatedObjectCount!.Keys.Order(StringComparer.Ordinal),
            EmbeddedIpc.Decode<SchemaInfo>(data.Schema).EstimatedObjectCount!.Keys);
        Assert.Equal(["a", "b", "c"], EmbeddedIpc.Decode<ViewInfo>(Assert.Single(data.Views)).Tags.Keys);
        Assert.Equal(["a", "b", "c"], EmbeddedIpc.Decode<ViewInfo>(Assert.Single(data.Views)).ColumnComments.Keys);
        Assert.Equal(["a", "b", "c"], EmbeddedIpc.Decode<TableInfo>(Assert.Single(data.Tables)).Tags.Keys);
        Assert.Equal(["a", "b", "c"], EmbeddedIpc.Decode<MacroInfo>(Assert.Single(data.ScalarMacros)).Tags.Keys);
    }

    [Fact]
    public async Task KindsCountedAsZero_AreEmptyAndNotFetched()
    {
        var registry = new CatalogRegistry();
        registry.RegisterScalar(new AddValuesFunction());
        var counting = new CountingService(new VgiServiceImpl(registry));
        var attach = await AttachAsync(counting);

        var entry = Assert.Single(Decode(await ((IVgiService)counting).CatalogContentsAsync(attach)));

        Assert.Single(entry.ScalarFunctions);
        Assert.Empty(entry.Tables);
        Assert.Empty(entry.Views);
        Assert.Empty(entry.AggregateFunctions);
        Assert.Empty(entry.TableFunctions);
        Assert.Empty(entry.ScalarMacros);
        Assert.Empty(entry.TableMacros);
        Assert.Equal([SchemaObjectType.ScalarFunction], counting.FunctionKindsFetched);
        Assert.Equal(0, counting.OtherKindFetches);
    }

    [Fact]
    public async Task Attach_AdvertisesCatalogContents_ByDefault_AndNotWhenDisabled()
    {
        var on = new CatalogRegistry();
        Assert.True((await new VgiServiceImpl(on).CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).SupportsCatalogContents);
        Assert.True((await new VgiServiceImpl(on).CatalogAttachAsync(new CatalogAttachRequest { Name = "other" })).SupportsCatalogContents);

        var off = new CatalogRegistry { SupportsCatalogContents = false };
        Assert.False((await new VgiServiceImpl(off).CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).SupportsCatalogContents);
    }

    [Fact]
    public async Task Attach_PerAttachOverride_WinsOverTheWorkerSetting()
    {
        var registry = new CatalogRegistry
        {
            OnAttach = request => request.Name == "legacy" ? new AttachContext { SupportsCatalogContents = false } : null,
        };
        var service = new VgiServiceImpl(registry);

        Assert.False((await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "legacy" })).SupportsCatalogContents);
        Assert.True((await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "probe" })).SupportsCatalogContents);
    }

    [Fact]
    public async Task OnCatalogContents_SeesTheAttachIdentity_AndCanRefuseTheCall()
    {
        var registry = PopulatedRegistry();
        var seen = new List<string>();
        registry.OnCatalogContents = identity =>
        {
            seen.Add(identity);
            if (identity == "broken")
            {
                throw new InvalidOperationException("deliberately fails");
            }
        };
        IVgiService service = new VgiServiceImpl(registry);

        Assert.NotEmpty((await service.CatalogContentsAsync(await AttachAsync(service, "probe"))).Schemas);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await service.CatalogContentsAsync(await AttachAsync(service, "broken")));
        Assert.Equal("deliberately fails", error.Message);
        Assert.Equal(["probe", "broken"], seen);
    }

    [Fact]
    public async Task RoundTripsThroughARealClient_OverTheWorkersSocket()
    {
        var worker = new Worker()
            .CatalogName("example")
            .DefaultSchema("main")
            .RegisterScalar(new AddValuesFunction())
            .RegisterTable(new SequenceFunction())
            .RegisterSchema("data", "Data schema")
            .RegisterCatalogTable(new CatalogTable { Name = "plain", SchemaName = "data", Columns = OneColumn })
            .CatalogContentsEtag(CatalogContentsEtagMode.ContentHash);
        var path = Path.Combine(Path.GetTempPath(), $"vgi-csharp-test-{Guid.NewGuid():N}.sock");
        using var cts = new CancellationTokenSource();
        var serveTask = worker.RunUnixSocketAsync(path, idleTimeoutSeconds: 30, cts.Token);
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (!File.Exists(path) && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }

            using var transport = (SocketTransport)await SocketTransport.ConnectUnixAsync(path);
            var client = new RpcConnection<IVgiService>(
                transport, new RpcClientOptions { ProtocolVersion = Worker.DefaultProtocolVersion }).CreateProxy();

            var attached = await client.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
            Assert.True(attached.SupportsCatalogContents);

            var response = await client.CatalogContentsAsync(attached.AttachOpaqueData);
            var contents = Decode(response);
            var byPath = contents.ToDictionary(
                c => Key(EmbeddedIpc.Decode<SchemaInfo>(c.Schema).Path), StringComparer.Ordinal);
            Assert.Equal(["data", "main"], byPath.Keys.Order(StringComparer.Ordinal));

            var main = byPath["main"];
            Assert.Equal(
                (await client.CatalogSchemaContentsFunctionsAsync(attached.AttachOpaqueData, ["main"], SchemaObjectType.ScalarFunction, null)).Items,
                main.ScalarFunctions);
            Assert.Equal("add_values", EmbeddedIpc.Decode<FunctionInfo>(Assert.Single(main.ScalarFunctions)).Name);
            Assert.Equal("plain", EmbeddedIpc.Decode<TableInfo>(Assert.Single(byPath["data"].Tables)).Name);

            // Revalidation over the wire: if_none_match travels as a nullable utf8 column, and
            // etag / not_modified / an empty schemas list come back.
            Assert.Equal(CatalogContentsDigest.Compute(response.Schemas), response.Etag);
            var unchanged = await client.CatalogContentsAsync(attached.AttachOpaqueData, response.Etag);
            Assert.True(unchanged.NotModified);
            Assert.Empty(unchanged.Schemas);
            Assert.Equal(response.Etag, unchanged.Etag);
            Assert.Equal(response.CatalogVersion, unchanged.CatalogVersion);
            var stale = await client.CatalogContentsAsync(attached.AttachOpaqueData, "stale");
            Assert.False(stale.NotModified);
            Assert.Equal(Fingerprint(response), Fingerprint(stale));
        }
        finally
        {
            cts.Cancel();
            await serveTask;
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Revalidation (etag / if_none_match / not_modified) and the content-hash etag.
    // ---------------------------------------------------------------------------------------------

    /// <summary>The content-hash etag is byte-for-byte vgi-python's <c>catalog_contents_digest()</c>:
    /// these digests were computed by vgi-python (cc83818) over the same snapshots.</summary>
    [Fact]
    public void Digest_MatchesVgiPython()
    {
        List<SchemaContents> snapshot =
        [
            new() { Path = ["main"], Schema = [1, 2], Tables = ["t1"u8.ToArray()], ScalarFunctions = ["f"u8.ToArray(), []], TableMacros = ["m"u8.ToArray()] },
            new() { Path = ["main", "ü"], Schema = "s"u8.ToArray(), Indexes = ["i"u8.ToArray()] },
        ];

        Assert.Equal("7d5a7cc20cd648bf3218c0d63d9bfaa7acfbc93e41f199bceab06cc6c98d1b2b", CatalogContentsDigest.Compute(snapshot));
        Assert.Equal("af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc", CatalogContentsDigest.Compute([]));
    }

    [Fact]
    public async Task ByDefault_NoEtag_AndIfNoneMatchIsIgnored()
    {
        IVgiService service = new VgiServiceImpl(PopulatedRegistry());
        var attach = await AttachAsync(service);

        var full = await service.CatalogContentsAsync(attach);
        Assert.Null(full.Etag);
        Assert.False(full.NotModified);

        var conditional = await service.CatalogContentsAsync(attach, ifNoneMatch: "anything");
        Assert.Null(conditional.Etag);
        Assert.False(conditional.NotModified);
        Assert.Equal(Fingerprint(full), Fingerprint(conditional));
    }

    [Fact]
    public async Task ContentHash_EtagIsTheDigest_AndAMatchIsNotModified()
    {
        var registry = PopulatedRegistry();
        registry.CatalogContentsEtag = CatalogContentsEtagMode.ContentHash;
        IVgiService service = new VgiServiceImpl(registry);
        var attach = await AttachAsync(service);

        var full = await service.CatalogContentsAsync(attach);
        Assert.False(full.NotModified);
        Assert.NotEmpty(full.Schemas);
        Assert.Equal(CatalogContentsDigest.Compute(full.Schemas), full.Etag);

        var unchanged = await service.CatalogContentsAsync(attach, ifNoneMatch: full.Etag);
        Assert.True(unchanged.NotModified);
        Assert.Empty(unchanged.Schemas);
        Assert.Equal(full.Etag, unchanged.Etag);
        Assert.Equal(full.CatalogVersion, unchanged.CatalogVersion);

        var stale = await service.CatalogContentsAsync(attach, ifNoneMatch: "not-the-etag");
        Assert.False(stale.NotModified);
        Assert.Equal(full.Etag, stale.Etag);
        Assert.Equal(Fingerprint(full), Fingerprint(stale));
    }

    /// <summary>The same catalog built twice (separate registries and services, so nothing is
    /// shared or cached between them) hashes alike.</summary>
    [Fact]
    public async Task ContentHash_IsDeterministicAcrossBuilds()
    {
        async Task<string?> EtagOf()
        {
            var registry = PopulatedRegistry();
            registry.CatalogContentsEtag = CatalogContentsEtagMode.ContentHash;
            IVgiService service = new VgiServiceImpl(registry);
            return (await service.CatalogContentsAsync(await AttachAsync(service))).Etag;
        }

        var first = await EtagOf();
        Assert.NotNull(first);
        Assert.Equal(first, await EtagOf());
    }

    [Fact]
    public async Task ContentHash_IsPerIdentity()
    {
        var registry = PopulatedRegistry();
        registry.SetCatalogContentsEtag("hashed", CatalogContentsEtagMode.ContentHash);
        IVgiService service = new VgiServiceImpl(registry);

        Assert.NotNull((await service.CatalogContentsAsync(await AttachAsync(service, "hashed"))).Etag);
        Assert.Null((await service.CatalogContentsAsync(await AttachAsync(service, "plain"))).Etag);
    }

    /// <summary>A cheap validator answers not-modified before anything is built; a miss builds and
    /// carries the etag.</summary>
    [Fact]
    public async Task Handler_CheapValidator_ShortCircuitsWithoutBuilding()
    {
        var registry = PopulatedRegistry();
        var builds = 0;
        registry.SetCatalogContentsHandler("example", async request =>
        {
            const string etag = "gen-7";
            if (request.IfNoneMatch == etag)
            {
                return CatalogContentsResult.Unchanged(etag);
            }

            builds++;
            return await request.BuildAsync() with { Etag = etag };
        });
        IVgiService service = new VgiServiceImpl(registry);
        var attach = await AttachAsync(service);

        var full = await service.CatalogContentsAsync(attach);
        Assert.Equal("gen-7", full.Etag);
        Assert.False(full.NotModified);
        Assert.NotEmpty(Decode(full));
        Assert.Equal(1, builds);

        var unchanged = await service.CatalogContentsAsync(attach, ifNoneMatch: "gen-7");
        Assert.True(unchanged.NotModified);
        Assert.Empty(unchanged.Schemas);
        Assert.Equal("gen-7", unchanged.Etag);
        Assert.Equal(1, builds);

        var stale = await service.CatalogContentsAsync(attach, ifNoneMatch: "gen-6");
        Assert.False(stale.NotModified);
        Assert.Equal(Fingerprint(full), Fingerprint(stale));
        Assert.Equal(2, builds);
    }

    [Fact]
    public async Task Handler_SeesTheRequest()
    {
        var registry = PopulatedRegistry();
        CatalogContentsRequest? seen = null;
        registry.SetCatalogContentsHandler("example", request =>
        {
            seen = request;
            return request.BuildAsync();
        });
        IVgiService service = new VgiServiceImpl(registry);
        var attach = await AttachAsync(service);

        var response = await service.CatalogContentsAsync(attach, ifNoneMatch: "x");
        Assert.NotNull(seen);
        Assert.Equal("example", seen.Identity);
        Assert.Equal("x", seen.IfNoneMatch);
        Assert.Equal(attach, seen.AttachOpaqueData);
        Assert.Equal(response.CatalogVersion, seen.CatalogVersion);
        Assert.Null(response.Etag);
        Assert.False(response.NotModified);
    }

    /// <summary>A full answer whose own etag equals <c>if_none_match</c> is sent not-modified.</summary>
    [Fact]
    public async Task Handler_FullAnswerWithMatchingEtag_BecomesNotModified()
    {
        var registry = PopulatedRegistry();
        registry.SetCatalogContentsHandler("example", async request => await request.BuildAsync() with { Etag = "v1" });
        IVgiService service = new VgiServiceImpl(registry);

        var response = await service.CatalogContentsAsync(await AttachAsync(service), ifNoneMatch: "v1");
        Assert.True(response.NotModified);
        Assert.Empty(response.Schemas);
        Assert.Equal("v1", response.Etag);
    }

    public static TheoryData<string, string?, string?, bool> BadNotModified => new()
    {
        // case, result etag, if_none_match, with schemas
        { "no etag", null, "v1", false },
        { "no if_none_match", "v1", null, false },
        { "etag differs from if_none_match", "v2", "v1", false },
        { "schemas with not_modified", "v1", "v1", true },
    };

    [Theory]
    [MemberData(nameof(BadNotModified))]
    public async Task Handler_InvalidNotModified_FailsTheCall(string scenario, string? etag, string? ifNoneMatch, bool withSchemas)
    {
        var registry = PopulatedRegistry();
        registry.SetCatalogContentsHandler("example", async request => new CatalogContentsResult
        {
            Etag = etag,
            NotModified = true,
            Schemas = withSchemas ? (await request.BuildAsync()).Schemas : [],
        });
        IVgiService service = new VgiServiceImpl(registry);
        var attach = await AttachAsync(service);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await service.CatalogContentsAsync(attach, ifNoneMatch));
        Assert.Contains("not_modified", error.Message, StringComparison.Ordinal);
        _ = scenario;
    }

    private static SchemaContents Entry(params string[] path) => new()
    {
        Path = [.. path],
        Schema = EmbeddedIpc.Encode(new SchemaInfo { AttachOpaqueData = [], Path = [.. path], Comment = null, Tags = [] }),
    };

    [Fact]
    public void Respond_ChecksPaths_AndOrdersParentsFirst()
    {
        var ordered = CatalogContentsResponder.Respond(
            3, new CatalogContentsResult { Schemas = [Entry("a", "b"), Entry("z"), Entry("a")] }, null, CatalogContentsEtagMode.None);
        Assert.Equal(3, ordered.CatalogVersion);
        Assert.Equal([["z"], ["a"], ["a", "b"]], ordered.Schemas.Select(s => s.Path));

        Assert.Contains("duplicate", Assert.Throws<InvalidOperationException>(() => CatalogContentsResponder.Respond(
            1, new CatalogContentsResult { Schemas = [Entry("a"), Entry("a")] }, null, CatalogContentsEtagMode.None)).Message);
        Assert.Contains("without its parent", Assert.Throws<InvalidOperationException>(() => CatalogContentsResponder.Respond(
            1, new CatalogContentsResult { Schemas = [Entry("a", "b")] }, null, CatalogContentsEtagMode.None)).Message);
        var mismatched = Entry("a");
        mismatched.Path = ["b"];
        Assert.Contains("SchemaInfo.path", Assert.Throws<InvalidOperationException>(() => CatalogContentsResponder.Respond(
            1, new CatalogContentsResult { Schemas = [mismatched] }, null, CatalogContentsEtagMode.None)).Message);
    }

    /// <summary>The default snapshot is built once per identity and reused — until something is
    /// registered, which rebuilds it (and moves the content-hash etag).</summary>
    [Fact]
    public async Task Snapshot_IsCached_UntilTheRegistryChanges()
    {
        var registry = PopulatedRegistry();
        registry.CatalogContentsEtag = CatalogContentsEtagMode.ContentHash;
        IVgiService service = new VgiServiceImpl(registry);
        var attach = await AttachAsync(service);

        var first = await service.CatalogContentsAsync(attach);
        var second = await service.CatalogContentsAsync(attach);
        Assert.Same(first.Schemas[0], second.Schemas[0]);
        Assert.Equal(first.Etag, second.Etag);

        registry.RegisterView(new CatalogView { Name = "late_view", SchemaName = "data", Definition = "SELECT 3 AS three" });
        var third = await service.CatalogContentsAsync(attach, ifNoneMatch: first.Etag);
        Assert.False(third.NotModified);
        Assert.NotEqual(first.Etag, third.Etag);
        Assert.Contains(
            third.Schemas.Single(s => s.Path.SequenceEqual(["data"])).Views,
            item => EmbeddedIpc.Decode<ViewInfo>(item).Name == "late_view");
    }

    /// <summary>A decorating service's <see cref="CatalogContentsComposer.ServeAsync"/> composition has
    /// no etag and ignores <c>if_none_match</c>.</summary>
    [Fact]
    public async Task ComposedContents_HasNoEtag()
    {
        var registry = PopulatedRegistry();
        registry.CatalogContentsEtag = CatalogContentsEtagMode.ContentHash; // the decorator does not use it
        IVgiService counting = new CountingService(new VgiServiceImpl(registry));
        var attach = await AttachAsync(counting);

        var response = await counting.CatalogContentsAsync(attach, ifNoneMatch: "x");
        Assert.Null(response.Etag);
        Assert.False(response.NotModified);
        Assert.NotEmpty(Decode(response));
    }

    /// <summary>Forwards to a real service and records which per-kind RPCs the
    /// <see cref="CatalogContentsComposer"/> composition issues through it.</summary>
    private sealed class CountingService(IVgiService real) : IVgiService
    {
        /// <summary>The composition under test, issued through this decorator's own RPCs.</summary>
        public Task<CatalogContentsResponse> CatalogContentsAsync(
            byte[] attachOpaqueData, string? ifNoneMatch = null, VgiRpc.Server.ICallContext? ctx = null) =>
            CatalogContentsComposer.ServeAsync(this, attachOpaqueData, ifNoneMatch, ctx);

        public Task<CatalogVersionResponse> CatalogVersionAsync(
            byte[] attachOpaqueData, byte[]? transactionOpaqueData = null, VgiRpc.Server.ICallContext? ctx = null) =>
            real.CatalogVersionAsync(attachOpaqueData, transactionOpaqueData, ctx);

        public List<SchemaObjectType> FunctionKindsFetched { get; } = [];

        public int OtherKindFetches { get; private set; }

        public Task<BindResponse> BindAsync(BindRequest request, VgiRpc.Server.ICallContext? ctx = null) => real.BindAsync(request, ctx);

        public Task<VgiRpc.Streaming.RpcStream<VgiRpc.Streaming.StreamState>> InitAsync(InitRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.InitAsync(request, ctx);

        public Task<TableFunctionPlanResult> TableFunctionPlanAsync(TableFunctionPlanRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.TableFunctionPlanAsync(request, ctx);

        public Task<TableFunctionCardinalityResult> TableFunctionCardinalityAsync(TableFunctionCardinalityRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.TableFunctionCardinalityAsync(request, ctx);

        public Task<AggregateUpdateResult> AggregateUpdateAsync(AggregateUpdateRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.AggregateUpdateAsync(request, ctx);

        public Task<AggregateCombineResult> AggregateCombineAsync(AggregateCombineRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.AggregateCombineAsync(request, ctx);

        public Task<AggregateFinalizeResult> AggregateFinalizeAsync(AggregateFinalizeRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.AggregateFinalizeAsync(request, ctx);

        public Task<AggregateDestructorResult> AggregateDestructorAsync(AggregateDestructorRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.AggregateDestructorAsync(request, ctx);

        public Task<AggregateBindResult> AggregateBindAsync(AggregateBindRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.AggregateBindAsync(request, ctx);

        public Task<TableBufferingProcessResult> TableBufferingProcessAsync(TableBufferingProcessRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.TableBufferingProcessAsync(request, ctx);

        public Task<TableBufferingCombineResult> TableBufferingCombineAsync(TableBufferingCombineRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.TableBufferingCombineAsync(request, ctx);

        public Task<TableBufferingDestructorResult> TableBufferingDestructorAsync(TableBufferingDestructorRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.TableBufferingDestructorAsync(request, ctx);

        public Task<CatalogAttachResult> CatalogAttachAsync(CatalogAttachRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.CatalogAttachAsync(request, ctx);

        public Task CatalogDetachAsync(byte[] attachOpaqueData, VgiRpc.Server.ICallContext? ctx = null) =>
            real.CatalogDetachAsync(attachOpaqueData, ctx);

        public Task<ItemsResponse> CatalogSchemasAsync(byte[] attachOpaqueData, byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null) =>
            real.CatalogSchemasAsync(attachOpaqueData, transactionOpaqueData, ctx);

        public Task<ItemsResponse> CatalogSchemaContentsFunctionsAsync(
            byte[] attachOpaqueData, List<string> path, SchemaObjectType type, byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null)
        {
            FunctionKindsFetched.Add(type);
            return real.CatalogSchemaContentsFunctionsAsync(attachOpaqueData, path, type, transactionOpaqueData, ctx);
        }

        public Task<ItemsResponse> CatalogSchemaContentsTablesAsync(
            byte[] attachOpaqueData, List<string> path, byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null)
        {
            OtherKindFetches++;
            return real.CatalogSchemaContentsTablesAsync(attachOpaqueData, path, transactionOpaqueData, ctx);
        }

        public Task<ItemsResponse> CatalogSchemaContentsViewsAsync(
            byte[] attachOpaqueData, List<string> path, byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null)
        {
            OtherKindFetches++;
            return real.CatalogSchemaContentsViewsAsync(attachOpaqueData, path, transactionOpaqueData, ctx);
        }

        public Task<ItemsResponse> CatalogSchemaContentsMacrosAsync(
            byte[] attachOpaqueData, List<string> path, SchemaObjectType type, byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null)
        {
            OtherKindFetches++;
            return real.CatalogSchemaContentsMacrosAsync(attachOpaqueData, path, type, transactionOpaqueData, ctx);
        }

        public Task<ScanBranchesResult> CatalogTableScanBranchesGetAsync(
            byte[] attachOpaqueData, List<string> schemaPath, string name, string? atUnit, string? atValue,
            byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null) =>
            real.CatalogTableScanBranchesGetAsync(attachOpaqueData, schemaPath, name, atUnit, atValue, transactionOpaqueData, ctx);
    }
}
