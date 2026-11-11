using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;
using QueryFarm.VgiRpc.Wire;
using Xunit;

namespace QueryFarm.Vgi.Tests.Catalog;

/// <summary>The public API a writable, transactional catalog is built with: per-attach
/// <see cref="AttachContext.SupportsTransactions"/>, <see cref="TransactionHooks"/> (plaintext ids on
/// the sealed HTTP path too), <c>TransactionOpaqueData</c> on table in-out params, the
/// enforce-profile negotiation for writable constrained tables, row-id validation,
/// <see cref="WriteResults"/>/<see cref="WriteRequest"/>, <see cref="AttachOptions"/>,
/// <see cref="AttachData"/> and the asynchronous producer/processor interfaces.</summary>
public sealed class WritableTxApiTests
{
    private static readonly Schema s_input = new([new Field("x", Int64Type.Default, nullable: true)], metadata: null);

    private sealed class RecordingHooks : TransactionHooks
    {
        public byte[]? MintedId { get; init; }

        public string? FailCommitWith { get; init; }

        public List<(string Kind, byte[] Attach, byte[] Tx)> Calls { get; } = [];

        public override Task<byte[]?> BeginAsync(TransactionContext context)
        {
            Calls.Add(("begin", context.AttachOpaqueData, context.TransactionOpaqueData));
            return Task.FromResult(MintedId);
        }

        public override Task CommitAsync(TransactionContext context)
        {
            Calls.Add(("commit", context.AttachOpaqueData, context.TransactionOpaqueData));
            return FailCommitWith is null ? Task.CompletedTask : throw new InvalidOperationException(FailCommitWith);
        }

        public override Task RollbackAsync(TransactionContext context)
        {
            Calls.Add(("rollback", context.AttachOpaqueData, context.TransactionOpaqueData));
            return Task.CompletedTask;
        }
    }

    // ------------------------------------------------------------------ supports_transactions

    [Fact]
    public async Task SupportsTransactionsIsChosenPerAttach()
    {
        var registry = new CatalogRegistry
        {
            OnAttach = request => new AttachContext
            {
                SupportsTransactions = AttachOptions.Read(request).GetBoolean("transactional") ?? false,
            },
        };
        var service = new VgiServiceImpl(registry);

        var plain = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c", Options = Options(("transactional", false)) });
        var tx = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c", Options = Options(("transactional", true)) });
        var example = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });

        Assert.False(plain.SupportsTransactions);
        Assert.True(tx.SupportsTransactions);
        // An explicit false wins over the legacy "example" default.
        Assert.False(example.SupportsTransactions);
    }

    [Fact]
    public async Task SupportsTransactionsNullKeepsTheLegacyDefault()
    {
        var registry = new CatalogRegistry { OnAttach = _ => new AttachContext() };
        var service = new VgiServiceImpl(registry);
        Assert.True((await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).SupportsTransactions);
        Assert.False((await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "other" })).SupportsTransactions);
    }

    // ------------------------------------------------------------------ transaction hooks

    [Fact]
    public async Task HooksMintTheIdAndReceiveItBackOnCommitAndRollback()
    {
        byte[] minted = [9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6];
        var hooks = new RecordingHooks { MintedId = minted };
        var service = new VgiServiceImpl(new CatalogRegistry { TransactionHooks = hooks, OnAttach = _ => new AttachContext { ExtraOpaqueData = [42] } });
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c" })).AttachOpaqueData;

        var begun = await service.CatalogTransactionBeginAsync(attach);
        Assert.Equal(minted, begun.TransactionOpaqueData);
        await service.CatalogTransactionCommitAsync(attach, minted);
        await service.CatalogTransactionRollbackAsync(attach, minted);

        Assert.Equal(["begin", "commit", "rollback"], hooks.Calls.Select(c => c.Kind));
        Assert.All(hooks.Calls, call => Assert.Equal(attach, call.Attach));
        Assert.Empty(hooks.Calls[0].Tx);
        Assert.Equal(minted, hooks.Calls[1].Tx);
        Assert.Equal(minted, hooks.Calls[2].Tx);
        Assert.Equal([42], AttachData.Parse(hooks.Calls[1].Attach).Extra);
    }

    [Fact]
    public async Task ABeginHookReturningNullGetsAnSdkMintedId()
    {
        var service = new VgiServiceImpl(new CatalogRegistry { TransactionHooks = new RecordingHooks() });
        var first = (await service.CatalogTransactionBeginAsync([])).TransactionOpaqueData!;
        var second = (await service.CatalogTransactionBeginAsync([])).TransactionOpaqueData!;
        Assert.Equal(16, first.Length);
        Assert.NotEqual(first, second);
    }

    [Fact]
    public async Task AFailingCommitHookFailsTheCommitAndStillClearsTransactionStorage()
    {
        var service = new VgiServiceImpl(new CatalogRegistry { TransactionHooks = new RecordingHooks { FailCommitWith = "commit refused: qty -5 for id 2" } });
        var tx = (await service.CatalogTransactionBeginAsync([])).TransactionOpaqueData!;
        new FunctionStorage(tx).WriteSingle("ns", "key", [1]);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => service.CatalogTransactionCommitAsync([], tx));

        Assert.Equal("commit refused: qty -5 for id 2", error.Message);
        Assert.Null(new FunctionStorage(tx).ReadSingle("ns", "key"));
    }

    /// <summary>Over HTTP the SDK seals both values on the wire; the hooks and the functions must
    /// still see the same plaintext id (the one the begin hook returned).</summary>
    [Fact]
    public async Task OnTheSealedPathHooksAndFunctionsSeeThePlaintextId()
    {
        byte[] minted = [.. Enumerable.Range(100, 16).Select(i => (byte)i)];
        var hooks = new RecordingHooks { MintedId = minted };
        var registry = new CatalogRegistry { TransactionHooks = hooks, OnAttach = _ => new AttachContext { SupportsTransactions = true } };
        var inOut = new CapturingInOut();
        registry.RegisterTableInOut(inOut);
        IVgiService service = OpaqueSealingProxy.Wrap(new VgiServiceImpl(registry), new OpaqueSealer("unit-test-sealing-key-0123456789"u8.ToArray()));

        var attached = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c" });
        Assert.True(attached.SupportsTransactions);
        var sealedTx = (await service.CatalogTransactionBeginAsync(attached.AttachOpaqueData)).TransactionOpaqueData!;
        Assert.NotEqual(minted, sealedTx);

        // The proxy opens a request in place, so each call gets its own (sealed) request.
        await service.BindAsync(BindFor(inOut.Name, attached.AttachOpaqueData, sealedTx));
        await service.InitAsync(new InitRequest { BindCall = EmbeddedIpc.Encode(BindFor(inOut.Name, attached.AttachOpaqueData, sealedTx)), Phase = VgiInitPhase.Input, ExecutionId = Guid.NewGuid().ToByteArray() });
        await service.CatalogTransactionCommitAsync(attached.AttachOpaqueData, sealedTx);

        Assert.Equal(minted, inOut.BindTx);
        Assert.Equal(minted, inOut.InitTx);
        Assert.Equal(minted, hooks.Calls.Single(c => c.Kind == "commit").Tx);
        Assert.Equal(inOut.InitAttach, hooks.Calls.Single(c => c.Kind == "commit").Attach);
    }

    /// <summary>On the sealed (HTTP) path the whole attach envelope is sealed, the
    /// <see cref="AttachContext.ExtraOpaqueData"/> bytes included: the client never holds them in
    /// plaintext, and functions get them back opened.</summary>
    [Fact]
    public async Task OnTheSealedPathExtraOpaqueDataIsSealedAndReachesFunctionsOpened()
    {
        var secret = "hue-app-key-0123456789abcdef"u8.ToArray();
        var registry = new CatalogRegistry { OnAttach = _ => new AttachContext { ExtraOpaqueData = secret } };
        var inOut = new CapturingInOut();
        registry.RegisterTableInOut(inOut);
        IVgiService service = OpaqueSealingProxy.Wrap(new VgiServiceImpl(registry), new OpaqueSealer("unit-test-sealing-key-0123456789"u8.ToArray()));

        var sealedAttach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c" })).AttachOpaqueData;
        Assert.Equal(OpaqueSealer.AttachVersion, sealedAttach[0]);
        Assert.Equal(-1, sealedAttach.AsSpan().IndexOf(secret));

        await service.InitAsync(new InitRequest
        {
            BindCall = EmbeddedIpc.Encode(BindFor(inOut.Name, sealedAttach, [])),
            Phase = VgiInitPhase.Input,
            ExecutionId = Guid.NewGuid().ToByteArray(),
        });
        Assert.Equal(secret, AttachData.ExtraOf(inOut.InitAttach));
    }

    // ------------------------------------------------------------------ table in-out params

    [Fact]
    public async Task TableInOutBindAndInitSeeTheTransactionId()
    {
        var registry = new CatalogRegistry();
        var inOut = new CapturingInOut();
        registry.RegisterTableInOut(inOut);
        var service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c" })).AttachOpaqueData;
        byte[] tx = [1, 2, 3];

        var bind = BindFor(inOut.Name, attach, tx);
        var bound = await service.BindAsync(bind);
        await service.InitAsync(new InitRequest
        {
            BindCall = EmbeddedIpc.Encode(bind),
            Phase = VgiInitPhase.Input,
            ExecutionId = Guid.NewGuid().ToByteArray(),
            OutputSchema = bound.OutputSchema,
        });

        Assert.Equal(tx, inOut.BindTx);
        Assert.Equal(tx, inOut.InitTx);
        Assert.Equal(attach, inOut.InitAttach);
        // Init reuses the schema bind accepted rather than re-running ResolveOutputSchema.
        Assert.Equal(1, inOut.ResolveCalls);
    }

    // ------------------------------------------------------------------ enforce.v1

    private static CatalogTable ConstrainedTable(string name, bool writable) => new()
    {
        Name = name,
        Columns = new Schema(
            [new Field("id", Int32Type.Default, nullable: false), new Field("v", Int64Type.Default, nullable: true), WriteResults.RowIdField()],
            metadata: null),
        RowIdColumn = writable ? "rowid" : null,
        Constraints = ConstraintRecord.For(["main"], name).PrimaryKey("id").Build(),
        UpdateFunction = writable ? new CapturingInOut($"{name}_update") : null,
        WriteResultModes = writable ? new Dictionary<string, string> { [WriteOperation.Update] = WriteResultMode.Rows } : new Dictionary<string, string>(),
    };

    private static byte[] FullOffer() => EmbeddedIpc.Encode(new ClientCapabilities
    {
        Engine = "test",
        Catalog = ConstraintRecord.EncodeCapabilities(
            read: [ConstraintProfiles.RecordsRead, ConstraintProfiles.ScalarExpression],
            ddl: [ConstraintProfiles.Create],
            write: [ConstraintProfiles.Enforce]),
    });

    [Fact]
    public async Task AWritableConstrainedTableNegotiatesEnforce()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(ConstrainedTable("items", writable: true));
        registry.RegisterCatalogTable(ConstrainedTable("labels", writable: false));
        var attach = await new VgiServiceImpl(registry).CatalogAttachAsync(new CatalogAttachRequest { Name = "c", ClientCapabilities = FullOffer() });

        Assert.Contains(ConstraintProfiles.Enforce, ConstraintRecord.DecodeCapabilities(attach.CatalogCapabilities).Write);
        Assert.Equal([ConstraintProfiles.Enforce], ConstraintRecord.DecodeCapabilities(attach.SelectedCapabilities).Write);
        Assert.Empty(ConstraintRecord.DecodeCapabilities(attach.SelectedCapabilities).Ddl);
    }

    [Fact]
    public async Task OnlyReadOnlyConstrainedTablesDoNotAdvertiseEnforce()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(ConstrainedTable("labels", writable: false));
        var attach = await new VgiServiceImpl(registry).CatalogAttachAsync(new CatalogAttachRequest { Name = "c", ClientCapabilities = FullOffer() });

        Assert.Empty(ConstraintRecord.DecodeCapabilities(attach.CatalogCapabilities).Write);
        Assert.Empty(ConstraintRecord.DecodeCapabilities(attach.SelectedCapabilities).Write);
    }

    // ------------------------------------------------------------------ row id validation

    private sealed class Scan(bool? pushdown) : ITableFunction
    {
        public string Name => "scan";

        public Schema ArgumentsSchema { get; } = new([], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("id", Int64Type.Default, true), WriteResults.RowIdField()], metadata: null);

        public bool? ProjectionPushdown => pushdown;

        public ITableFunctionProducer CreateProducer(TableInitParams initParams) => throw new NotSupportedException();
    }

    [Fact]
    public void ARowIdColumnOverAScanWithoutProjectionPushdownIsRefused()
    {
        var error = Assert.Throws<ArgumentException>(() =>
            new CatalogRegistry().RegisterCatalogTable(new CatalogTable { Name = "t", RowIdColumn = "rowid", ScanFunction = new Scan(null) }));
        Assert.Contains("ProjectionPushdown", error.Message);
        Assert.Contains("Virtual columns require projection pushdown", error.Message);

        new CatalogRegistry().RegisterCatalogTable(new CatalogTable { Name = "t", RowIdColumn = "rowid", ScanFunction = new Scan(true) });
    }

    [Fact]
    public void ARowIdColumnThatIsNotAColumnIsRefused()
    {
        var error = Assert.Throws<ArgumentException>(() =>
            new CatalogRegistry().RegisterCatalogTable(new CatalogTable { Name = "t", RowIdColumn = "nope", ScanFunction = new Scan(true) }));
        Assert.Contains("'nope' is not a column", error.Message);
    }

    // ------------------------------------------------------------------ write results

    private static readonly Schema s_user = new(
        [new Field("id", Int32Type.Default, nullable: false), new Field("name", StringType.Default, nullable: true)], metadata: null);

    [Fact]
    public void WriteResultSchemasMatchEachMode()
    {
        Assert.Equal("count", Assert.Single(WriteResults.Schema(WriteResultMode.Count, s_user).FieldsList).Name);
        Assert.False(WriteResults.CountSchema.GetFieldByName("count").IsNullable);
        Assert.Same(s_user, WriteResults.Schema(WriteResultMode.Rows, s_user));
        var changes = WriteResults.Schema(WriteResultMode.Changes, s_user);
        Assert.Equal(["old", "new"], changes.FieldsList.Select(f => f.Name));
        Assert.Equal(["id", "name"], ((StructType)changes.GetFieldByName("new").DataType).Fields.Select(f => f.Name));
        Assert.Throws<ArgumentException>(() => WriteResults.Schema("bogus", s_user));
    }

    [Fact]
    public void WriteResultBatchesCarryTheRows()
    {
        IReadOnlyDictionary<string, object?> before = new Dictionary<string, object?> { ["id"] = 1, ["name"] = "a" };
        IReadOnlyDictionary<string, object?> after = new Dictionary<string, object?> { ["id"] = 1, ["name"] = "b" };

        var count = WriteResults.Batch(WriteResultMode.Count, s_user, [before], [after]);
        Assert.Equal(1L, ((Int64Array)count.Column(0)).GetValue(0));

        var rows = WriteResults.Batch(WriteResultMode.Rows, s_user, [before, after], [after, null]);
        Assert.Equal(["b", "b"], WriteResults.ReadRows(rows).Select(r => (string?)r["name"]));

        var changes = WriteResults.Batch(WriteResultMode.Changes, s_user, [null, before], [after, null]);
        var oldColumn = (StructArray)changes.Column("old");
        var newColumn = (StructArray)changes.Column("new");
        Assert.True(oldColumn.IsNull(0));
        Assert.False(oldColumn.IsNull(1));
        Assert.True(newColumn.IsNull(1));
        Assert.Equal("b", ((StringArray)newColumn.Fields[1]).GetString(0));
    }

    [Fact]
    public void BuildBatchAndReadRowsRoundTripNestedAndTemporalTypes()
    {
        var point = new StructType([new Field("x", DoubleType.Default, true), new Field("tags", new ListType(new Field("item", StringType.Default, true)), true)]);
        var schema = new Schema(
            [
                new Field("at", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
                new Field("day", Date32Type.Default, true),
                new Field("names", new ListType(new Field("item", StringType.Default, true)), true),
                new Field("point", point, true),
            ],
            metadata: null);
        var at = new DateTimeOffset(2026, 11, 8, 12, 30, 0, TimeSpan.Zero);
        IReadOnlyDictionary<string, object?> full = new Dictionary<string, object?>
        {
            ["at"] = at,
            ["day"] = new DateOnly(2026, 11, 8),
            ["names"] = new[] { "a", "b" },
            ["point"] = new Dictionary<string, object?> { ["x"] = 1.5, ["tags"] = new List<string> { "t" } },
        };
        IReadOnlyDictionary<string, object?> empty = new Dictionary<string, object?>();

        var rows = WriteResults.ReadRows(WriteResults.BuildBatch(schema, [full, empty]));

        Assert.Equal(at, rows[0]["at"]);
        Assert.Equal(new DateOnly(2026, 11, 8), rows[0]["day"]);
        Assert.Equal(["a", "b"], Assert.IsType<List<object?>>(rows[0]["names"]));
        var p = Assert.IsType<Dictionary<string, object?>>(rows[0]["point"]);
        Assert.Equal(1.5, p["x"]);
        Assert.Equal(["t"], Assert.IsType<List<object?>>(p["tags"]));
        Assert.All(rows[1].Values, Assert.Null);
    }

    [Fact]
    public void RowIdFieldAndUserColumns()
    {
        var rowId = WriteResults.RowIdField();
        Assert.Equal(("rowid", false), (rowId.Name, rowId.IsNullable));
        Assert.IsType<Int64Type>(rowId.DataType);
        Assert.True(WriteResults.IsRowId(rowId));
        var columns = new Schema([.. s_user.FieldsList, rowId], metadata: null);
        Assert.Equal(["id", "name"], WriteResults.UserColumns(columns).FieldsList.Select(f => f.Name));
        Assert.Equal(["name"], WriteResults.UserColumns(s_user, rowIdColumn: "id").FieldsList.Select(f => f.Name));
    }

    [Fact]
    public void WriteRequestDecodesWriteOptions()
    {
        var options = EmbeddedIpc.Encode(new WriteOptions { ResultMode = "rows" });
        Assert.Equal(["result_mode"], SchemaOf(options));
        var request = WriteRequest.From(WriteOptionsArgument(options));
        Assert.Equal(WriteResultMode.Rows, request.ResultMode);
#pragma warning disable CS0618 // the deprecated accessors keep their fixed values
        Assert.Equal(("throw", 0), (request.OnConflict, request.OnConflictColumns.Count));
#pragma warning restore CS0618

        // A pre-3.3 peer's batch still carries on_conflict / on_conflict_columns: ignored.
        var legacy = EmbeddedIpc.Encode(new LegacyWriteOptions { ResultMode = "changes", OnConflict = "nothing", OnConflictColumns = ["id"] });
        Assert.Equal(["result_mode", "on_conflict", "on_conflict_columns"], SchemaOf(legacy));
        Assert.Equal(WriteResultMode.Changes, WriteRequest.From(WriteOptionsArgument(legacy)).ResultMode);

        Assert.Equal(WriteResultMode.Count, WriteRequest.From(TableArgCodec.Decode([])).ResultMode);
        Assert.True(WriteResultMode.Supports(WriteResultMode.Changes, WriteResultMode.Rows));
        Assert.False(WriteResultMode.Supports(WriteResultMode.Count, WriteResultMode.Rows));
        Assert.Throws<ArgumentException>(() => WriteResultMode.Validate(new Dictionary<string, string> { ["upsert"] = "count" }));
    }

    private static QueryFarm.Vgi.Arguments.TableArguments WriteOptionsArgument(byte[] options) =>
        new([], new Dictionary<string, IArrowArray>
        {
            ["write_options"] = new BinaryArray.Builder().Append(options.AsSpan()).Build(),
        });

    private static List<string> SchemaOf(byte[] ipc)
    {
        using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(new MemoryStream(ipc));
        return reader.Schema.FieldsList.Select(f => f.Name).ToList();
    }

    /// <summary>The write-options batch before protocol 3.3.</summary>
    private sealed class LegacyWriteOptions
    {
        public string ResultMode { get; set; } = "count";

        public string OnConflict { get; set; } = "throw";

        public List<string> OnConflictColumns { get; set; } = [];
    }

    // ------------------------------------------------------------------ attach options / data

    private static byte[] Options(params (string Name, object? Value)[] values)
    {
        var fields = new List<Field>();
        var arrays = new List<IArrowArray>();
        foreach (var (name, value) in values)
        {
            var type = value switch { bool => (IArrowType)BooleanType.Default, long => Int64Type.Default, _ => StringType.Default };
            fields.Add(new Field(name, type, nullable: true));
            arrays.Add(AnyArrayBuilder.Build(type, [value]));
        }

        return RecordBatchIpc.Write(new RecordBatch(new Schema(fields, metadata: null), arrays, 1));
    }

    [Fact]
    public void AttachOptionsReadTypedValues()
    {
        var options = AttachOptions.Read(Options(("Token", "tok-123"), ("transactional", true), ("limit", 7L), ("missing_value", null)));
        Assert.Equal("tok-123", options.GetString("token"));
        Assert.True(options.GetBoolean("TRANSACTIONAL"));
        Assert.Equal(7L, options.GetInt64("limit"));
        Assert.Equal(7.0, options.GetDouble("limit"));
        Assert.True(options.Contains("missing_value"));
        Assert.Null(options.GetString("missing_value"));
        Assert.Null(options.GetBoolean("absent"));
        Assert.Throws<FormatException>(() => options.GetBoolean("token"));
        Assert.Empty(AttachOptions.Read(new CatalogAttachRequest { Name = "c" }).Values);
    }

    [Fact]
    public void AttachOptionSpecsDeclareTypeDefaultAndFlags()
    {
        var spec = EmbeddedIpc.Decode<AttachOptionSpec>(AttachOptions.Spec("transactional", "d", BooleanType.Default, defaultValue: false));
        Assert.Equal(("transactional", false, false), (spec.Name, spec.Required, spec.Secret));
        Assert.False(((BooleanArray)RecordBatchIpc.Read(spec.DefaultValue!).Column(0)).GetValue(0));

        var secret = EmbeddedIpc.Decode<AttachOptionSpec>(AttachOptions.Spec("token", "d", StringType.Default, secret: true));
        Assert.True(secret.Secret);
        Assert.Null(secret.DefaultValue);

        Assert.Throws<ArgumentException>(() => AttachOptions.Spec("k", "d", StringType.Default, defaultValue: "x", required: true));
        Assert.Throws<ArgumentException>(() => AttachOptions.Spec("vgi_attach_ticket", "d", StringType.Default));
    }

    [Fact]
    public async Task AttachDataSplitsTheEnvelope()
    {
        var registry = new CatalogRegistry { OnAttach = _ => new AttachContext { Identity = "routed", ExtraOpaqueData = [1, 0, 2] } };
        var service = new VgiServiceImpl(registry);
        var first = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c" })).AttachOpaqueData;
        var second = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "c" })).AttachOpaqueData;

        var data = AttachData.Parse(first);
        Assert.Equal("routed", data.Identity);
        Assert.Equal(AttachData.AttachIdLength, data.AttachId.Length);
        Assert.Equal([1, 0, 2], data.Extra);
        Assert.Equal([1, 0, 2], AttachData.ExtraOf(first));
        Assert.NotEqual(data.AttachId, AttachData.Parse(second).AttachId);

        var empty = AttachData.Parse(null);
        Assert.Equal((CatalogRegistry.DefaultIdentity, 0, 0), (empty.Identity, empty.AttachId.Length, empty.Extra.Length));
    }

    // ------------------------------------------------------------------ async producer / processor

    private sealed class AsyncProducer : IAsyncTableFunctionProducer
    {
        public bool Done { get; private set; }

        public async Task ProduceAsync(OutputCollector output, CancellationToken cancellationToken)
        {
            await Task.Yield();
            if (!Done)
            {
                Done = true;
                output.Emit(new RecordBatch(s_input, [new Int64Array.Builder().Append(41).Build()], 1));
                return;
            }

            output.Finish();
        }
    }

    [Fact]
    public async Task AnAsyncProducerIsAwaited()
    {
        var producer = new AsyncProducer();
        using var output = new OutputCollector(s_input);
        await new TableProducerStreamState(producer).ProduceAsync(output, null, CancellationToken.None);
        Assert.Equal(41L, ((Int64Array)output.EmittedBatch!.Column(0)).GetValue(0));

        // The synchronous entry point still works for code that drives a producer directly.
        using var second = new OutputCollector(s_input);
        ((ITableFunctionProducer)new AsyncProducer()).Produce(second);
        Assert.NotNull(second.EmittedBatch);
    }

    private sealed class AsyncProcessor : IAsyncTableInOutProcessor
    {
        public async Task ProcessAsync(RecordBatch input, OutputCollector output, CancellationToken cancellationToken)
        {
            await Task.Yield();
            var x = (Int64Array)input.Column(0);
            output.Emit(new RecordBatch(s_input, [new Int64Array.Builder().Append(x.GetValue(0)!.Value * 2).Build()], 1));
        }

        public async Task FinalizeAsync(OutputCollector output, CancellationToken cancellationToken)
        {
            await Task.Yield();
            output.Finish();
        }
    }

    private sealed class AsyncFunction : ITableInOutFunction
    {
        public string Name => "async_double";

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

        public Schema OutputSchema => s_input;

        public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new AsyncProcessor();
    }

    [Fact]
    public async Task AnAsyncProcessorIsAwaitedPerTurnAndAtFinalize()
    {
        var function = new AsyncFunction();
        var processor = new AsyncProcessor();
        using var output = new OutputCollector(s_input);
        var batch = new RecordBatch(s_input, [new Int64Array.Builder().Append(21).Build()], 1);
        await new TableInOutExchangeStreamState(function, processor).ExchangeAsync(new AnnotatedBatch(batch, null), output, null, CancellationToken.None);
        Assert.Equal(42L, ((Int64Array)output.EmittedBatch!.Column(0)).GetValue(0));

        using var finalize = new OutputCollector(s_input);
        await new TableInOutFinalizeStreamState(processor).ProduceAsync(finalize, null, CancellationToken.None);
        Assert.Null(finalize.EmittedBatch);

        using var direct = new OutputCollector(s_input);
        ((ITableInOutProcessor)processor).Process(batch, direct);
        Assert.Equal(42L, ((Int64Array)direct.EmittedBatch!.Column(0)).GetValue(0));
    }

    // ------------------------------------------------------------------ helpers

    private static BindRequest BindFor(string name, byte[]? attach, byte[] tx) => new()
    {
        FunctionName = name,
        FunctionType = FunctionType.Table,
        Arguments = [],
        InputSchema = SchemaIpc.WriteSchemaOnly(s_input),
        AttachOpaqueData = attach,
        TransactionOpaqueData = tx,
    };

    /// <summary>Records what its bind and init saw.</summary>
    private sealed class CapturingInOut(string name = "capturing_in_out") : ITableInOutFunction
    {
        public byte[]? BindTx { get; private set; }

        public byte[]? InitTx { get; private set; }

        public byte[]? InitAttach { get; private set; }

        public int ResolveCalls { get; private set; }

        public string Name => name;

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

        public Schema OutputSchema => s_input;

        public void Bind(TableInOutBindParams bindParams) => BindTx = bindParams.TransactionOpaqueData;

        public Schema ResolveOutputSchema(TableInOutBindParams bindParams)
        {
            ResolveCalls++;
            return s_input;
        }

        public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams)
        {
            InitTx = initParams.TransactionOpaqueData;
            InitAttach = initParams.AttachOpaqueData;
            return new AsyncProcessor();
        }
    }
}
