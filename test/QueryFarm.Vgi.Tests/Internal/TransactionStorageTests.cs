using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// Coverage for <c>table/transaction_storage.test</c>'s real per-transaction cross-process storage
/// support: <see cref="VgiServiceImpl.CatalogAttachAsync"/> advertising <c>SupportsTransactions</c>
/// scoped to the <c>"example"</c> identity only, the <c>CatalogTransactionBegin/Commit/RollbackAsync</c>
/// overrides, and <see cref="Table.TableBindParams.TransactionOpaqueData"/>/
/// <see cref="Table.TableInitParams.TransactionOpaqueData"/> actually reaching a bound/initialized
/// table function.
/// </summary>
public sealed class TransactionStorageTests
{
    private static VgiServiceImpl NewService() => new(new CatalogRegistry());

    [Fact]
    public async Task CatalogAttach_SupportsTransactions_TrueOnlyForExample()
    {
        var service = NewService();

        var example = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
        var accumulate = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "accumulate" });
        var narrowBind = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "narrow_bind" });

        Assert.True(example.SupportsTransactions);
        Assert.False(accumulate.SupportsTransactions);
        Assert.False(narrowBind.SupportsTransactions);
    }

    [Fact]
    public async Task CatalogTransactionBegin_MintsADistinctNonEmptyId_EveryCall()
    {
        var service = NewService();

        var first = await service.CatalogTransactionBeginAsync(attachOpaqueData: []);
        var second = await service.CatalogTransactionBeginAsync(attachOpaqueData: []);

        Assert.NotNull(first.TransactionOpaqueData);
        Assert.NotEmpty(first.TransactionOpaqueData!);
        Assert.NotEqual(first.TransactionOpaqueData, second.TransactionOpaqueData);
    }

    [Fact]
    public async Task CatalogTransactionCommit_ClearsFunctionStorageWrittenUnderThatTransactionId()
    {
        var service = NewService();
        var begin = await service.CatalogTransactionBeginAsync(attachOpaqueData: []);
        var txId = begin.TransactionOpaqueData!;

        new FunctionStorage(txId).WriteSingle("ns", "key", [1, 2, 3]);
        Assert.NotNull(new FunctionStorage(txId).ReadSingle("ns", "key"));

        await service.CatalogTransactionCommitAsync(attachOpaqueData: [], transactionOpaqueData: txId);

        Assert.Null(new FunctionStorage(txId).ReadSingle("ns", "key"));
    }

    [Fact]
    public async Task CatalogTransactionRollback_AlsoClearsFunctionStorage()
    {
        var service = NewService();
        var begin = await service.CatalogTransactionBeginAsync(attachOpaqueData: []);
        var txId = begin.TransactionOpaqueData!;

        new FunctionStorage(txId).WriteSingle("ns", "key", [9]);

        await service.CatalogTransactionRollbackAsync(attachOpaqueData: [], transactionOpaqueData: txId);

        Assert.Null(new FunctionStorage(txId).ReadSingle("ns", "key"));
    }

    [Fact]
    public async Task BindAsync_ThreadsTransactionOpaqueData_OntoTableBindParams()
    {
        var registry = new CatalogRegistry();
        var function = new CapturingTableFunction();
        registry.RegisterTable(function);
        var service = new VgiServiceImpl(registry);

        var attach = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
        byte[] txId = [1, 2, 3, 4];

        await service.BindAsync(new BindRequest
        {
            FunctionName = "capturing",
            FunctionType = FunctionType.Table,
            Arguments = [],
            AttachOpaqueData = attach.AttachOpaqueData,
            TransactionOpaqueData = txId,
        });

        Assert.Equal(txId, function.LastBindTransactionOpaqueData);
    }

    [Fact]
    public async Task BindAsync_NoTransactionOpaqueData_SeenAsEmpty_NotNull()
    {
        var registry = new CatalogRegistry();
        var function = new CapturingTableFunction();
        registry.RegisterTable(function);
        var service = new VgiServiceImpl(registry);

        var attach = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });

        await service.BindAsync(new BindRequest
        {
            FunctionName = "capturing",
            FunctionType = FunctionType.Table,
            Arguments = [],
            AttachOpaqueData = attach.AttachOpaqueData,
            TransactionOpaqueData = null,
        });

        Assert.NotNull(function.LastBindTransactionOpaqueData);
        Assert.Empty(function.LastBindTransactionOpaqueData!);
    }

    [Fact]
    public async Task InitAsync_ThreadsTransactionOpaqueData_OntoTableInitParams()
    {
        var registry = new CatalogRegistry();
        var function = new CapturingTableFunction();
        registry.RegisterTable(function);
        var service = new VgiServiceImpl(registry);

        var attach = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
        byte[] txId = [5, 6, 7];

        var bindRequest = new BindRequest
        {
            FunctionName = "capturing",
            FunctionType = FunctionType.Table,
            Arguments = [],
            AttachOpaqueData = attach.AttachOpaqueData,
            TransactionOpaqueData = txId,
        };

        await service.InitAsync(new InitRequest { BindCall = EmbeddedIpc.Encode(bindRequest) });

        Assert.Equal(txId, function.LastInitTransactionOpaqueData);
    }
}

/// <summary>Records the <c>TransactionOpaqueData</c> it was bound/initialized with, for the
/// <see cref="TransactionStorageTests"/> assertions above — a real (simplified) analog of
/// <c>TxCachedValueFunction</c>.</summary>
file sealed class CapturingTableFunction : ITableFunction
{
    public byte[]? LastBindTransactionOpaqueData { get; private set; }

    public byte[]? LastInitTransactionOpaqueData { get; private set; }

    public string Name => "capturing";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public void Bind(TableBindParams bindParams) => LastBindTransactionOpaqueData = bindParams.TransactionOpaqueData;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        LastInitTransactionOpaqueData = initParams.TransactionOpaqueData;
        return new NullProducer();
    }

    private sealed class NullProducer : ITableFunctionProducer
    {
        public void Produce(OutputCollector output) => output.Finish();
    }
}
