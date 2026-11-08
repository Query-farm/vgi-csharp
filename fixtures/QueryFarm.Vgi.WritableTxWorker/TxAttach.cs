using System.Security.Cryptography;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.WritableTxWorker;

/// <summary>What one attach of <c>txfix</c> is: its <c>token</c> (a secret option) and whether it is
/// transactional, both read through the SDK's attach-options accessor.</summary>
public sealed record TxAttachInfo(string? Token, bool Transactional)
{
    /// <summary>This attach's state partition (its token's).</summary>
    public Partition PartitionOf(StateDoc state) => state.For(Token);
}

/// <summary>
/// The ATTACH options and the attach hook. The secret <c>token</c> reaches every function and hook
/// through <see cref="AttachOptions"/> (the SDK keeps it encrypted inside the attach data); this
/// worker never stores it itself.
/// </summary>
public static class TxAttach
{
    public const string CatalogName = "txfix";

    public static CatalogInfo Catalog { get; } = new()
    {
        Name = CatalogName,
        AttachOptionSpecs =
        [
            AttachOptions.Spec("transactional", "Stage writes in worker transactions (supports_transactions)", BooleanType.Default, defaultValue: false),
            AttachOptions.Spec("token", "Credential every function of the attach must see", StringType.Default, secret: true),
        ],
    };

    public static AttachContext OnAttach(CatalogAttachRequest request) =>
        new() { SupportsTransactions = AttachOptions.Read(request).GetBoolean("transactional") ?? false };

    /// <summary>Reads an attach's options (any <c>*Params.AttachOptions</c> or
    /// <see cref="TransactionContext.AttachOptions"/>).</summary>
    public static TxAttachInfo Info(AttachOptions options) =>
        new(options.GetString("token"), options.GetBoolean("transactional") ?? false);

    /// <summary>The lowercase-hex transaction id, or <see langword="null"/> for none.</summary>
    public static string? TxId(byte[] transactionOpaqueData) =>
        transactionOpaqueData is { Length: > 0 } ? Convert.ToHexStringLower(transactionOpaqueData) : null;
}

/// <summary>
/// begin mints the id and logs it; commit applies the transaction's staged rows, or — when one
/// has a negative qty — discards them and fails the COMMIT; rollback discards them.
/// </summary>
public sealed class TxHooks : TransactionHooks
{
    public override Task<byte[]?> BeginAsync(TransactionContext context)
    {
        var id = RandomNumberGenerator.GetBytes(16);
        var attach = TxAttach.Info(context.AttachOptions);
        TxState.Update(state =>
        {
            attach.PartitionOf(state).AddEvent("begin", Convert.ToHexStringLower(id));
            return 0;
        });
        return Task.FromResult<byte[]?>(id);
    }

    public override Task CommitAsync(TransactionContext context)
    {
        var tx = Convert.ToHexStringLower(context.TransactionOpaqueData);
        var attach = TxAttach.Info(context.AttachOptions);
        var refused = TxState.Update(doc =>
        {
            var state = attach.PartitionOf(doc);
            var staged = state.Pending.GetValueOrDefault(tx) ?? [];
            state.Pending.Remove(tx);
            var bad = staged.Values.OrderBy(row => row.Id).FirstOrDefault(row => row.Qty < 0);
            if (bad is not null)
            {
                state.AddEvent("commit", tx, ok: false);
                return $"txfix: commit refused: qty {bad.Qty} for id {bad.Id}";
            }

            state.Items = [.. state.Items.Select(row => staged.GetValueOrDefault(row.Id) ?? row)];
            state.AddEvent("commit", tx, ok: true);
            return null;
        });
        return refused is null ? Task.CompletedTask : throw new InvalidOperationException(refused);
    }

    public override Task RollbackAsync(TransactionContext context)
    {
        var tx = Convert.ToHexStringLower(context.TransactionOpaqueData);
        var attach = TxAttach.Info(context.AttachOptions);
        TxState.Update(doc =>
        {
            var state = attach.PartitionOf(doc);
            state.Pending.Remove(tx);
            state.AddEvent("rollback", tx);
            return 0;
        });
        return Task.CompletedTask;
    }
}
