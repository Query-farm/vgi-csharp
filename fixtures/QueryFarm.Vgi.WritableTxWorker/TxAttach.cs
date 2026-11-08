using System.Security.Cryptography;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.WritableTxWorker;

/// <summary>What one attach of <c>txfix</c> is: transactional or not, and the handle its secret
/// <c>token</c> is kept under server-side.</summary>
public sealed record TxAttachInfo(bool Transactional, string Handle)
{
    /// <summary>The <c>token</c> this attach was given, or <see langword="null"/>.</summary>
    public string? Token => TxState.Read(state => state.Tokens.GetValueOrDefault(Handle));

    /// <summary>This attach's state partition (its token's).</summary>
    public Partition PartitionOf(StateDoc state) => state.For(state.Tokens.GetValueOrDefault(Handle));
}

/// <summary>
/// The ATTACH options and the attach hook. The secret <c>token</c> never goes into the attach's
/// opaque data (vgi-opaque-data-sealing.md rule 5 — on stdio and unix it reaches the client
/// unsealed): the hook stores it in the shared state under a random handle and keeps only the
/// handle and the <c>transactional</c> flag in <see cref="AttachContext.ExtraOpaqueData"/>.
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

    public static AttachContext OnAttach(CatalogAttachRequest request)
    {
        var options = AttachOptions.Read(request);
        var transactional = options.GetBoolean("transactional") ?? false;
        var handle = RandomNumberGenerator.GetBytes(16);
        if (options.GetString("token") is { } token)
        {
            TxState.Update(state => state.Tokens[Convert.ToHexStringLower(handle)] = token);
        }

        return new AttachContext
        {
            SupportsTransactions = transactional,
            ExtraOpaqueData = [transactional ? (byte)1 : (byte)0, .. handle],
        };
    }

    /// <summary>Decodes an attach's opaque data (any <c>*Params.AttachOpaqueData</c>).</summary>
    public static TxAttachInfo Info(byte[] attachOpaqueData)
    {
        var extra = AttachData.ExtraOf(attachOpaqueData);
        return extra.Length == 17
            ? new TxAttachInfo(extra[0] == 1, Convert.ToHexStringLower(extra.AsSpan(1)))
            : new TxAttachInfo(false, "");
    }

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
        var attach = TxAttach.Info(context.AttachOpaqueData);
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
        var attach = TxAttach.Info(context.AttachOpaqueData);
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
        var attach = TxAttach.Info(context.AttachOpaqueData);
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
