using System.Text.Json;
using System.Text.Json.Serialization;

namespace QueryFarm.Vgi.WritableTxWorker;

/// <summary>One <c>items</c> row.</summary>
public sealed record ItemRow(int Id, string Name, long? Qty);

/// <summary>One <c>labels</c> row.</summary>
public sealed record LabelRow(int Id, string? Label);

/// <summary>One event-log entry; <see cref="Ok"/> is false only for a refused commit.</summary>
public sealed record EventRow(long Seq, string Kind, string TxId, bool Ok);

/// <summary>One finished write call (<c>writes()</c>): its operation, transaction id (null on the
/// non-transactional attach) and the rows it reported.</summary>
public sealed record WriteRow(long Seq, string Op, string? TxId, long Rows, bool Finalized);

/// <summary>One token's partition: committed rows of both tables, changes staged per transaction
/// id (lowercase hex), the event log and the write calls.</summary>
public sealed class Partition
{
    public List<ItemRow> Items { get; set; } = [];

    public List<LabelRow> Labels { get; set; } = [];

    /// <summary>Per transaction id, the staged image of each <c>items</c> row it touched by id: the
    /// row as inserted or updated, or <see langword="null"/> for a staged delete.</summary>
    public Dictionary<string, Dictionary<int, ItemRow?>> Pending { get; set; } = [];

    public List<EventRow> Events { get; set; } = [];

    public long Seq { get; set; }

    public List<WriteRow> Writes { get; set; } = [];

    public long WriteSeq { get; set; }

    public void AddWrite(string op, string? txId, long rows)
    {
        WriteSeq++;
        Writes.Add(new WriteRow(WriteSeq, op, txId, rows, Finalized: true));
    }

    /// <summary>The staged images of <paramref name="txId"/>, created empty on first use.</summary>
    public Dictionary<int, ItemRow?> Staged(string txId) =>
        Pending.TryGetValue(txId, out var staged) ? staged : Pending[txId] = [];

    /// <summary>Applies staged images to the committed rows.</summary>
    public void Apply(IReadOnlyDictionary<int, ItemRow?> staged) => Items = Overlay(Items, staged);

    private static List<ItemRow> Overlay(IEnumerable<ItemRow> rows, IReadOnlyDictionary<int, ItemRow?>? staged)
    {
        var byId = rows.ToDictionary(row => row.Id);
        foreach (var (id, image) in staged ?? new Dictionary<int, ItemRow?>())
        {
            if (image is null)
            {
                byId.Remove(id);
            }
            else
            {
                byId[id] = image;
            }
        }

        return [.. byId.Values.OrderBy(row => row.Id)];
    }

    public static Partition Initial() => new()
    {
        Items = [new(1, "a", 10), new(2, "b", 20), new(3, "c", 30)],
        Labels = [new(1, "x"), new(2, "y")],
    };

    public void AddEvent(string kind, string txId, bool ok = true)
    {
        Seq++;
        Events.Add(new EventRow(Seq, kind, txId, ok));
    }

    /// <summary>The <c>items</c> rows as transaction <paramref name="txId"/> sees them: committed
    /// rows overlaid with its staged inserts, updates and deletes (none when
    /// <paramref name="txId"/> is null), by id.</summary>
    public List<ItemRow> ItemsFor(string? txId) =>
        Overlay(Items, txId is null ? null : Pending.GetValueOrDefault(txId));
}

/// <summary>The fixture's whole state: one <see cref="Partition"/> per <c>token</c> value (a NULL
/// token is its own partition).</summary>
public sealed class StateDoc
{
    private const string NullToken = "null";

    public Dictionary<string, Partition> Partitions { get; set; } = [];

    /// <summary>The partition of <paramref name="token"/>, created with the initial rows on first use.</summary>
    public Partition For(string? token)
    {
        var key = token is null ? NullToken : "t:" + token;
        if (!Partitions.TryGetValue(key, out var partition))
        {
            Partitions[key] = partition = Partition.Initial();
        }

        return partition;
    }
}

/// <summary>
/// The state, shared by every worker process (stdio and launcher pools, HTTP): a JSON file under
/// <c>$VGI_TXFIX_STATE_DIR</c> (default <c>&lt;temp&gt;/vgi-txfix</c>), read-modify-written under an
/// exclusive lock file. Write calls run concurrently (the actions of one MERGE, in different
/// processes, next to the scan feeding them), so each batch's read-validate-apply is one
/// <see cref="Update{T}"/>, and a taken lock is waited for, not failed on.
/// </summary>
public static class TxState
{
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    private static string Dir =>
        Environment.GetEnvironmentVariable("VGI_TXFIX_STATE_DIR") is { Length: > 0 } dir
            ? dir
            : Path.Combine(Path.GetTempPath(), "vgi-txfix");

    /// <summary>Runs <paramref name="action"/> on the current state under the lock and saves it.</summary>
    public static T Update<T>(Func<StateDoc, T> action)
    {
        Directory.CreateDirectory(Dir);
        using var lockFile = AcquireLock(Path.Combine(Dir, "state.lock"));
        var path = Path.Combine(Dir, "state.json");
        var state = File.Exists(path)
            ? JsonSerializer.Deserialize<StateDoc>(File.ReadAllBytes(path), Json) ?? new StateDoc()
            : new StateDoc();
        var result = action(state);
        var temp = path + "." + Environment.ProcessId + ".tmp";
        File.WriteAllBytes(temp, JsonSerializer.SerializeToUtf8Bytes(state, Json));
        File.Move(temp, path, overwrite: true);
        return result;
    }

    /// <summary>Reads the current state under the lock.</summary>
    public static T Read<T>(Func<StateDoc, T> read) => Update(read);

    private static FileStream AcquireLock(string path)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            try
            {
                return new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException) when (DateTime.UtcNow < deadline)
            {
                Thread.Sleep(5);
            }
        }
    }
}
