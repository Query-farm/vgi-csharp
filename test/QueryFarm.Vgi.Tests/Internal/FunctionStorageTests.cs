using QueryFarm.Vgi.Internal;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// Coverage for <see cref="FunctionStorage"/>'s cross-process file layout, including a regression
/// test for a real race found this session: two concurrent <see cref="FunctionStorage.WriteSingle"/>
/// calls to the SAME (ns, key) used to share one temp filename (<c>finalPath + ".tmp"</c>), so
/// whichever call's <c>File.Move</c> ran second could find its own temp file already renamed away
/// by the first, throwing <see cref="FileNotFoundException"/> instead of just losing the race
/// harmlessly. Surfaced as flaky failures in <c>table_in_out/sum_all_columns.test</c> and
/// <c>table_in_out/parallel_finalize.test</c>, whose multi-worker execution can have two pooled
/// workers race to stash the same bind-context entry.
/// </summary>
public sealed class FunctionStorageTests
{
    private static byte[] NewExecutionId() => Guid.NewGuid().ToByteArray();

    [Fact]
    public void WriteSingleThenReadSingle_RoundTrips()
    {
        var executionId = NewExecutionId();
        var storage = new FunctionStorage(executionId);
        try
        {
            storage.WriteSingle("ns", "key", [1, 2, 3]);

            Assert.Equal<byte[]>([1, 2, 3], storage.ReadSingle("ns", "key")!);
        }
        finally
        {
            FunctionStorage.DeleteAll(executionId);
        }
    }

    [Fact]
    public void ReadSingle_NeverWritten_ReturnsNull()
    {
        var executionId = NewExecutionId();
        var storage = new FunctionStorage(executionId);
        try
        {
            Assert.Null(storage.ReadSingle("ns", "missing"));
        }
        finally
        {
            FunctionStorage.DeleteAll(executionId);
        }
    }

    [Fact]
    public void Append_ThenScanLog_ReturnsEveryEntry()
    {
        var executionId = NewExecutionId();
        var storage = new FunctionStorage(executionId);
        try
        {
            storage.Append("ns", "log-key", [1]);
            storage.Append("ns", "log-key", [2]);
            storage.Append("ns", "log-key", [3]);

            var entries = storage.ScanLog("ns", "log-key");

            Assert.Equal(3, entries.Count);
            Assert.Equal([1, 2, 3], entries.Select(e => (int)e[0]));
        }
        finally
        {
            FunctionStorage.DeleteAll(executionId);
        }
    }

    [Fact]
    public void ConcurrentWriteSingleCallsToTheSameKey_NeverThrow()
    {
        // Regression test for the shared-temp-filename race described in this class's doc comment.
        // Before the fix, running this loop a handful of times reliably reproduced
        // FileNotFoundException within a few iterations; 50 concurrent writers × 20 rounds gives
        // ample opportunity for the race to resurface if it were ever reintroduced.
        var executionId = NewExecutionId();
        var storage = new FunctionStorage(executionId);
        try
        {
            for (var round = 0; round < 20; round++)
            {
                var value = BitConverter.GetBytes(round);
                var writers = Enumerable.Range(0, 50)
                    .Select(_ => Task.Run(() => storage.WriteSingle("ns", "contended-key", value)))
                    .ToArray();

                var exception = Record.Exception(() => Task.WaitAll(writers));
                Assert.Null(exception);
            }

            // Whichever writer's value won the last round, SOME valid value must have survived.
            Assert.NotNull(storage.ReadSingle("ns", "contended-key"));
        }
        finally
        {
            FunctionStorage.DeleteAll(executionId);
        }
    }

    [Fact]
    public void DeleteAll_RemovesEverythingUnderTheExecution()
    {
        var executionId = NewExecutionId();
        var storage = new FunctionStorage(executionId);
        storage.WriteSingle("ns", "key", [1]);
        storage.Append("ns", "log-key", [1]);

        FunctionStorage.DeleteAll(executionId);

        Assert.Null(storage.ReadSingle("ns", "key"));
        Assert.Empty(storage.ScanLog("ns", "log-key"));
    }
}
