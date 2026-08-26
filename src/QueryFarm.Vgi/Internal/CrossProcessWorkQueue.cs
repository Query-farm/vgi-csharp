namespace QueryFarm.Vgi.Internal;

/// <summary>
/// A cross-PROCESS (not just cross-thread) atomic chunk-claim counter, keyed by execution id — the
/// coordination primitive a <c>MaxWorkers &gt; 1</c> table function needs to divide work across
/// however many parallel readers DuckDB opens for one logical scan.
///
/// Discovered empirically (diagnosed via <c>VGI_WORKER_STDERR_PASSTHROUGH=1</c> against the real
/// C++ extension — see <c>partitioned_sequence.test</c>'s own comment: "under subprocess each conn
/// maps to a distinct worker pid"): under the stdio/subprocess transport, DuckDB spawns a SEPARATE
/// OS PROCESS per parallel reader connection, not multiple threads inside one process. An in-memory
/// (even a <c>static ConcurrentDictionary</c>) work-queue is invisible across that process boundary
/// — each spawned process starts with its own empty state and independently "claims" the same first
/// chunk, producing duplicate rows. This coordinates via a small counter file in the temp directory
/// instead, claimed with an OS-level exclusive-lock retry loop (portable, no extra dependency) —
/// good enough for test-scale contention; a production-grade version would want a proper
/// cross-process primitive (a named semaphore/mutex) rather than lock-retry-on-open.
/// </summary>
public static class CrossProcessWorkQueue
{
    /// <summary>Atomically claims the next <paramref name="chunkSize"/>-row (or smaller, if this is
    /// the final chunk) range from the shared counter for <paramref name="key"/>, returning the
    /// number of rows claimed (0 once <paramref name="total"/> is exhausted) and the claimed
    /// range's start offset in <paramref name="start"/>.</summary>
    public static long ClaimChunk(string key, long chunkSize, long total, out long start)
    {
        var path = Path.Combine(Path.GetTempPath(), $"vgi_wq_{SanitizeKey(key)}.counter");

        while (true)
        {
            try
            {
                using var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                long current = 0;
                if (stream.Length >= sizeof(long))
                {
                    Span<byte> buffer = stackalloc byte[sizeof(long)];
                    var read = stream.Read(buffer);
                    if (read == sizeof(long))
                    {
                        current = BitConverter.ToInt64(buffer);
                    }
                }

                start = current;
                var claimed = current >= total ? 0 : Math.Min(chunkSize, total - current);

                stream.Position = 0;
                Span<byte> next = stackalloc byte[sizeof(long)];
                BitConverter.TryWriteBytes(next, current + chunkSize);
                stream.Write(next);
                stream.SetLength(sizeof(long));
                stream.Flush();

                return claimed;
            }
            catch (IOException)
            {
                // Another process holds the exclusive lock — brief backoff and retry. Test-scale
                // contention (single-digit concurrent processes) resolves in microseconds.
                Thread.Sleep(1);
            }
        }
    }

    private static string SanitizeKey(string key)
    {
        Span<char> buffer = stackalloc char[key.Length];
        for (var i = 0; i < key.Length; i++)
        {
            var c = key[i];
            buffer[i] = char.IsAsciiLetterOrDigit(c) ? c : '_';
        }

        return new string(buffer);
    }
}
