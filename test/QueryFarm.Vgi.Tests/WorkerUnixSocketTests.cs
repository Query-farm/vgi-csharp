using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Client;
using QueryFarm.VgiRpc.Transport;
using Xunit;

namespace QueryFarm.Vgi.Tests;

/// <summary>
/// Coverage for <see cref="Worker.RunUnixSocketAsync"/> — the AF_UNIX launcher transport
/// (<c>LOCATION 'launch:&lt;argv&gt;'</c>). Verified end-to-end against the real DuckDB extension
/// separately (see the session's own manual smoke test: one worker process served two independent
/// <c>haybarn</c> processes attaching to the same tuple); these tests cover the primitive in
/// isolation — real client round-trip and idle-timeout self-shutdown — without a DuckDB binary.
/// </summary>
public sealed class WorkerUnixSocketTests
{
    private static string NewSocketPath() =>
        Path.Combine(Path.GetTempPath(), $"vgi-csharp-test-{Guid.NewGuid():N}.sock");

    [Fact]
    public async Task RunUnixSocketAsync_AcceptsARealClientConnectionAndDispatchesAWireCall()
    {
        // A real VGI client (the DuckDB extension) sends the vgi_rpc.protocol_version metadata key
        // this server enforces, proving full round-trip correctness end-to-end (also verified
        // manually against the real DuckDB extension via `launch:` — see this class's doc comment).
        // QueryFarm.VgiRpc's own generic RpcConnection<T> client has no VGI-specific knowledge of
        // that header, so this test targets what it CAN prove without one: the socket accepts a
        // real connection and the server dispatches (and correctly rejects, per protocol-version
        // enforcement) a real wire call — i.e. the launcher transport's socket mechanics work, not
        // just that a TCP-level handshake succeeded.
        var worker = new Worker().CatalogName("test_catalog").DefaultSchema("main");
        var path = NewSocketPath();
        using var cts = new CancellationTokenSource();
        var serveTask = worker.RunUnixSocketAsync(path, idleTimeoutSeconds: 30, cts.Token);

        try
        {
            // Wait for the socket file to appear rather than racing a fixed delay — bind happens
            // synchronously before RunUnixSocketAsync's first await, but give it a little slack.
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (!File.Exists(path) && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }

            Assert.True(File.Exists(path), "Worker did not bind its AF_UNIX socket in time.");

            using var clientTransport = (SocketTransport)await SocketTransport.ConnectUnixAsync(path);
            var connection = new RpcConnection<IVgiService>(clientTransport);
            var client = connection.CreateProxy();

            // Expected to fail with ProtocolVersionException (see comment above) — reaching that
            // exception (rather than a connection-level failure) proves the request was actually
            // accepted, read, and dispatched by the server over the real AF_UNIX socket.
            await Assert.ThrowsAsync<QueryFarm.VgiRpc.Errors.ProtocolVersionException>(
                () => client.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" }));
        }
        finally
        {
            cts.Cancel();
            await serveTask;
        }

        Assert.False(File.Exists(path), "Socket file should be unlinked once the accept loop ends.");
    }

    [Fact]
    public async Task RunUnixSocketAsync_SelfShutsDownAfterIdleTimeoutWithNoConnections()
    {
        var worker = new Worker();
        var path = NewSocketPath();

        var start = DateTime.UtcNow;
        // No client ever connects — the idle monitor should fire on its own well before this
        // test's own timeout, proving self-shutdown works without relying on external cancellation.
        var serveTask = worker.RunUnixSocketAsync(path, idleTimeoutSeconds: 0.5);
        var completed = await Task.WhenAny(serveTask, Task.Delay(TimeSpan.FromSeconds(10)));

        Assert.Same(serveTask, completed);
        await serveTask; // rethrow if it actually faulted instead of completing normally
        Assert.True(DateTime.UtcNow - start < TimeSpan.FromSeconds(10), "Idle self-shutdown took too long.");
        Assert.False(File.Exists(path), "Socket file should be unlinked after idle self-shutdown.");
    }

    [Fact]
    public async Task RunFromArgsAsync_WithUnixFlag_RoutesToUnixSocketAndHonorsIdleTimeout()
    {
        var worker = new Worker();
        var path = NewSocketPath();

        var start = DateTime.UtcNow;
        var serveTask = worker.RunFromArgsAsync(["--unix", path, "--idle-timeout", "0.5"]);
        var completed = await Task.WhenAny(serveTask, Task.Delay(TimeSpan.FromSeconds(10)));

        Assert.Same(serveTask, completed);
        await serveTask;
        Assert.True(DateTime.UtcNow - start < TimeSpan.FromSeconds(10), "Idle self-shutdown took too long.");
    }

    [Fact]
    public async Task RunFromArgsAsync_WithUnixFlagMissingPath_ThrowsArgumentException()
    {
        var worker = new Worker();
        await Assert.ThrowsAsync<ArgumentException>(() => worker.RunFromArgsAsync(["--unix"]));
    }
}
