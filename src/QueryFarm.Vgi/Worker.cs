using QueryFarm.Vgi.Aggregate;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Transport;

namespace QueryFarm.Vgi;

/// <summary>
/// Fluent builder for a VGI worker process — ports vgi-java's <c>Worker</c> builder pattern.
/// Serves over stdio (the default, and what DuckDB's bare-command <c>LOCATION</c> subprocess
/// transport uses) or over an AF_UNIX socket (<see cref="RunUnixSocketAsync"/>, the
/// <c>LOCATION 'launch:&lt;argv&gt;'</c> pooled-launcher transport). <c>RunTcp</c>/<c>RunHttp</c>
/// and the rest of <c>RunFromArgs</c>'s flag surface land in later milestones.
///
/// CRITICAL: stdout is the wire channel (stdio mode) or the launcher's discovery-line channel
/// (unix-socket mode) — never write to <see cref="Console.Out"/> from a registered function or
/// from a worker's own <c>Main</c>; use <see cref="Console.Error"/> for any diagnostics.
/// </summary>
public sealed class Worker
{
    /// <summary>
    /// VGI application protocol surface version this worker declares — emitted as the
    /// <c>vgi_rpc.protocol_version</c> per-request metadata key and enforced by
    /// <c>QueryFarm.VgiRpc.Server.RpcServer</c> (exact major+minor match; patch ignored) at the
    /// dispatch boundary, before any method-specific handling runs. Mirrors vgi-python's
    /// <c>VgiProtocol.protocol_version</c>/vgi-java's <c>Worker.VGI_PROTOCOL_VERSION</c> —
    /// bump rules: MAJOR = backward-incompatible surface change, MINOR = additive, PATCH = worker
    /// bug fixes.
    ///
    /// <para>1.1.0 added the nullable <c>schema_name</c> field to the bind request. 1.3.0 added
    /// <c>global_functions</c>/<c>global_function_prefix</c> to the <c>catalog_attach</c> result
    /// (see <see cref="Protocol.CatalogAttachResult"/>). 1.4.0 added <c>table_function_plan</c>
    /// (split-based scan planning) plus <c>split_tokens</c>/<c>row_limit</c> on the init
    /// request.</para>
    /// </summary>
    public const string DefaultProtocolVersion = "1.4.0";

    private readonly CatalogRegistry _catalog = new();
    private string _protocolVersion = DefaultProtocolVersion;

    /// <summary>Overrides the declared VGI protocol version — for test fixtures ONLY (e.g.
    /// <c>protocol_version/version_mismatch.test</c>'s deliberately-incompatible worker). Every
    /// real worker should leave this at <see cref="DefaultProtocolVersion"/>.</summary>
    public Worker ProtocolVersion(string version)
    {
        _protocolVersion = version;
        return this;
    }

    /// <summary>Declares a catalog this worker process serves, visible via the pre-<c>ATTACH</c>
    /// discovery table function <c>vgi_catalogs('&lt;location&gt;')</c> — see
    /// <see cref="CatalogRegistry.RegisterCatalog"/>'s doc comment (including the
    /// <paramref name="exclusive"/> parameter's meaning). Optional: a worker with none declared is
    /// still perfectly attachable (this only affects PRE-attach discovery); most
    /// single-logical-catalog fixtures never call this.</summary>
    public Worker RegisterCatalog(Protocol.CatalogInfo info, bool exclusive = false)
    {
        _catalog.RegisterCatalog(info, exclusive);
        return this;
    }

    public Worker CatalogName(string name)
    {
        _catalog.CatalogName = name;
        return this;
    }

    public Worker DefaultSchema(string name)
    {
        _catalog.DefaultSchema = name;
        return this;
    }

    /// <summary>Declares this worker's database-level comment, surfaced via
    /// <c>duckdb_databases().comment</c> (see <see cref="CatalogRegistry.DatabaseComment"/>).</summary>
    public Worker DatabaseComment(string comment)
    {
        _catalog.DatabaseComment = comment;
        return this;
    }

    /// <summary>Declares this worker's database-level tags, surfaced via
    /// <c>duckdb_databases().tags</c> (see <see cref="CatalogRegistry.DatabaseTags"/>).</summary>
    public Worker DatabaseTags(Dictionary<string, string> tags)
    {
        _catalog.DatabaseTags = tags;
        return this;
    }

    /// <summary>Registers a scalar function. <paramref name="identity"/> is the attach-identity
    /// bucket it lives under (see <see cref="CatalogRegistry"/>'s doc comment) — leave it at the
    /// default for an ordinary single-logical-catalog worker; set it to a specific attach name
    /// (the first argument of <c>ATTACH '&lt;name&gt;' AS ...</c>) only for a fixture that
    /// deliberately serves different function sets depending on which name the SAME worker binary
    /// was attached under (<c>same_name_catalogs.test</c>).</summary>
    public Worker RegisterScalar(IScalarFunction function, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterScalar(function, identity);
        return this;
    }

    /// <summary>Registers a table ("producer") function — see <see cref="RegisterScalar"/>'s doc
    /// comment for the <paramref name="identity"/> parameter's meaning.</summary>
    public Worker RegisterTable(ITableFunction function, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterTable(function, identity);
        return this;
    }

    /// <summary>Registers a streaming table-in-out function — see <see cref="RegisterScalar"/>'s doc
    /// comment for the <paramref name="identity"/> parameter's meaning.</summary>
    public Worker RegisterTableInOut(ITableInOutFunction function, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterTableInOut(function, identity);
        return this;
    }

    /// <summary>Registers a table-buffering (Sink+Source) function — see <see cref="RegisterScalar"/>'s
    /// doc comment for the <paramref name="identity"/> parameter's meaning.</summary>
    public Worker RegisterTableBuffering(ITableBufferingFunction function, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterTableBuffering(function, identity);
        return this;
    }

    /// <summary>Registers an aggregate function — see <see cref="RegisterScalar"/>'s doc comment
    /// for the <paramref name="identity"/> parameter's meaning.</summary>
    public Worker RegisterAggregate(IAggregateFunction function, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterAggregate(function, identity);
        return this;
    }

    /// <summary>Sets the prefix (<c>&lt;prefix&gt;_&lt;name&gt;</c>) every <c>RegisterGlobal*</c>
    /// function is published under catalog-wide — see <see cref="Protocol.CatalogAttachResult.GlobalFunctionPrefix"/>.
    /// Leave unset (<c>""</c>) to publish bare names.</summary>
    public Worker GlobalFunctionPrefix(string prefix)
    {
        _catalog.GlobalFunctionPrefix = prefix;
        return this;
    }

    /// <summary>Registers a scalar function BOTH at its normal schema-qualified name AND
    /// catalog-wide (callable unqualified, or with <see cref="GlobalFunctionPrefix"/>) — see
    /// <see cref="CatalogRegistry.GlobalFunctions"/>'s doc comment.</summary>
    public Worker RegisterGlobalScalar(IScalarFunction function, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterScalar(function, identity);
        _catalog.RegisterGlobalFunction(function);
        return this;
    }

    /// <summary>Registers a table function BOTH at its normal schema-qualified name AND
    /// catalog-wide — see <see cref="RegisterGlobalScalar"/>'s doc comment.</summary>
    public Worker RegisterGlobalTable(ITableFunction function, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterTable(function, identity);
        _catalog.RegisterGlobalFunction(function);
        return this;
    }

    /// <summary>Registers a table-in-out function BOTH at its normal schema-qualified name AND
    /// catalog-wide — see <see cref="RegisterGlobalScalar"/>'s doc comment.</summary>
    public Worker RegisterGlobalTableInOut(ITableInOutFunction function, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterTableInOut(function, identity);
        _catalog.RegisterGlobalFunction(function);
        return this;
    }

    /// <summary>Registers a table-buffering function BOTH at its normal schema-qualified name AND
    /// catalog-wide — see <see cref="RegisterGlobalScalar"/>'s doc comment.</summary>
    public Worker RegisterGlobalTableBuffering(ITableBufferingFunction function, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterTableBuffering(function, identity);
        _catalog.RegisterGlobalFunction(function);
        return this;
    }

    /// <summary>Registers an aggregate function BOTH at its normal schema-qualified name AND
    /// catalog-wide — see <see cref="RegisterGlobalScalar"/>'s doc comment.</summary>
    public Worker RegisterGlobalAggregate(IAggregateFunction function, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterAggregate(function, identity);
        _catalog.RegisterGlobalFunction(function);
        return this;
    }

    /// <summary>Declares a global/session DuckDB setting (<c>SET &lt;name&gt; = ...</c>) this worker
    /// exposes via <c>catalog_attach</c> — a setting must be declared here at least once for
    /// <c>duckdb_settings()</c> to know it exists at all; a function's own
    /// <see cref="Attributes.SettingAttribute"/>/<c>RequiredSettings</c> only reads an
    /// already-declared setting's current value at bind time. <paramref name="defaultValue"/> is a
    /// single-element Arrow array (e.g. <c>new BooleanArray.Builder().Append(false).Build()</c>)
    /// holding the setting's default; pass <see langword="null"/> for a setting with no default
    /// (e.g. a struct-typed setting with no meaningful all-fields default).</summary>
    public Worker RegisterSetting(string name, string description, Apache.Arrow.Types.IArrowType type, Apache.Arrow.IArrowArray? defaultValue = null)
    {
        _catalog.RegisterSetting(Internal.SettingSpecBuilder.Build(name, description, type, defaultValue));
        return this;
    }

    /// <summary>Declares a custom DuckDB secret TYPE (<c>CREATE SECRET (TYPE &lt;name&gt;, ...)</c>)
    /// this worker exposes via <c>catalog_attach</c> — a secret type must be declared here at least
    /// once for <c>duckdb_secret_types()</c>/<c>CREATE SECRET</c> to know it exists at all.
    /// <paramref name="parametersSchema"/> describes the secret's key/value parameters; mark a
    /// sensitive field's metadata <c>"redact":"true"</c> so DuckDB masks it in <c>duckdb_secrets()</c>.
    /// A function reads a resolved secret of this type via <see cref="Attributes.SecretAttribute"/>
    /// (scalar) or <c>ITableFunction.RequiredSecrets</c>/<see cref="Internal.SecretsAccessor"/>
    /// (table/table-in-out, including dynamic scope-based lookups).</summary>
    public Worker RegisterSecretType(string name, string description, Apache.Arrow.Schema parametersSchema)
    {
        _catalog.RegisterSecretType(Internal.SecretTypeSpecBuilder.Build(name, description, parametersSchema));
        return this;
    }

    /// <summary>Declares a schema's comment/tags explicitly — optional, see
    /// <see cref="CatalogRegistry.RegisterSchema"/>.</summary>
    public Worker RegisterSchema(string schemaName, string? comment = null, Dictionary<string, string>? tags = null, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterSchema(schemaName, comment, tags, identity);
        return this;
    }

    /// <summary>Registers a real catalog table (queryable as a plain table, e.g.
    /// <c>SELECT * FROM catalog.schema.table_name</c> — not just as <c>schema.function_name(...)</c>)
    /// — see <see cref="CatalogTable"/>'s doc comment.</summary>
    public Worker RegisterCatalogTable(CatalogTable table, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterCatalogTable(table, identity);
        return this;
    }

    /// <summary>Registers a real catalog view.</summary>
    public Worker RegisterView(CatalogView view, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterView(view, identity);
        return this;
    }

    /// <summary>Registers a real catalog macro (scalar or table).</summary>
    public Worker RegisterMacro(CatalogMacro macro, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterMacro(macro, identity);
        return this;
    }

    /// <summary>Registers a <c>COPY ... FROM (FORMAT '&lt;formatName&gt;', ...)</c> reader —
    /// <paramref name="handler"/> is an ordinary <see cref="Table.ITableFunction"/> (also
    /// registered under its own name, exactly as <see cref="RegisterTable"/> would) whose
    /// <see cref="Table.TableBindParams.CopyFrom"/>/<see cref="Table.TableInitParams.CopyFrom"/>
    /// carry the destination path and DuckDB-required output schema. <paramref name="formatName"/>
    /// is the bare (unqualified) name <c>FORMAT '&lt;alias&gt;.&lt;formatName&gt;'</c> will use —
    /// see <see cref="Protocol.CopyFromFormatInfo.FormatName"/>'s doc comment.</summary>
    public Worker RegisterCopyFromFormat(
        Table.ITableFunction handler, string formatName, string? description = null, string? comment = null,
        Dictionary<string, string>? tags = null, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterTable(handler, identity);
        _catalog.RegisterCopyFormat(
            new CopyFormat
            {
                FormatName = formatName,
                Handler = handler.Name,
                Direction = "from",
                Options = handler.ArgumentsSchema,
                Description = description ?? handler.Description,
                Comment = comment,
                Tags = tags ?? [],
            },
            identity);
        return this;
    }

    /// <summary>Registers a <c>COPY ... TO (FORMAT '&lt;formatName&gt;', ...)</c> writer —
    /// <paramref name="handler"/> is an ordinary <see cref="Buffering.ITableBufferingFunction"/>
    /// (also registered under its own name, exactly as <see cref="RegisterTableBuffering"/> would)
    /// whose <see cref="TableInOut.TableInOutBindParams.CopyTo"/>/
    /// <see cref="Buffering.TableBufferingProcessParams.CopyTo"/>/
    /// <see cref="Buffering.TableBufferingCombineParams.CopyTo"/> carry the destination path.
    /// <paramref name="formatName"/> — see <see cref="RegisterCopyFromFormat"/>'s doc comment.
    /// <see cref="Buffering.ITableBufferingFunction.SinkOrderDependent"/> is advertised as this
    /// format's <c>ordered</c> flag automatically.</summary>
    public Worker RegisterCopyToFormat(
        Buffering.ITableBufferingFunction handler, string formatName, string? description = null, string? comment = null,
        Dictionary<string, string>? tags = null, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterTableBuffering(handler, identity);
        _catalog.RegisterCopyFormat(
            new CopyFormat
            {
                FormatName = formatName,
                Handler = handler.Name,
                Direction = "to",
                Options = handler.ArgumentsSchema,
                Ordered = handler.SinkOrderDependent,
                Description = description ?? handler.Description,
                Comment = comment,
                Tags = tags ?? [],
            },
            identity);
        return this;
    }

    /// <summary>Serves over stdin/stdout until the client disconnects.</summary>
    public Task RunStdioAsync(CancellationToken cancellationToken = default)
    {
        var impl = new VgiServiceImpl(_catalog);
        var server = new RpcServer(typeof(IVgiService), impl, expectedProtocolVersion: _protocolVersion);
        return server.ServeAsync(new StdioTransport(), cancellationToken);
    }

    /// <summary>
    /// Serves over an AF_UNIX domain socket at <paramref name="path"/> — the launcher transport
    /// (<c>LOCATION 'launch:&lt;argv&gt;'</c>), letting a single worker process amortize its own
    /// startup cost across every DuckDB connection/process pointed at the same worker tuple. Follows
    /// the worker-side contract in <c>~/Development/vgi/docs/launcher-protocol.md</c> exactly: binds,
    /// emits exactly one <c>UNIX:&lt;abs path&gt;</c> line on stdout (flushed, and nothing else on
    /// stdout ever again — logging MUST go to <see cref="Console.Error"/>), serves each connection on
    /// its own task, and self-shuts-down once <paramref name="idleTimeoutSeconds"/> have elapsed with
    /// zero connected clients (0 = never times out). Returns once the socket has been closed and every
    /// in-flight connection has drained — whether from an idle timeout or <paramref name="cancellationToken"/>
    /// being cancelled by the caller (e.g. on SIGTERM/SIGINT).
    /// </summary>
    public async Task RunUnixSocketAsync(string path, double idleTimeoutSeconds = 300, CancellationToken cancellationToken = default)
    {
        var impl = new VgiServiceImpl(_catalog);
        var server = new RpcServer(typeof(IVgiService), impl, expectedProtocolVersion: _protocolVersion);

        using var shutdownCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var activeConnections = 0;
        var lastActivityTicks = DateTime.UtcNow.Ticks;

        Task? idleMonitorTask = null;
        if (idleTimeoutSeconds > 0)
        {
            var idleTimeout = TimeSpan.FromSeconds(idleTimeoutSeconds);
            idleMonitorTask = Task.Run(async () =>
            {
                while (!shutdownCts.IsCancellationRequested)
                {
                    try
                    {
                        // Poll frequently enough that a burst of secondary-worker connections
                        // (a parallel scan opening its per-substream connections shortly after
                        // the primary's) is never mistaken for sustained idleness.
                        await Task.Delay(TimeSpan.FromMilliseconds(500), shutdownCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    if (Volatile.Read(ref activeConnections) != 0)
                    {
                        continue;
                    }

                    var idleSince = new DateTime(Interlocked.Read(ref lastActivityTicks), DateTimeKind.Utc);
                    if (DateTime.UtcNow - idleSince >= idleTimeout)
                    {
                        shutdownCts.Cancel();
                        break;
                    }
                }
            }, cancellationToken);
        }

        async Task HandleConnectionAsync(IRpcTransport transport, CancellationToken connectionToken)
        {
            Interlocked.Increment(ref activeConnections);
            try
            {
                await server.ServeAsync(transport, connectionToken).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref activeConnections);
                Interlocked.Exchange(ref lastActivityTicks, DateTime.UtcNow.Ticks);
            }
        }

        void OnBound()
        {
            Console.WriteLine($"UNIX:{path}");
            Console.Out.Flush();
        }

        try
        {
            await SocketTransport.ServeUnixAsync(path, HandleConnectionAsync, shutdownCts.Token, OnBound).ConfigureAwait(false);
        }
        finally
        {
            if (idleMonitorTask is not null)
            {
                try
                {
                    await idleMonitorTask.ConfigureAwait(false);
                }
                catch
                {
                    // The monitor's own Task.Delay throwing OperationCanceledException on shutdown
                    // is expected — nothing else it can throw should silently swallow a real bug,
                    // but there's nothing meaningful to do with it here either way.
                }
            }
        }
    }

    /// <summary>
    /// The canonical CLI entry point every worker's <c>Main</c> calls. Understands the launcher
    /// transport (<c>--unix &lt;path&gt; [--idle-timeout &lt;seconds&gt;]</c>) and defaults to stdio
    /// when no flags are given — <c>--http</c>/<c>--tcp</c>/<c>--access-log</c> are parsed by later
    /// milestones.
    /// </summary>
    public Task RunFromArgsAsync(string[] args, CancellationToken cancellationToken = default)
    {
        var unixIndex = Array.IndexOf(args, "--unix");
        if (unixIndex >= 0)
        {
            if (unixIndex + 1 >= args.Length)
            {
                throw new ArgumentException("--unix requires a socket path.", nameof(args));
            }

            var path = args[unixIndex + 1];
            var idleTimeoutSeconds = 300.0;
            var idleIndex = Array.IndexOf(args, "--idle-timeout");
            if (idleIndex >= 0)
            {
                if (idleIndex + 1 >= args.Length || !double.TryParse(args[idleIndex + 1], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out idleTimeoutSeconds))
                {
                    throw new ArgumentException("--idle-timeout requires a numeric seconds value.", nameof(args));
                }

                // The wire encoding treats 0 and negative alike as "unbounded" (see
                // docs/launcher-protocol.md's idle-timeout table) — RunUnixSocketAsync only checks
                // ">0", so a negative value would otherwise be misread as "already expired".
                if (idleTimeoutSeconds < 0)
                {
                    idleTimeoutSeconds = 0;
                }
            }

            return RunUnixSocketAsync(path, idleTimeoutSeconds, cancellationToken);
        }

        return RunStdioAsync(cancellationToken);
    }
}
