using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using QueryFarm.Vgi.Aggregate;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Http;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Identity;
using QueryFarm.VgiRpc.Http;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Transport;

namespace QueryFarm.Vgi;

/// <summary>
/// Fluent builder for a VGI worker process — ports vgi-java's <c>Worker</c> builder pattern.
/// Serves over stdio (the default, and what DuckDB's bare-command <c>LOCATION</c> subprocess
/// transport uses) or over an AF_UNIX socket (<see cref="RunUnixSocketAsync"/>, the
/// <c>LOCATION 'launch:&lt;argv&gt;'</c> pooled-launcher transport), raw TCP behind
/// an identity-preserving Iroh bridge, or HTTP through <see cref="RunHttpAsync"/>.
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
    /// request. 1.5.0 added the nullable <c>schema_name</c> field to
    /// <see cref="Protocol.ScanFunctionResult"/>/<see cref="Protocol.ScanBranch"/> — the worker's own
    /// authoritative schema for the scan/write function it just resolved, so the client no longer has
    /// to guess (the table's own schema, then <c>default_schema</c>) when one function name is
    /// registered in more than one schema. 2.0.0 replaces schema names throughout the protocol
    /// with raw identifier-component paths so schemas can be nested to arbitrary depth. 2.1.0 added
    /// the <c>catalog_contents</c> RPC and the <c>supports_catalog_contents</c> column on the
    /// <c>catalog_attach</c> result (see <see cref="CatalogContents"/>).</para>
    /// </summary>
    public const string DefaultProtocolVersion = "2.1.0";

    private readonly CatalogRegistry _catalog = new();
    private string _protocolVersion = DefaultProtocolVersion;
    private Func<IEnumerable<HostedProtocol>>? _hostedProtocols;
    private IdentityImpl.TokenResolver? _resolveToken;
    private IdentityImpl.GrantMinter? _mintGrant;
    private IReadOnlyList<string>? _introspectPrincipals;
    private double _maxAuthAge = 900.0;
    private RpcHttpEndpoints.AuthenticateDelegate? _httpAuthenticate;

    /// <summary>The transport a server is being built for. Only <see cref="Http"/> changes what is
    /// hosted (identity); the rest are named so that decision lives in <see cref="NewRpcServer"/>
    /// rather than at each call site.</summary>
    internal enum ServerTransport
    {
        Stdio,
        Unix,
        IrohTcp,
        Http,
    }

    /// <summary>The hook that supplies further application protocols for this worker to host
    /// beside <c>vgi.v2</c>, as <see cref="HostedProtocol"/> <c>(interface, implementation)</c>
    /// pairs.</summary>
    /// <param name="hook">Called <b>exactly once</b>, when the worker's server is built -- so it
    /// may consult configuration or the environment -- and its result is fixed for the life of the
    /// process. The protocols are hosted, in the order returned, on <b>every</b> transport this
    /// worker serves (stdio, unix, the Iroh raw upstream, HTTP), listed by
    /// <c>vgi_rpc.Reflection.v1</c> after <c>vgi.v2</c>.</param>
    /// <returns>This builder.</returns>
    /// <remarks>
    /// <para>Hosting another protocol cannot change <c>vgi.v2</c>: every request is routed on its
    /// <c>vgi_rpc.protocol</c> key with no fallback to the primary, so the DuckDB extension, which
    /// only ever names <c>vgi.v2</c>, dispatches exactly as it would against a single-protocol
    /// worker.</para>
    /// <para>Each protocol needs a distinct wire name (<c>[ProtocolName]</c> on its interface,
    /// conventionally dot-qualified with a major version), not <c>vgi.v2</c>, and not under the
    /// reserved <c>vgi_rpc.</c> prefix: framework protocols are not supplied here. Reflection is
    /// hosted automatically, and <c>vgi_rpc.Identity.v1</c> is enabled with
    /// <see cref="Identity"/>. The protocol is the unit of optionality -- there is no way to host
    /// a subset of a protocol's methods, so a capability that may be absent is its own protocol.</para>
    /// </remarks>
    /// <example>
    /// <code>
    /// new Worker()
    ///     .RegisterScalar(...)
    ///     .HostedProtocols(() => [HostedProtocol.For&lt;IReports&gt;(new Reports(config))])
    ///     .RunFromArgsAsync(args);
    /// </code>
    /// </example>
    public Worker HostedProtocols(Func<IEnumerable<HostedProtocol>> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        _hostedProtocols = hook;
        return this;
    }

    /// <summary>Opts this worker into hosting <c>vgi_rpc.Identity.v1</c> over HTTP.</summary>
    /// <param name="resolveToken">Resolves an opaque bearer credential to a principal; supplying it
    /// hosts <c>introspect_token</c>. Return <see langword="null"/> for "the store answered and
    /// the credential is unknown". For "the answer is not knowable" -- a store or sidecar is down,
    /// a timeout, a 5xx -- throw <see cref="QueryFarm.VgiRpc.Errors.AuthUnavailableException"/>
    /// (the same error an HTTP authenticate delegate throws to get a 503 with
    /// <c>Retry-After</c>): the framework reports it as <c>identity_unavailable</c> with your
    /// retry hint, and a caller that negative-caches the first answer must not cache this one.
    /// <see cref="IdentityUnavailableException"/> works too. Never throw an
    /// <see cref="ArgumentException"/> for an outage.</param>
    /// <param name="mintGrant">Mints a standing grant for the calling principal; supplying it
    /// hosts <c>issue_grant</c>. The same rule for transient failures applies.</param>
    /// <param name="introspectPrincipals">Principals allowed to call <c>introspect_token</c>.
    /// <see langword="null"/> reads <c>VGI_INTROSPECT_PRINCIPALS</c> (comma-separated). Required
    /// whenever <paramref name="resolveToken"/> is supplied: with neither, the worker refuses to
    /// start on HTTP.</param>
    /// <param name="maxAuthAge">How recently, in seconds, a caller must have authenticated to mint.</param>
    /// <returns>This builder.</returns>
    /// <remarks>
    /// <para>Absent unless opted into -- and then only the methods whose hooks were supplied are
    /// hosted, so a dependency upgrade never grows a credential-to-identity oracle on an existing
    /// worker. Hosted on HTTP only: its allowlist is a list of <em>principals</em>, which a
    /// transport without caller identity (stdio, unix) cannot check. Supply the caller's identity
    /// with <see cref="HttpAuthenticate"/>.</para>
    /// <para>There is no permissive default for the allowlist on purpose: authenticating and
    /// introspecting are different capabilities, and "any authenticated caller" lets any user
    /// resolve any other user's credential to its owner.</para>
    /// </remarks>
    public Worker Identity(
        IdentityImpl.TokenResolver? resolveToken = null,
        IdentityImpl.GrantMinter? mintGrant = null,
        IEnumerable<string>? introspectPrincipals = null,
        double maxAuthAge = 900.0)
    {
        _resolveToken = resolveToken;
        _mintGrant = mintGrant;
        _introspectPrincipals = introspectPrincipals?.ToList();
        _maxAuthAge = maxAuthAge;
        return this;
    }

    /// <summary>Authenticates HTTP callers -- composed with the Iroh bridge's peer identity when
    /// that is configured. Throw <c>AuthFailure</c> to reject; throw
    /// <see cref="QueryFarm.VgiRpc.Errors.AuthUnavailableException"/> when the authority cannot
    /// answer (503 with <c>Retry-After</c>, never a 401).</summary>
    public Worker HttpAuthenticate(RpcHttpEndpoints.AuthenticateDelegate authenticate)
    {
        ArgumentNullException.ThrowIfNull(authenticate);
        _httpAuthenticate = authenticate;
        return this;
    }

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

    /// <summary>See <see cref="CatalogRegistry.MarkIdentityExclusive"/>.</summary>
    public Worker MarkIdentityExclusive(string identity)
    {
        _catalog.MarkIdentityExclusive(identity);
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
        _catalog.DefaultSchemaPath = [name];
        return this;
    }

    /// <summary>Sets the default schema path used when a bind request omits one. DuckDB 1.x only
    /// consumes the final component in its attach result, while VGI 2.0 dispatch retains all
    /// components.</summary>
    public Worker DefaultSchema(IReadOnlyList<string> path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path.LastOrDefault());
        _catalog.DefaultSchemaPath = path.ToList();
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

    /// <summary>Registers a <c>catalog_attach</c> hook — called once per ATTACH, before
    /// <c>VgiServiceImpl.CatalogAttachAsync</c> builds its result, with the raw
    /// <see cref="Protocol.CatalogAttachRequest"/> (caller-supplied options, requested
    /// <c>data_version_spec</c>/<c>implementation_version</c>). A worker serving several catalog
    /// names (via <see cref="RegisterCatalog"/>) switches on <c>request.Name</c> inside the handler
    /// — this is a single global hook, not one per catalog name.
    ///
    /// Throw to reject the ATTACH: the exception's <c>Message</c> propagates verbatim to the
    /// client as the ATTACH failure (the same generic unary-RPC-error path every other worker
    /// exception already uses — no special handling needed). Return a non-null
    /// <see cref="AttachContext"/> to validate-and-accept while customizing the result (a resolved
    /// version, extra opaque payload, an overridden routing identity for
    /// per-attach-differentiated catalogs — see that type's doc comment); return
    /// <see langword="null"/> to accept with today's default behavior unchanged. Leaving no
    /// handler registered at all is exactly the same as one that always returns
    /// <see langword="null"/>.</summary>
    public Worker OnAttach(Func<Protocol.CatalogAttachRequest, Protocol.AttachContext?> handler)
    {
        _catalog.OnAttach = handler;
        return this;
    }

    /// <summary>Whether <c>catalog_attach</c> advertises
    /// <see cref="Protocol.CatalogAttachResult.SupportsCatalogContents"/>, letting the client load
    /// the whole catalog with one <c>catalog_contents</c> call instead of <c>catalog_schemas</c> plus
    /// a <c>catalog_schema_contents_*</c> call per schema and kind. On by default — this worker's
    /// catalogs are declarative (fixed by the <c>Register*</c> calls, no runtime DDL), the shape
    /// vgi-python's <c>ReadOnlyCatalogInterface</c> advertises it for. The RPC itself is always
    /// served (see <see cref="Protocol.IVgiService.CatalogContentsAsync"/>); turning this off only
    /// stops the client from calling it.</summary>
    public Worker CatalogContents(bool enabled)
    {
        _catalog.SupportsCatalogContents = enabled;
        return this;
    }

    /// <summary>Registers a hook run with the attach identity before every <c>catalog_contents</c>
    /// answer is built. Throwing refuses the call — the exception's message reaches the client as
    /// the RPC error, and a client falls back to <c>catalog_schemas</c> plus the per-schema RPCs —
    /// mirroring how a vgi-python catalog overrides <c>catalog_contents</c>. To stop the client
    /// calling it at all, withdraw the advertisement instead (<see cref="CatalogContents"/>, or
    /// <see cref="Protocol.AttachContext.SupportsCatalogContents"/> per attach).</summary>
    public Worker OnCatalogContents(Action<string> handler)
    {
        _catalog.OnCatalogContents = handler;
        return this;
    }

    /// <summary>Answers <c>catalog_contents</c> for one catalog identity — the C# counterpart of
    /// overriding vgi-python's <c>CatalogInterface.catalog_contents(attach_opaque_data,
    /// if_none_match)</c>. The handler gets the client's <see cref="Catalog.CatalogContentsRequest.IfNoneMatch"/>
    /// and returns a <see cref="Catalog.CatalogContentsResult"/>: typically
    /// <see cref="Catalog.CatalogContentsResult.Unchanged"/> when it equals a cheap validator
    /// (generation counter, schema version, git sha) — <i>before</i> building anything — and
    /// otherwise <see cref="Catalog.CatalogContentsRequest.BuildAsync"/>'s snapshot with that etag:
    /// <code>
    /// worker.OnCatalogContents("mycat", async request =&gt;
    ///     request.IfNoneMatch == etag
    ///         ? CatalogContentsResult.Unchanged(etag)
    ///         : await request.BuildAsync() with { Etag = etag });
    /// </code>
    /// The worker enforces the rules (not-modified only with an etag equal to
    /// <c>if_none_match</c> and no schemas; a full answer whose etag matches is sent not-modified; a
    /// result with no etag never is) and checks the schema paths. Runs after
    /// <see cref="OnCatalogContents(Action{string})"/>.</summary>
    public Worker OnCatalogContents(string identity, Func<Catalog.CatalogContentsRequest, Task<Catalog.CatalogContentsResult>> handler)
    {
        _catalog.SetCatalogContentsHandler(identity, handler);
        return this;
    }

    /// <summary>Opts catalogs in to a framework-supplied <c>catalog_contents</c> etag when they
    /// return none of their own — <see cref="Catalog.CatalogContentsEtagMode.ContentHash"/>: the
    /// SHA-256 of the snapshot, the same digest vgi-python computes, with a matching
    /// <c>if_none_match</c> answered not-modified. <paramref name="identity"/> scopes it to one
    /// catalog identity; <see langword="null"/> sets it for every catalog without its own setting.
    /// Off by default, as in vgi-python. Every <see cref="Worker"/> catalog is version-frozen and its
    /// snapshot is cached (see <see cref="Internal.VgiServiceImpl"/>), so the hash is computed once
    /// per registration change, not per call.</summary>
    public Worker CatalogContentsEtag(Catalog.CatalogContentsEtagMode mode, string? identity = null)
    {
        if (identity is null)
        {
            _catalog.CatalogContentsEtag = mode;
        }
        else
        {
            _catalog.SetCatalogContentsEtag(identity, mode);
        }

        return this;
    }

    /// <summary>Serves <paramref name="name"/> as a DDL-capable in-memory catalog — the C#
    /// counterpart of vgi-python's <c>InMemoryCatalog</c>. Every ATTACH of it gets a private, empty
    /// catalog (one schema, the worker's default schema) that <c>CREATE</c>/<c>DROP SCHEMA</c>,
    /// <c>CREATE</c>/<c>DROP TABLE</c> and <c>CREATE</c>/<c>DROP VIEW</c> (with <c>OR REPLACE</c> /
    /// <c>IF [NOT] EXISTS</c>) change; DETACH discards it. Its tables are catalog entries only (a
    /// scan of one fails). The attach is not version-frozen: <c>catalog_version</c> is a generation
    /// counter every DDL moves (or always 0, see <see cref="Catalog.InMemoryCatalogOptions.ReportsVersion"/>),
    /// and <c>catalog_contents</c> is advertised and composed fresh on every call — combine with
    /// <see cref="OnCatalogContents(string, Func{Catalog.CatalogContentsRequest, Task{Catalog.CatalogContentsResult}})"/>
    /// (whose <see cref="Catalog.CatalogContentsRequest.CatalogVersion"/> is that counter) or
    /// <see cref="CatalogContentsEtag"/> to revalidate it. The state lives in this process, so it
    /// needs one long-lived worker (<c>launch:</c> or HTTP).</summary>
    public Worker RegisterInMemoryCatalog(string name, Catalog.InMemoryCatalogOptions? options = null)
    {
        _catalog.RegisterInMemoryCatalog(name, options);
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
    /// <c>CatalogRegistry.RegisterSchema</c>.</summary>
    public Worker RegisterSchema(string schemaName, string? comment = null, Dictionary<string, string>? tags = null, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterSchema(schemaName, comment, tags, identity);
        return this;
    }

    public Worker RegisterSchema(IReadOnlyList<string> schemaPath, string? comment = null, Dictionary<string, string>? tags = null, string identity = CatalogRegistry.DefaultIdentity)
    {
        _catalog.RegisterSchema(schemaPath, comment, tags, identity);
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

    /// <summary>
    /// The one place this worker's RPC server is built, for every transport, so a transport added
    /// later inherits the wiring rather than repeating it.
    /// </summary>
    /// <remarks>
    /// The protocol's wire name rides on the contract type itself — <see cref="IVgiService"/>
    /// carries <c>[ProtocolName]</c>, so this is hosted under <see cref="VgiProtocol.Name"/> and
    /// not under the C# type's own name, and any other site that hosts the same interface gets
    /// the same name whether or not it went through here.
    /// </remarks>
    /// <para>What it hosts, in reflection order: <c>vgi.v2</c>; the protocols the
    /// <see cref="HostedProtocols"/> hook returns, on every transport; <c>vgi_rpc.Reflection.v1</c>,
    /// on every transport; and <c>vgi_rpc.Identity.v1</c> on HTTP when <see cref="Identity"/> was
    /// called.</para>
    internal RpcServer NewRpcServer(ServerTransport transport, string? serverId = null)
    {
        var extra = ValidatedHostedProtocols();
        var identity = transport == ServerTransport.Http ? BuildIdentity() : null;
        try
        {
            return new RpcServer(
                typeof(IVgiService), new VgiServiceImpl(_catalog), serverId: serverId,
                expectedProtocolVersion: _protocolVersion, identity: identity, additionalProtocols: extra);
        }
        catch (ArgumentException exc) when (extra.Count > 0)
        {
            throw new ArgumentException($"{HookName}: {exc.Message}", exc);
        }
    }

    private const string HookName = "Worker.HostedProtocols hook";

    /// <summary>Calls the hook once and checks what it returned, so an error names the hook rather
    /// than only the protocol type vgi-rpc would name.</summary>
    private List<HostedProtocol> ValidatedHostedProtocols()
    {
        if (_hostedProtocols is null)
        {
            return [];
        }

        var returned = _hostedProtocols() ?? throw new ArgumentException($"{HookName} returned null; return an empty sequence instead.");
        var primary = QueryFarm.VgiRpc.Reflection.WireNaming.ForProtocol(typeof(IVgiService));
        var seen = new Dictionary<string, Type>(StringComparer.Ordinal);
        var pairs = new List<HostedProtocol>();
        var index = 0;
        foreach (var entry in returned)
        {
            if (entry is null)
            {
                throw new ArgumentException($"{HookName} entry {index} is null.");
            }

            string name;
            try
            {
                name = QueryFarm.VgiRpc.Reflection.WireNaming.ForProtocol(entry.ServiceInterface);
            }
            catch (ArgumentException exc)
            {
                throw new ArgumentException($"{HookName} entry {index} ({entry.ServiceInterface.Name}): {exc.Message}", exc);
            }

            if (QueryFarm.VgiRpc.Reflection.WireNaming.IsReservedProtocolName(name))
            {
                throw new ArgumentException(
                    $"{HookName} entry {index} ({entry.ServiceInterface.Name}) is named '{name}', which claims the "
                    + "reserved 'vgi_rpc.' prefix. Framework protocols are not supplied through this hook: reflection "
                    + "is hosted automatically, and vgi_rpc.Identity.v1 is enabled with Worker.Identity(...).");
            }

            if (name == primary)
            {
                throw new ArgumentException(
                    $"{HookName} entry {index} ({entry.ServiceInterface.Name}) is named '{name}', the worker's own "
                    + "protocol. Give it a distinct [ProtocolName].");
            }

            if (seen.TryGetValue(name, out var previous))
            {
                throw new ArgumentException(
                    $"{HookName} lists protocol name '{name}' twice ({previous.Name} and {entry.ServiceInterface.Name}). "
                    + "The name is the routing key, so each hosted protocol needs a distinct [ProtocolName].");
            }

            seen[name] = entry.ServiceInterface;
            pairs.Add(entry);
            index++;
        }

        return pairs;
    }

    /// <summary>The <c>vgi_rpc.Identity.v1</c> implementation, or <see langword="null"/> when the
    /// worker did not opt in -- absent beats routed-and-refusing.</summary>
    /// <exception cref="InvalidOperationException">A resolver was supplied without an introspector
    /// allowlist -- the worker refuses to start.</exception>
    private IdentityImpl? BuildIdentity()
    {
        if (_resolveToken is null && _mintGrant is null)
        {
            return null;
        }

        // Only consulted for introspect_token: a worker that mints but resolves nothing is not an
        // oracle and needs no allowlist.
        IReadOnlyList<string>? principals = null;
        if (_resolveToken is not null)
        {
            principals = (_introspectPrincipals
                    ?? (Environment.GetEnvironmentVariable("VGI_INTROSPECT_PRINCIPALS") ?? "").Split(','))
                .Select(p => p.Trim())
                .Where(p => p.Length > 0)
                .ToList();
            if (principals.Count == 0)
            {
                throw new InvalidOperationException(
                    "This worker supplies a resolveToken hook, which hosts the vgi_rpc.Identity.v1 protocol, but no "
                    + "introspector allowlist was configured. Pass introspectPrincipals to Worker.Identity(...) or set "
                    + "VGI_INTROSPECT_PRINCIPALS (comma-separated). There is no permissive default on purpose: "
                    + "introspection is a separate capability from authentication, and allowing every authenticated "
                    + "caller lets any user resolve any other user's credential to its owner. Remove the resolveToken "
                    + "hook to leave the protocol unhosted entirely.");
            }
        }

        return new IdentityImpl(_resolveToken, _mintGrant, principals, _maxAuthAge);
    }

    /// <summary>Serves over stdin/stdout until the client disconnects.</summary>
    public Task RunStdioAsync(CancellationToken cancellationToken = default)
    {
        var server = NewRpcServer(ServerTransport.Stdio);
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
        var server = NewRpcServer(ServerTransport.Unix);

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
    /// Serves the raw Arrow-mux upstream expected by <c>vgi-iroh-bridge</c>.
    /// The listener is deliberately loopback-only, requires the bridge's
    /// identity-bearing PROXY v2 preamble, and authenticates the Iroh EndpointId
    /// by default. Set <paramref name="authenticate"/> to false for observation mode.
    /// </summary>
    public async Task RunIrohTcpUpstreamAsync(
        string host,
        int port,
        string issuer,
        IEnumerable<string>? trustedProxyAddresses = null,
        bool authenticate = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsLoopbackHost(host))
            throw new ArgumentException("Iroh bridge upstream must bind loopback.", nameof(host));
        ArgumentException.ThrowIfNullOrWhiteSpace(issuer);

        var server = NewRpcServer(ServerTransport.IrohTcp);
        var options = new TcpServerOptions
        {
            ProxyProtocolV2Required = true,
            TrustedProxyAddresses = (trustedProxyAddresses ?? ["127.0.0.1"]).ToArray(),
            IrohProxyIssuer = issuer,
            PeerAuthenticationPolicy = authenticate
                ? PeerAuthenticationPolicies.Primary("iroh")
                : PeerAuthenticationPolicies.Observe,
        };
        await SocketTransport.ServeTcpAsync(
            host,
            port,
            (transport, token) => server.ServeAsync(transport, token),
            options,
            cancellationToken,
            actualPort =>
            {
                Console.WriteLine($"TCP:{host}:{actualPort}");
                Console.Out.Flush();
            }).ConfigureAwait(false);
    }

    /// <summary>
    /// Serves the VGI HTTP protocol. Supplying <paramref name="irohBridge"/>
    /// enables identity-preserving HTTP-over-Iroh behind an adjacent bridge;
    /// the HTTP semantics, response budgets, continuations, and externalized
    /// payload behavior remain unchanged.
    /// </summary>
    public async Task RunHttpAsync(
        string host = "127.0.0.1",
        int port = 0,
        string prefix = "",
        IrohBridgeOptions? irohBridge = null,
        CancellationToken cancellationToken = default)
    {
        if (irohBridge is not null && !IsLoopbackHost(host))
            throw new ArgumentException("Iroh HTTP bridge upstream must bind loopback.", nameof(host));

        var serverId = Guid.NewGuid().ToString("n");
        var rpc = NewRpcServer(ServerTransport.Http, serverId);
        var builder = WebApplication.CreateSlimBuilder(WorkerHostOptions());
        builder.WebHost.UseUrls($"http://{FormatHostForUrl(host)}:{port}");
        var app = builder.Build();

        var authenticate = _httpAuthenticate;
        if (irohBridge is not null)
        {
            app.UseVgiRpcPhysicalPeerSnapshot();
            var provider = IrohPeerIdentityProviders.Forwarded(
                irohBridge.Issuer,
                irohBridge.EffectiveTrustedProxyAddresses);
            authenticate = PeerIdentityAuthentication.Compose(
                _httpAuthenticate,
                [provider],
                irohBridge.Authenticate
                    ? PeerAuthenticationPolicies.Primary("iroh")
                    : PeerAuthenticationPolicies.Observe);
        }

        app.MapVgiRpc(rpc, prefix: prefix, authenticate: authenticate);
        app.MapVgiLandingPage(_catalog.CatalogName, serverId, prefix, authenticate: authenticate);
        await app.StartAsync(cancellationToken).ConfigureAwait(false);
        var addresses = app.Services.GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()?.Addresses;
        var actualPort = addresses?
            .Select(address => new Uri(address).Port)
            .FirstOrDefault() ?? port;
        Console.WriteLine($"PORT:{actualPort}");
        Console.Out.Flush();
        try
        {
            await app.WaitForShutdownAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await app.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The canonical CLI entry point every worker's <c>Main</c> calls. Understands the launcher
    /// transport (<c>--unix &lt;path&gt; [--idle-timeout &lt;seconds&gt;]</c>), HTTP,
    /// and the Iroh bridge flags; it defaults to stdio when no transport is selected.
    /// </summary>
    public Task RunFromArgsAsync(string[] args, CancellationToken cancellationToken = default)
    {
        var httpIndex = Array.IndexOf(args, "--http");
        if (httpIndex >= 0)
        {
            var port = 0;
            var positionalPort = httpIndex + 1 < args.Length
                && !args[httpIndex + 1].StartsWith("--", StringComparison.Ordinal);
            var portIndex = Array.IndexOf(args, "--port");
            if (positionalPort && portIndex >= 0)
                throw new ArgumentException("Use either --http PORT or --port, not both.", nameof(args));
            var portValue = positionalPort
                ? args[httpIndex + 1]
                : portIndex >= 0 && portIndex + 1 < args.Length ? args[portIndex + 1] : "0";
            if (!int.TryParse(portValue, out port) || port is < 0 or > 65535)
                throw new ArgumentException("--http port must be in 0..65535.", nameof(args));
            var hostIndex = Array.IndexOf(args, "--host");
            if (hostIndex >= 0 && hostIndex + 1 >= args.Length)
                throw new ArgumentException("--host requires a value.", nameof(args));
            var host = hostIndex >= 0 ? args[hostIndex + 1] : "127.0.0.1";
            var issuerIndex = Array.IndexOf(args, "--iroh-issuer");
            IrohBridgeOptions? bridge = null;
            if (issuerIndex >= 0)
            {
                if (issuerIndex + 1 >= args.Length)
                    throw new ArgumentException("--iroh-issuer requires a value.", nameof(args));
                var trusted = ValuesAfter(args, "--iroh-trusted-proxy");
                bridge = new IrohBridgeOptions(
                    args[issuerIndex + 1],
                    trusted.Count == 0 ? null : trusted,
                    !args.Contains("--iroh-observe", StringComparer.Ordinal));
            }
            return RunHttpAsync(host: host, port: port, irohBridge: bridge, cancellationToken: cancellationToken);
        }

        var irohIndex = Array.IndexOf(args, "--iroh-raw-upstream");
        if (irohIndex >= 0)
        {
            if (irohIndex + 1 >= args.Length)
                throw new ArgumentException("--iroh-raw-upstream requires [HOST:]PORT.", nameof(args));
            var issuerIndex = Array.IndexOf(args, "--iroh-issuer");
            if (issuerIndex < 0 || issuerIndex + 1 >= args.Length)
                throw new ArgumentException("--iroh-raw-upstream requires --iroh-issuer.", nameof(args));
            var (host, port) = ParseTcpBind(args[irohIndex + 1]);
            var trusted = ValuesAfter(args, "--iroh-trusted-proxy");
            return RunIrohTcpUpstreamAsync(
                host,
                port,
                args[issuerIndex + 1],
                trusted.Count == 0 ? null : trusted,
                authenticate: !args.Contains("--iroh-observe", StringComparer.Ordinal),
                cancellationToken: cancellationToken);
        }

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

    /// <summary>Host options for the HTTP worker: content root at the application's own
    /// directory, configuration reload off.</summary>
    /// <remarks>
    /// The defaults make the content root the process's cwd and watch it for configuration
    /// changes. A worker has no appsettings to reload, and a launcher or harness may start it from
    /// a home directory or a large checkout, where setting up that watch took more than a minute
    /// (measured: 80 s from a 511 GB home directory, under 1 s with either of these off) before
    /// the worker could print <c>PORT:</c>.
    /// </remarks>
    internal static WebApplicationOptions WorkerHostOptions() => new()
    {
        ContentRootPath = AppContext.BaseDirectory,
        Args = ["--hostBuilder:reloadConfigOnChange=false"],
    };

    private static bool IsLoopbackHost(string host) =>
        host is "localhost" or "127.0.0.1" or "::1"
        || System.Net.IPAddress.TryParse(host, out var address)
            && System.Net.IPAddress.IsLoopback(address);

    private static string FormatHostForUrl(string host) =>
        host.Contains(':', StringComparison.Ordinal) ? $"[{host}]" : host;

    private static (string Host, int Port) ParseTcpBind(string value)
    {
        var host = "127.0.0.1";
        var portText = value;
        var split = value.LastIndexOf(':');
        if (split >= 0)
        {
            host = value[..split];
            portText = value[(split + 1)..];
            if (host.Length == 0) host = "127.0.0.1";
            if (host.Length >= 2 && host[0] == '[' && host[^1] == ']') host = host[1..^1];
        }
        if (!int.TryParse(portText, out var port) || port is < 0 or > 65535)
            throw new ArgumentException("--iroh-raw-upstream requires [HOST:]PORT in 0..65535.");
        return (host, port);
    }

    private static List<string> ValuesAfter(string[] args, string flag)
    {
        var values = new List<string>();
        for (var index = 0; index < args.Length; index++)
        {
            if (args[index] != flag) continue;
            if (++index >= args.Length)
                throw new ArgumentException($"{flag} requires a value.", nameof(args));
            values.Add(args[index]);
        }
        return values;
    }
}
