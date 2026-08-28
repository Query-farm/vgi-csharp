using Apache.Arrow;
using QueryFarm.Vgi.Aggregate;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Scalar;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The set of functions/settings a <see cref="Worker"/> has registered, and the small bits of
/// catalog identity (name/default schema) <see cref="VgiServiceImpl"/> needs to answer the
/// catalog-discovery RPCs.
///
/// Functions are keyed by <c>(identity, schemaName, name)</c> rather than just <c>name</c> for two
/// reasons the M2 scalar test suite exercises directly:
/// <list type="bullet">
/// <item><b>Same name, different schema, same catalog</b> (<c>same_name_schemas.test</c>) — e.g.
/// <c>main.test_same_name_bind</c> and <c>data.test_same_name_bind</c> are two distinct
/// registrations that must not collide in a name-only dictionary.</item>
/// <item><b>Same worker process, attached as two different catalogs</b>
/// (<c>same_name_catalogs.test</c>) — DuckDB's <c>ATTACH '&lt;name&gt;' AS alias (...)</c> sends
/// that first <c>'&lt;name&gt;'</c> string as <see cref="Protocol.CatalogAttachRequest.Name"/>;
/// the SAME worker binary attached twice under two different names must be able to serve two
/// disjoint function sets. <see cref="VgiServiceImpl"/> threads that attach name through as the
/// "identity" (via <c>CatalogAttachResult.AttachOpaqueData</c>, echoed back on every subsequent
/// bind/catalog RPC) so a single <see cref="CatalogRegistry"/> can hold BOTH catalogs' functions
/// side by side.</item>
/// </list>
/// A function registered without an explicit identity lives under <see cref="DefaultIdentity"/>
/// (<c>""</c>) and is visible under ANY attach name that has no identity-specific registrations of
/// its own — the common case every other fixture (single logical catalog) relies on.
///
/// <b>Overloading</b> (<c>test/sql/integration/overload/*.test</c>): each <c>(identity, schemaName,
/// name)</c> key maps to a LIST of candidates, not a single function — registering a second function
/// under an already-used name is additive, not a silent overwrite. A key with exactly one candidate
/// resolves unconditionally (the overwhelmingly common case); a key with several candidates is
/// disambiguated at bind/init time by <see cref="OverloadResolver"/> against the call-site's resolved
/// argument types, mirroring the grouping the C++ extension does for its OWN <c>ScalarFunctionSet</c>/
/// <c>TableFunctionSet</c> (see <c>storage/vgi_scalar_function_set.cpp</c>/<c>vgi_table_function_set.cpp</c>) —
/// DuckDB's binder already grouped every <see cref="Protocol.FunctionInfo"/> sharing a name into one
/// overload set and resolved the call to exactly one of them before ever sending a bind RPC; this
/// registry's job is only to emit all of those <see cref="Protocol.FunctionInfo"/> rows for discovery
/// and to re-derive (redundantly, since the wire carries no overload id) which one a given bind/init
/// call meant.
/// </summary>
public sealed class CatalogRegistry
{
    /// <summary>The identity bucket ordinary (non-multi-catalog-identity) registrations live
    /// under, and the fallback every attach name resolves to when it has no dedicated bucket.</summary>
    public const string DefaultIdentity = "";

    public string CatalogName { get; set; } = "example";

    public string DefaultSchema { get; set; } = "main";

    /// <summary>Database-level comment/tags surfaced via <c>duckdb_databases()</c> — set through
    /// <see cref="Worker.DatabaseComment"/>/<see cref="Worker.DatabaseTags"/>, read back by
    /// <c>VgiServiceImpl.CatalogAttachAsync</c> onto <see cref="Protocol.CatalogAttachResult.Comment"/>/
    /// <see cref="Protocol.CatalogAttachResult.Tags"/>. <see langword="null"/>/empty (the default)
    /// reports no comment and no tags — this worker never had a per-identity need for these (unlike
    /// schemas' <see cref="RegisterSchema"/>), so they're plain single-valued properties.</summary>
    public string? DatabaseComment { get; set; }

    public Dictionary<string, string> DatabaseTags { get; set; } = [];

    /// <summary>The pre-<c>ATTACH</c> discovery surface (<c>vgi_catalogs('&lt;location&gt;')</c>) —
    /// the catalog NAMES this worker process serves, each a valid first argument to
    /// <c>ATTACH '&lt;name&gt;' AS ...</c> (see <see cref="Protocol.CatalogAttachRequest.Name"/>, the
    /// SAME string this registry's identity-bucket routing keys off — a "MetaWorker" fixture
    /// serving several logically-distinct catalogs from one process registers one
    /// <see cref="Protocol.CatalogInfo"/> per name here, then registers each catalog's OWN
    /// functions under a matching <c>identity:</c>).</summary>
    private readonly List<Protocol.CatalogInfo> _catalogs = [];

    /// <summary>Identities that never fall back to <see cref="DefaultIdentity"/>'s bucket — a
    /// wholly independent catalog (e.g. a "MetaWorker" fixture's second, unrelated catalog) whose
    /// content must NOT inherit the primary catalog's default-bucket registrations the way
    /// <c>same_name_catalogs.test</c>'s twin_a/twin_b identities deliberately do. See
    /// <see cref="RegisterCatalog"/>'s <c>exclusive</c> parameter.</summary>
    private readonly HashSet<string> _exclusiveIdentities = new(StringComparer.Ordinal);

    /// <param name="info">The catalog's discovery metadata.</param>
    /// <param name="exclusive">When <see langword="true"/>, <paramref name="info"/>.Name's identity
    /// bucket is self-contained — lookups for it NEVER fall back to <see cref="DefaultIdentity"/>'s
    /// bucket, even for a <c>(schemaName, name)</c> it has no registration of its own for. Leave
    /// <see langword="false"/> (the default) for a worker's own PRIMARY catalog, whose content
    /// lives in the default bucket precisely so ordinary (no explicit <c>identity:</c>)
    /// registrations reach it.</param>
    public void RegisterCatalog(Protocol.CatalogInfo info, bool exclusive = false)
    {
        _catalogs.Add(info);
        if (exclusive)
        {
            _exclusiveIdentities.Add(info.Name);
        }
    }

    /// <summary>Marks an identity bucket exclusive (see <see cref="RegisterCatalog"/>'s
    /// <c>exclusive</c> parameter for what that means) WITHOUT adding a corresponding
    /// pre-attach-discoverable <see cref="Protocol.CatalogInfo"/> — for an identity that is never
    /// itself an ATTACH-able catalog name, e.g. a composite <c>"&lt;name&gt;@&lt;resolved-version&gt;"</c>
    /// bucket a worker's own <see cref="Protocol.AttachContext.Identity"/> routes to internally (one
    /// real catalog name fanning out into several isolated table sets keyed by a value resolved at
    /// attach time — see <c>ExampleWorker.Versioned.VersionedTablesSetup</c>). Calling
    /// <see cref="RegisterCatalog"/> for such an identity instead would leak it into
    /// <c>vgi_catalogs()</c> discovery as a spurious extra catalog, which is exactly what this
    /// avoids.</summary>
    public void MarkIdentityExclusive(string identity) => _exclusiveIdentities.Add(identity);

    public IReadOnlyList<Protocol.CatalogInfo> Catalogs => _catalogs;

    private bool FallsBackToDefault(string identity) => identity != DefaultIdentity && !_exclusiveIdentities.Contains(identity);

    private readonly Dictionary<(string Identity, string SchemaName, string Name), List<IScalarFunction>> _scalarFunctions = new();
    private readonly Dictionary<(string Identity, string SchemaName, string Name), List<ITableFunction>> _tableFunctions = new();
    private readonly Dictionary<(string Identity, string SchemaName, string Name), List<ITableInOutFunction>> _tableInOutFunctions = new();
    private readonly Dictionary<(string Identity, string SchemaName, string Name), List<ITableBufferingFunction>> _tableBufferingFunctions = new();
    private readonly Dictionary<(string Identity, string SchemaName, string Name), List<IAggregateFunction>> _aggregateFunctions = new();

    private readonly Dictionary<(string Identity, string SchemaName, string Name), CatalogTable> _tables = new();
    private readonly Dictionary<(string Identity, string SchemaName, string Name), CatalogView> _views = new();
    private readonly Dictionary<(string Identity, string SchemaName, string Name), CatalogMacro> _macros = new();

    private readonly Dictionary<(string Identity, string FormatName), CopyFormat> _copyFormats = new();

    /// <summary>Per-<c>(identity, schemaName)</c> schema-level comment/tags — set via
    /// <see cref="RegisterSchema"/>. A schema with no explicit registration (the common case — most
    /// fixture schemas are implied purely by their tables'/functions' <c>SchemaName</c>) reports no
    /// comment and no tags.</summary>
    private readonly Dictionary<(string Identity, string SchemaName), (string? Comment, Dictionary<string, string> Tags)> _schemas = new();

    /// <summary>Every declared global/session setting (<c>Worker.Settings</c>) — see
    /// <see cref="Protocol.SettingSpec"/>. Order is registration order (also the order advertised on
    /// <c>CatalogAttachResult.Settings</c>).</summary>
    private readonly List<Protocol.SettingSpec> _settings = [];

    /// <summary>Every declared custom DuckDB secret TYPE (<c>Worker.RegisterSecretType</c>) — see
    /// <see cref="Protocol.SecretTypeSpec"/>. Order is registration order (also the order advertised
    /// on <c>CatalogAttachResult.SecretTypes</c>).</summary>
    private readonly List<Protocol.SecretTypeSpec> _secretTypes = [];

    /// <summary>The worker-wide <c>catalog_attach</c> validation/customization hook (<see
    /// cref="Worker.OnAttach"/>), if any — consulted by <c>VgiServiceImpl.CatalogAttachAsync</c>
    /// before building the result. A single global hook, not per-catalog-name: a fixture serving
    /// several catalog names switches on <see cref="Protocol.CatalogAttachRequest.Name"/> inside
    /// it, mirroring how a Python worker's own <c>catalog_attach</c> override dispatches.</summary>
    public Func<Protocol.CatalogAttachRequest, Protocol.AttachContext?>? OnAttach { get; set; }

    public void RegisterScalar(IScalarFunction function, string identity = DefaultIdentity) =>
        Add(_scalarFunctions, identity, function.SchemaName, function.Name, function);

    public void RegisterTable(ITableFunction function, string identity = DefaultIdentity) =>
        Add(_tableFunctions, identity, function.SchemaName, function.Name, function);

    public void RegisterTableInOut(ITableInOutFunction function, string identity = DefaultIdentity) =>
        Add(_tableInOutFunctions, identity, function.SchemaName, function.Name, function);

    public void RegisterTableBuffering(ITableBufferingFunction function, string identity = DefaultIdentity) =>
        Add(_tableBufferingFunctions, identity, function.SchemaName, function.Name, function);

    public void RegisterAggregate(IAggregateFunction function, string identity = DefaultIdentity) =>
        Add(_aggregateFunctions, identity, function.SchemaName, function.Name, function);

    public void RegisterSetting(Protocol.SettingSpec setting) => _settings.Add(setting);

    public IReadOnlyList<Protocol.SettingSpec> Settings => _settings;

    public void RegisterSecretType(Protocol.SecretTypeSpec secretType) => _secretTypes.Add(secretType);

    public IReadOnlyList<Protocol.SecretTypeSpec> SecretTypes => _secretTypes;

    /// <summary>Functions published catalog-wide (protocol 1.3.0+ <c>CatalogAttachResult.global_functions</c>)
    /// — each element one of <see cref="IScalarFunction"/>/<see cref="ITableFunction"/>/
    /// <see cref="ITableInOutFunction"/>/<see cref="ITableBufferingFunction"/>/<see cref="IAggregateFunction"/>,
    /// ALSO independently reachable at its normal schema-qualified name (global publication is purely
    /// an additive alias, never a replacement for the qualified registration). Registration order is
    /// advertisement order.</summary>
    private readonly List<object> _globalFunctions = [];

    public void RegisterGlobalFunction(object function) => _globalFunctions.Add(function);

    public IReadOnlyList<object> GlobalFunctions => _globalFunctions;

    /// <summary>Prefix prepended (with an underscore) to every <see cref="GlobalFunctions"/> entry's
    /// published name — <c>""</c> (the default) publishes bare names.</summary>
    public string GlobalFunctionPrefix { get; set; } = "";

    private static void Add<T>(
        Dictionary<(string Identity, string SchemaName, string Name), List<T>> store,
        string identity, string schemaName, string name, T function)
    {
        var key = (identity, schemaName, name);
        if (!store.TryGetValue(key, out var list))
        {
            list = [];
            store[key] = list;
        }

        list.Add(function);
    }

    /// <summary>Every candidate registered for <c>(identity-or-fallback, schemaName, name)</c> —
    /// the attach-specific bucket if it has ANY registrations for this key, else the default
    /// bucket. <see langword="null"/> when neither bucket has one.</summary>
    private List<T>? CandidatesFor<T>(
        Dictionary<(string Identity, string SchemaName, string Name), List<T>> store,
        string identity, string schemaName, string name)
    {
        if (identity != DefaultIdentity && store.TryGetValue((identity, schemaName, name), out var direct) && direct.Count > 0)
        {
            return direct;
        }

        return identity == DefaultIdentity || FallsBackToDefault(identity)
            ? store.GetValueOrDefault((DefaultIdentity, schemaName, name))
            : null;
    }

    /// <summary>Resolves a scalar function for a bind/init call — <paramref name="constArguments"/>/
    /// <paramref name="paramSchema"/> disambiguate a multi-overload name; see
    /// <see cref="OverloadResolver.SelectScalar{T}"/> for why two separate wire sources are needed.</summary>
    public IScalarFunction? FindScalar(string identity, string schemaName, string name, byte[] constArguments, Schema? paramSchema)
    {
        var candidates = CandidatesFor(_scalarFunctions, identity, schemaName, name);
        return candidates is null ? null : OverloadResolver.SelectScalar(candidates, f => f.ArgumentsSchema, constArguments, paramSchema, name);
    }

    /// <summary>Every scalar function (every overload) visible under the given attach identity —
    /// its own identity-specific registrations if any exist for a given name, else every
    /// default-bucket one for that name.</summary>
    public IReadOnlyCollection<IScalarFunction> ScalarFunctionsFor(string identity) => Flatten(_scalarFunctions, identity);

    /// <summary>Resolves a table function for a bind call — every table-function argument is a
    /// bind-time constant, so <paramref name="arguments"/> (the DECODED values, not a wire type
    /// schema — table calls carry no <c>InputSchema</c> at all) disambiguates a multi-overload
    /// name; see <see cref="OverloadResolver.SelectTable{T}"/>.</summary>
    public ITableFunction? FindTable(string identity, string schemaName, string name, TableArguments? arguments = null)
    {
        var candidates = CandidatesFor(_tableFunctions, identity, schemaName, name);
        return candidates is null ? null : OverloadResolver.SelectTable(candidates, f => f.ArgumentsSchema, arguments ?? TableArgCodec.Decode(null), name);
    }

    /// <summary>Every table function (every overload) visible under the given attach identity —
    /// same rule as <see cref="ScalarFunctionsFor"/>.</summary>
    public IReadOnlyCollection<ITableFunction> TableFunctionsFor(string identity) => Flatten(_tableFunctions, identity);

    /// <summary>Resolves a table-in-out function for a bind call — same identity-fallback rule as
    /// <see cref="FindScalar"/>. A name with more than one candidate is disambiguated by
    /// <see cref="OverloadResolver.SelectTableInOut{T}"/> against <paramref name="inputSchema"/>
    /// (the "blended" arity-overload case — e.g. <c>geo_encode.test</c>'s 2-arg vs 3-arg
    /// registrations); <paramref name="inputSchema"/> may be <see langword="null"/> when the caller
    /// has no candidate call to disambiguate against yet (fine as long as the name has exactly one
    /// registered candidate).</summary>
    public ITableInOutFunction? FindTableInOut(string identity, string schemaName, string name, Apache.Arrow.Schema? inputSchema = null)
    {
        var candidates = CandidatesFor(_tableInOutFunctions, identity, schemaName, name);
        return candidates is null ? null : OverloadResolver.SelectTableInOut(candidates, f => f.ArgumentsSchema, inputSchema, name);
    }

    /// <summary>Every table-in-out function (every overload) visible under the given attach
    /// identity — same rule as <see cref="ScalarFunctionsFor"/>.</summary>
    public IReadOnlyCollection<ITableInOutFunction> TableInOutFunctionsFor(string identity) => Flatten(_tableInOutFunctions, identity);

    /// <summary>Resolves a table-buffering function for a bind call — see <see cref="FindTableInOut"/>'s
    /// doc comment (no overload fixture exists for this function kind).</summary>
    public ITableBufferingFunction? FindTableBuffering(string identity, string schemaName, string name)
    {
        var candidates = CandidatesFor(_tableBufferingFunctions, identity, schemaName, name);
        return candidates is null ? null : RequireSingle(candidates, name);
    }

    /// <summary>Every table-buffering function (every overload) visible under the given attach
    /// identity — same rule as <see cref="ScalarFunctionsFor"/>.</summary>
    public IReadOnlyCollection<ITableBufferingFunction> TableBufferingFunctionsFor(string identity) => Flatten(_tableBufferingFunctions, identity);

    /// <summary>Resolves an aggregate function for a bind call — see <see cref="FindTableInOut"/>'s
    /// doc comment (no overload fixture exists for this function kind).</summary>
    public IAggregateFunction? FindAggregate(string identity, string schemaName, string name)
    {
        var candidates = CandidatesFor(_aggregateFunctions, identity, schemaName, name);
        return candidates is null ? null : RequireSingle(candidates, name);
    }

    private static T RequireSingle<T>(List<T> candidates, string name) => candidates.Count switch
    {
        1 => candidates[0],
        0 => throw new InvalidOperationException($"'{name}': no candidates registered (unreachable — caller already checked null)."),
        _ => throw new NotSupportedException($"'{name}' has {candidates.Count} overloads, but this function kind's registry lookup doesn't support disambiguating them yet."),
    };

    /// <summary>Every aggregate function (every overload) visible under the given attach identity —
    /// same rule as <see cref="ScalarFunctionsFor"/>.</summary>
    public IReadOnlyCollection<IAggregateFunction> AggregateFunctionsFor(string identity) => Flatten(_aggregateFunctions, identity);

    /// <summary>Merges the default bucket with the identity-specific bucket (which wins per-NAME,
    /// not per-registration, on a collision) then flattens every remaining <c>(schemaName, name)</c>
    /// group's full candidate list — the generalized, list-aware form of the override rule every
    /// <c>*FunctionsFor</c> method above shares.</summary>
    private IReadOnlyCollection<T> Flatten<T>(
        Dictionary<(string Identity, string SchemaName, string Name), List<T>> store, string identity)
    {
        var byKey = new Dictionary<(string SchemaName, string Name), List<T>>();
        if (identity == DefaultIdentity || FallsBackToDefault(identity))
        {
            foreach (var (key, list) in store)
            {
                if (key.Identity != DefaultIdentity)
                {
                    continue;
                }

                byKey[(key.SchemaName, key.Name)] = list;
            }
        }

        if (identity != DefaultIdentity)
        {
            foreach (var (key, list) in store)
            {
                if (key.Identity != identity)
                {
                    continue;
                }

                byKey[(key.SchemaName, key.Name)] = list;
            }
        }

        return byKey.Values.SelectMany(list => list).ToList();
    }

    /// <summary>Every schema name with at least one registered scalar, table, table-in-out,
    /// table-buffering, OR aggregate function, real catalog table, real catalog view, or explicit
    /// <see cref="RegisterSchema"/> call, visible under the given attach identity.</summary>
    public IReadOnlyCollection<string> SchemaNamesFor(string identity)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var function in ScalarFunctionsFor(identity))
        {
            names.Add(function.SchemaName);
        }

        foreach (var function in TableFunctionsFor(identity))
        {
            names.Add(function.SchemaName);
        }

        foreach (var function in TableInOutFunctionsFor(identity))
        {
            names.Add(function.SchemaName);
        }

        foreach (var function in TableBufferingFunctionsFor(identity))
        {
            names.Add(function.SchemaName);
        }

        foreach (var function in AggregateFunctionsFor(identity))
        {
            names.Add(function.SchemaName);
        }

        foreach (var table in CatalogTablesFor(identity))
        {
            names.Add(table.SchemaName);
        }

        foreach (var view in CatalogViewsFor(identity))
        {
            names.Add(view.SchemaName);
        }

        foreach (var macro in CatalogMacrosFor(identity))
        {
            names.Add(macro.SchemaName);
        }

        foreach (var key in _schemas.Keys)
        {
            if (key.Identity == identity || (key.Identity == DefaultIdentity && FallsBackToDefault(identity)))
            {
                names.Add(key.SchemaName);
            }
        }

        if (names.Count == 0)
        {
            names.Add(DefaultSchema);
        }

        return names;
    }

    /// <summary>Declares a schema's comment/tags explicitly — optional; a schema implied purely by
    /// its tables'/functions' <c>SchemaName</c> reports no comment and no tags without this.</summary>
    public void RegisterSchema(string schemaName, string? comment = null, Dictionary<string, string>? tags = null, string identity = DefaultIdentity) =>
        _schemas[(identity, schemaName)] = (comment, tags ?? []);

    public (string? Comment, Dictionary<string, string> Tags) SchemaMetadataFor(string identity, string schemaName)
    {
        if (_schemas.TryGetValue((identity, schemaName), out var direct))
        {
            return direct;
        }

        return FallsBackToDefault(identity) && _schemas.TryGetValue((DefaultIdentity, schemaName), out var fallback)
            ? fallback
            : (null, []);
    }

    /// <summary>Registers a real catalog table — also registers its <see cref="CatalogTable.ScanFunction"/>/
    /// <see cref="CatalogTable.InsertFunction"/>/<see cref="CatalogTable.UpdateFunction"/>/
    /// <see cref="CatalogTable.DeleteFunction"/> (whichever are non-null) as ordinary functions under
    /// the SAME identity, so the C++ side can resolve them by name once it decodes this table's
    /// inline <see cref="Protocol.TableInfo.ScanFunction"/>/<c>*_function</c> fields.
    ///
    /// <see cref="CatalogTable.ScanFunction"/> is deliberately deduped by REFERENCE (not by name):
    /// two DIFFERENT catalog tables sharing the exact same <see cref="ITableFunction"/> INSTANCE
    /// (e.g. <c>rff_or</c> reusing <c>rff_simple</c>'s scan function verbatim, mirroring the
    /// reference Python/Go workers' "rff_or reuses rff_simple_scan so it adds no function" fixture
    /// note — see <c>table/function_registration.test</c>'s hardcoded function-count inventory)
    /// register it only ONCE, not once per table. This is pure reference-identity comparison — two
    /// DISTINCT objects that happen to share a name (the overload-testing pattern) are untouched.</summary>
    public void RegisterCatalogTable(CatalogTable table, string identity = DefaultIdentity)
    {
        _tables[(identity, table.SchemaName, table.Name)] = table;

        if (table.ScanFunction is { } scan && !AlreadyRegisteredByReference(_tableFunctions, identity, scan.SchemaName, scan.Name, scan))
        {
            RegisterTable(scan, identity);
        }

        if (table.InsertFunction is { } insert)
        {
            RegisterTableInOut(insert, identity);
        }

        if (table.UpdateFunction is { } update)
        {
            RegisterTableInOut(update, identity);
        }

        if (table.DeleteFunction is { } delete)
        {
            RegisterTableInOut(delete, identity);
        }
    }

    /// <summary>Whether <paramref name="function"/> (compared by REFERENCE, not value) is already
    /// present in <paramref name="store"/>'s bucket for <c>(identity, schemaName, name)</c> — see
    /// <see cref="RegisterCatalogTable"/>'s doc comment.</summary>
    private static bool AlreadyRegisteredByReference<T>(
        Dictionary<(string Identity, string SchemaName, string Name), List<T>> store,
        string identity, string schemaName, string name, T function) =>
        store.TryGetValue((identity, schemaName, name), out var existing) && existing.Contains(function);

    public CatalogTable? FindCatalogTable(string identity, string schemaName, string name)
    {
        if (_tables.TryGetValue((identity, schemaName, name), out var direct))
        {
            return direct;
        }

        return FallsBackToDefault(identity)
            ? _tables.GetValueOrDefault((DefaultIdentity, schemaName, name))
            : null;
    }

    /// <summary>Every real catalog table visible under the given attach identity — same
    /// default-bucket-plus-identity-override rule as <see cref="ScalarFunctionsFor"/>.</summary>
    public IReadOnlyCollection<CatalogTable> CatalogTablesFor(string identity)
    {
        var byKey = new Dictionary<(string SchemaName, string Name), CatalogTable>();
        if (identity == DefaultIdentity || FallsBackToDefault(identity))
        {
            foreach (var (key, table) in _tables)
            {
                if (key.Identity != DefaultIdentity)
                {
                    continue;
                }

                byKey[(key.SchemaName, key.Name)] = table;
            }
        }

        if (identity != DefaultIdentity)
        {
            foreach (var (key, table) in _tables)
            {
                if (key.Identity != identity)
                {
                    continue;
                }

                byKey[(key.SchemaName, key.Name)] = table;
            }
        }

        return byKey.Values;
    }

    public void RegisterView(CatalogView view, string identity = DefaultIdentity) =>
        _views[(identity, view.SchemaName, view.Name)] = view;

    public CatalogView? FindView(string identity, string schemaName, string name)
    {
        if (_views.TryGetValue((identity, schemaName, name), out var direct))
        {
            return direct;
        }

        return FallsBackToDefault(identity)
            ? _views.GetValueOrDefault((DefaultIdentity, schemaName, name))
            : null;
    }

    /// <summary>Every real catalog view visible under the given attach identity — same
    /// default-bucket-plus-identity-override rule as <see cref="ScalarFunctionsFor"/>.</summary>
    public IReadOnlyCollection<CatalogView> CatalogViewsFor(string identity)
    {
        var byKey = new Dictionary<(string SchemaName, string Name), CatalogView>();
        if (identity == DefaultIdentity || FallsBackToDefault(identity))
        {
            foreach (var (key, view) in _views)
            {
                if (key.Identity != DefaultIdentity)
                {
                    continue;
                }

                byKey[(key.SchemaName, key.Name)] = view;
            }
        }

        if (identity != DefaultIdentity)
        {
            foreach (var (key, view) in _views)
            {
                if (key.Identity != identity)
                {
                    continue;
                }

                byKey[(key.SchemaName, key.Name)] = view;
            }
        }

        return byKey.Values;
    }

    public void RegisterMacro(CatalogMacro macro, string identity = DefaultIdentity) =>
        _macros[(identity, macro.SchemaName, macro.Name)] = macro;

    public CatalogMacro? FindMacro(string identity, string schemaName, string name)
    {
        if (_macros.TryGetValue((identity, schemaName, name), out var direct))
        {
            return direct;
        }

        return FallsBackToDefault(identity)
            ? _macros.GetValueOrDefault((DefaultIdentity, schemaName, name))
            : null;
    }

    /// <summary>Every real catalog macro visible under the given attach identity — same
    /// default-bucket-plus-identity-override rule as <see cref="ScalarFunctionsFor"/>.</summary>
    public IReadOnlyCollection<CatalogMacro> CatalogMacrosFor(string identity)
    {
        var byKey = new Dictionary<(string SchemaName, string Name), CatalogMacro>();
        if (identity == DefaultIdentity || FallsBackToDefault(identity))
        {
            foreach (var (key, macro) in _macros)
            {
                if (key.Identity != DefaultIdentity)
                {
                    continue;
                }

                byKey[(key.SchemaName, key.Name)] = macro;
            }
        }

        if (identity != DefaultIdentity)
        {
            foreach (var (key, macro) in _macros)
            {
                if (key.Identity != identity)
                {
                    continue;
                }

                byKey[(key.SchemaName, key.Name)] = macro;
            }
        }

        return byKey.Values;
    }

    public void RegisterCopyFormat(CopyFormat format, string identity = DefaultIdentity) =>
        _copyFormats[(identity, format.FormatName)] = format;

    /// <summary>Every registered COPY TO/FROM format visible under the given attach identity —
    /// same default-bucket-plus-identity-override rule as <see cref="ScalarFunctionsFor"/>. Copy
    /// formats are catalog-level (not schema-scoped), so this is keyed purely by format name.</summary>
    public IReadOnlyCollection<CopyFormat> CopyFormatsFor(string identity)
    {
        var byKey = new Dictionary<string, CopyFormat>(StringComparer.Ordinal);
        if (identity == DefaultIdentity || FallsBackToDefault(identity))
        {
            foreach (var (key, format) in _copyFormats)
            {
                if (key.Identity != DefaultIdentity)
                {
                    continue;
                }

                byKey[key.FormatName] = format;
            }
        }

        if (identity != DefaultIdentity)
        {
            foreach (var (key, format) in _copyFormats)
            {
                if (key.Identity != identity)
                {
                    continue;
                }

                byKey[key.FormatName] = format;
            }
        }

        return byKey.Values;
    }
}
