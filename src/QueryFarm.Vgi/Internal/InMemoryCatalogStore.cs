using System.Collections.Concurrent;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The DDL-capable in-memory catalogs a <see cref="Worker"/> serves
/// (<see cref="Worker.RegisterInMemoryCatalog"/>), and the private state of every live attach of
/// one. Every ATTACH gets its own empty catalog — one default schema, no tables or views — keyed by
/// that attach's whole <c>attach_opaque_data</c> (which carries a fresh per-attach GUID, see
/// <c>VgiServiceImpl.EncodeIdentity</c>), so two attaches sharing one warm worker never see each
/// other's objects. DETACH drops it. The state lives in this process, so it needs one long-lived
/// worker (<c>launch:</c> or HTTP), like vgi-python's <c>InMemoryCatalog</c>.
/// </summary>
internal sealed class InMemoryCatalogStore
{
    private readonly ConcurrentDictionary<string, InMemoryCatalogOptions> _catalogs = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, InMemoryCatalogState> _attaches = new(StringComparer.Ordinal);

    public void Register(string identity, InMemoryCatalogOptions options) => _catalogs[identity] = options;

    public bool IsInMemory(string identity) => _catalogs.ContainsKey(identity);

    /// <summary>A fresh private catalog for a new attach of <paramref name="identity"/>, or
    /// <see langword="null"/> when that identity is not an in-memory catalog.</summary>
    public InMemoryCatalogState? Attach(string identity, byte[] attachOpaqueData, IReadOnlyList<string> defaultSchemaPath)
    {
        if (!_catalogs.TryGetValue(identity, out var options))
        {
            return null;
        }

        var state = new InMemoryCatalogState(options, defaultSchemaPath);
        _attaches[Key(attachOpaqueData)] = state;
        return state;
    }

    /// <summary>The attach's private catalog; <see langword="null"/> for an attach of any other
    /// catalog.</summary>
    public InMemoryCatalogState? Find(byte[]? attachOpaqueData) =>
        attachOpaqueData is { Length: > 0 } && !_attaches.IsEmpty ? _attaches.GetValueOrDefault(Key(attachOpaqueData)) : null;

    public void Detach(byte[] attachOpaqueData) => _attaches.TryRemove(Key(attachOpaqueData), out _);

    /// <summary>How many attaches currently hold a private catalog (tests).</summary>
    internal int AttachCount => _attaches.Count;

    private static string Key(byte[] attachOpaqueData) => Convert.ToHexString(attachOpaqueData);
}

/// <summary>One attach's private, mutable catalog: schemas holding tables and views, created and
/// dropped by the catalog DDL RPCs. Every successful change moves <see cref="Version"/>; items are
/// encoded once, when created, so every listing serves the same bytes (and a content-hash etag is
/// stable while nothing changes).</summary>
internal sealed class InMemoryCatalogState
{
    private readonly object _lock = new();
    private readonly InMemoryCatalogOptions _options;
    private readonly List<SchemaState> _schemas = [];
    private long _version = 1;

    public InMemoryCatalogState(InMemoryCatalogOptions options, IReadOnlyList<string> defaultSchemaPath)
    {
        _options = options;
        _schemas.Add(new SchemaState([.. defaultSchemaPath], null, []));
    }

    /// <summary>The generation counter: 1 at attach, moved by every successful DDL.</summary>
    public long Version
    {
        get
        {
            lock (_lock)
            {
                return _version;
            }
        }
    }

    /// <summary>What <c>catalog_version</c> answers: <see cref="Version"/>, or always 0 when the
    /// catalog does not report one (<see cref="InMemoryCatalogOptions.ReportsVersion"/>).</summary>
    public long ReportedVersion => _options.ReportsVersion ? Version : 0;

    public sealed record SchemaSnapshot(List<string> Path, string? Comment, Dictionary<string, string> Tags, int Tables, int Views);

    /// <summary>Every schema, in creation order.</summary>
    public List<SchemaSnapshot> Schemas()
    {
        lock (_lock)
        {
            return _schemas.Select(s => s.Snapshot()).ToList();
        }
    }

    public SchemaSnapshot? Schema(IReadOnlyList<string> path)
    {
        lock (_lock)
        {
            return FindSchema(path)?.Snapshot();
        }
    }

    public List<byte[]> Tables(IReadOnlyList<string> path)
    {
        lock (_lock)
        {
            return FindSchema(path)?.Tables.Values.ToList() ?? [];
        }
    }

    public List<byte[]> Views(IReadOnlyList<string> path)
    {
        lock (_lock)
        {
            return FindSchema(path)?.Views.Values.ToList() ?? [];
        }
    }

    public byte[]? Table(IReadOnlyList<string> path, string name)
    {
        lock (_lock)
        {
            return FindSchema(path)?.Tables.GetValueOrDefault(name);
        }
    }

    public byte[]? View(IReadOnlyList<string> path, string name)
    {
        lock (_lock)
        {
            return FindSchema(path)?.Views.GetValueOrDefault(name);
        }
    }

    public void CreateSchema(IReadOnlyList<string> path, OnConflict onConflict, string? comment, Dictionary<string, string>? tags)
    {
        if (path.Count == 0)
        {
            throw new ArgumentException("CREATE SCHEMA needs a non-empty schema path.");
        }

        lock (_lock)
        {
            var existing = FindSchema(path);
            if (existing is not null && !Replaces("Schema", Dotted(path), onConflict))
            {
                return;
            }

            if (path.Count > 1 && FindSchema(path.Take(path.Count - 1).ToList()) is null)
            {
                throw new InvalidOperationException($"Schema with name {Dotted(path.Take(path.Count - 1).ToList())} does not exist!");
            }

            if (existing is not null)
            {
                _schemas.Remove(existing);
            }

            _schemas.Add(new SchemaState([.. path], comment, tags is null ? [] : new Dictionary<string, string>(tags)));
            _version++;
        }
    }

    public void DropSchema(IReadOnlyList<string> path, bool ignoreNotFound, bool cascade)
    {
        lock (_lock)
        {
            var schema = FindSchema(path);
            if (schema is null)
            {
                if (ignoreNotFound)
                {
                    return;
                }

                throw new InvalidOperationException($"Schema with name {Dotted(path)} does not exist!");
            }

            var children = _schemas.Where(s => s.Path.Count > path.Count && CatalogRegistry.PathsEqual(s.Path.Take(path.Count).ToList(), path)).ToList();
            if (!cascade && (schema.Tables.Count > 0 || schema.Views.Count > 0 || children.Count > 0))
            {
                throw new InvalidOperationException($"Schema {Dotted(path)} is not empty; use CASCADE to drop it and its contents.");
            }

            _schemas.Remove(schema);
            foreach (var child in children)
            {
                _schemas.Remove(child);
            }

            _version++;
        }
    }

    public void CreateTable(TableCreateRequest request)
    {
        lock (_lock)
        {
            var schema = RequireSchema(request.SchemaPath);
            if (schema.Tables.ContainsKey(request.Name) && !Replaces("Table", request.Name, request.OnConflict))
            {
                return;
            }

            // Checked before the table is stored: the column schema is what every listing serves.
            SchemaIpc.ReadSchemaOnly(request.Columns);
            schema.Tables[request.Name] = EmbeddedIpc.Encode(new TableInfo
            {
                Name = request.Name,
                SchemaPath = [.. schema.Path],
                Columns = request.Columns,
                NotNullConstraints = [.. request.NotNullConstraints],
                UniqueConstraints = request.UniqueConstraints.Select(group => group.ToList()).ToList(),
                CheckConstraints = [.. request.CheckConstraints],
                PrimaryKeyConstraints = request.PrimaryKeyConstraints.Select(group => group.ToList()).ToList(),
                ForeignKeyConstraints = [.. request.ForeignKeyConstraints],
            });
            _version++;
        }
    }

    public void DropTable(IReadOnlyList<string> path, string name, bool ignoreNotFound)
    {
        lock (_lock)
        {
            if (FindSchema(path)?.Tables.Remove(name) == true)
            {
                _version++;
            }
            else if (!ignoreNotFound)
            {
                throw new InvalidOperationException($"Table with name {name} does not exist!");
            }
        }
    }

    public void CreateView(IReadOnlyList<string> path, string name, string definition, OnConflict onConflict)
    {
        lock (_lock)
        {
            var schema = RequireSchema(path);
            if (schema.Views.ContainsKey(name) && !Replaces("View", name, onConflict))
            {
                return;
            }

            schema.Views[name] = EmbeddedIpc.Encode(new ViewInfo
            {
                Name = name,
                SchemaPath = [.. schema.Path],
                Definition = definition,
            });
            _version++;
        }
    }

    public void DropView(IReadOnlyList<string> path, string name, bool ignoreNotFound)
    {
        lock (_lock)
        {
            if (FindSchema(path)?.Views.Remove(name) == true)
            {
                _version++;
            }
            else if (!ignoreNotFound)
            {
                throw new InvalidOperationException($"View with name {name} does not exist!");
            }
        }
    }

    /// <summary><see langword="true"/> to replace an existing object, <see langword="false"/> to keep
    /// it (IF NOT EXISTS); throws on a plain conflict.</summary>
    private static bool Replaces(string kind, string name, OnConflict onConflict) => onConflict switch
    {
        OnConflict.Ignore => false,
        OnConflict.Replace => true,
        _ => throw new InvalidOperationException($"{kind} with name {name} already exists!"),
    };

    private SchemaState? FindSchema(IReadOnlyList<string> path) =>
        _schemas.FirstOrDefault(s => CatalogRegistry.PathsEqual(s.Path, path));

    private SchemaState RequireSchema(IReadOnlyList<string> path) =>
        FindSchema(path) ?? throw new InvalidOperationException($"Schema with name {Dotted(path)} does not exist!");

    private static string Dotted(IReadOnlyList<string> path) => string.Join('.', path);

    private sealed class SchemaState(List<string> path, string? comment, Dictionary<string, string> tags)
    {
        public List<string> Path { get; } = path;

        public string? Comment { get; } = comment;

        public Dictionary<string, string> Tags { get; } = tags;

        // SortedDictionary: listings come out in name order, independent of creation order.
        public SortedDictionary<string, byte[]> Tables { get; } = new(StringComparer.Ordinal);

        public SortedDictionary<string, byte[]> Views { get; } = new(StringComparer.Ordinal);

        public SchemaSnapshot Snapshot() => new([.. Path], Comment, new Dictionary<string, string>(Tags), Tables.Count, Views.Count);
    }
}
