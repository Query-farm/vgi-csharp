using Apache.Arrow;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The one place every served <see cref="TableInfo"/> passes through — declarative
/// (<see cref="CatalogTable"/>), time-travel versions, in-memory tables and raw
/// <c>catalog_contents</c> snapshots alike — so each path serves what the DuckDB extension accepts
/// at ATTACH: legacy generated metadata translated, constraint records checked and Arrow
/// nullability derived from them
/// (<see cref="ConstraintRecord.ValidateTable(IReadOnlyList{string}, string, Schema, IReadOnlyList{byte[]}, Func{IReadOnlyList{string}, string, IReadOnlyList{byte[]}?}?)"/>),
/// and generated columns checked (<see cref="ColumnExpressions.Validate"/>).
/// </summary>
internal static class TableServing
{
    /// <summary>A lookup of another served table's constraint records, for FOREIGN KEY targets.</summary>
    internal delegate IReadOnlyList<byte[]>? ConstraintLookup(IReadOnlyList<string> schemaPath, string table);

    internal static Schema ServedColumns(
        IReadOnlyList<string> schemaPath, string table, Schema columns, IReadOnlyList<byte[]> constraints, ConstraintLookup? lookup)
    {
        try
        {
            columns = ColumnExpressions.TranslateLegacy(columns, schemaPath);
        }
        catch (ArgumentException error)
        {
            throw new InvalidOperationException($"Table '{string.Join('.', schemaPath)}.{table}': {error.Message}", error);
        }

        var served = ConstraintRecord.ValidateTable(schemaPath, table, columns, constraints,
            lookup is null ? null : (path, name) => lookup(path, name));
        try
        {
            ColumnExpressions.Validate(served, schemaPath);
        }
        catch (ArgumentException error)
        {
            throw new InvalidOperationException($"Table '{string.Join('.', schemaPath)}.{table}': {error.Message}", error);
        }

        return served;
    }

    /// <summary><see cref="ServedColumns"/> over schema IPC: returns <paramref name="columns"/>
    /// byte-for-byte when serving changes nothing.</summary>
    internal static byte[] ServedColumnsIpc(
        IReadOnlyList<string> schemaPath, string table, byte[] columns, IReadOnlyList<byte[]> constraints, ConstraintLookup? lookup)
    {
        var declared = SchemaIpc.ReadSchemaOnly(columns);
        var served = SchemaIpc.WriteSchemaOnly(ServedColumns(schemaPath, table, declared, constraints, lookup));
        return served.AsSpan().SequenceEqual(SchemaIpc.WriteSchemaOnly(declared)) ? columns : served;
    }

    /// <summary>Normalizes one encoded <see cref="TableInfo"/>; returns <paramref name="item"/> itself
    /// when its columns are already served as-is.</summary>
    internal static byte[] Normalize(byte[] item, ConstraintLookup? lookup)
    {
        var info = EmbeddedIpc.Decode<TableInfo>(item);
        if (info.Columns.Length == 0)
            return item;
        var served = ServedColumnsIpc(info.SchemaPath, info.Name, info.Columns, info.Constraints, lookup);
        if (ReferenceEquals(served, info.Columns))
            return item;
        info.Columns = served;
        return EmbeddedIpc.Encode(info);
    }

    /// <summary>Normalizes every table of a <c>catalog_contents</c> snapshot, checking FOREIGN KEY
    /// targets against the snapshot's own tables. Returns new <see cref="SchemaContents"/> (the
    /// caller's are not modified).</summary>
    internal static List<SchemaContents> Normalize(IReadOnlyList<SchemaContents> schemas)
    {
        if (schemas.All(schema => schema.Tables.Count == 0))
            return [.. schemas];
        var decoded = schemas.SelectMany(schema => schema.Tables).Select(EmbeddedIpc.Decode<TableInfo>).ToList();
        var byName = new Dictionary<(string, string), IReadOnlyList<byte[]>>();
        foreach (var info in decoded)
            byName[(CatalogRegistry.PathKey(info.SchemaPath), info.Name)] = info.Constraints;
        IReadOnlyList<byte[]>? Lookup(IReadOnlyList<string> path, string name) =>
            byName.GetValueOrDefault((CatalogRegistry.PathKey(path), name));
        return schemas.Select(schema => schema.Tables.Count == 0 ? schema : new SchemaContents
        {
            Path = schema.Path,
            Schema = schema.Schema,
            Tables = schema.Tables.Select(item => Normalize(item, Lookup)).ToList(),
            Views = schema.Views,
            ScalarFunctions = schema.ScalarFunctions,
            AggregateFunctions = schema.AggregateFunctions,
            TableFunctions = schema.TableFunctions,
            ScalarMacros = schema.ScalarMacros,
            TableMacros = schema.TableMacros,
            Indexes = schema.Indexes,
        }).ToList();
    }
}
