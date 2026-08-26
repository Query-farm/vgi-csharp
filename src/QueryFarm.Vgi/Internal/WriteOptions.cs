namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Decodes the <c>write_options</c> NAMED argument the C++ extension attaches to every
/// INSERT/UPDATE/DELETE delegate call (<c>BuildWriteOptions</c> in <c>vgi_physical_write.cpp</c>) —
/// tells a writable-table's <see cref="TableInOut.ITableInOutFunction"/> whether the caller wants
/// RETURNING rows back (<see cref="ReturnChunks"/>) and the requested <c>ON CONFLICT</c> behavior.
/// Property order matches <c>BuildWriteOptions</c>'s schema: return_chunks, on_conflict,
/// on_conflict_columns.
/// </summary>
public sealed class WriteOptions
{
    public bool ReturnChunks { get; set; }

    /// <summary>Either <c>"throw"</c> or <c>"nothing"</c>.</summary>
    public string OnConflict { get; set; } = "throw";

    public List<string> OnConflictColumns { get; set; } = [];

    /// <summary>Decodes the <c>write_options</c> named argument off a table-in-out bind call's
    /// <see cref="Table.TableArguments"/> — returns the "no RETURNING, THROW on conflict" default
    /// when the argument is absent (shouldn't normally happen for a real write call, but keeps a
    /// unit test or a hand-rolled direct call from needing to fabricate one).</summary>
    public static WriteOptions Decode(TableArguments arguments)
    {
        if (arguments.Named("write_options") is not byte[] bytes || bytes.Length == 0)
        {
            return new WriteOptions();
        }

        return EmbeddedIpc.Decode<WriteOptions>(bytes);
    }
}
