namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Decodes the <c>write_options</c> NAMED argument the C++ extension attaches to every
/// INSERT/UPDATE/DELETE delegate call (<c>BuildWriteOptions</c> in <c>vgi_physical_write.cpp</c>) —
/// tells a writable-table's <see cref="TableInOut.ITableInOutFunction"/> which exact result shape
/// to return. Protocol 3.3: the batch carries only <c>result_mode</c>; the former
/// <c>on_conflict</c> / <c>on_conflict_columns</c> fields are gone (DuckDB rewrites every
/// <c>INSERT ... ON CONFLICT</c> into a MERGE before the catalog sees it, so they could only ever
/// say <c>throw</c> / empty).
/// </summary>
public sealed class WriteOptions
{
    public string ResultMode { get; set; } = "count";

    /// <summary>Decodes the <c>write_options</c> named argument off a table-in-out bind call's
    /// <see cref="Table.TableArguments"/> — returns the <c>count</c> default when the argument is
    /// absent (shouldn't normally happen for a real write call, but keeps a unit test or a
    /// hand-rolled direct call from needing to fabricate one). Fields this version does not know
    /// (a pre-3.3 peer's <c>on_conflict</c>) are ignored.</summary>
    public static WriteOptions Decode(Arguments.TableArguments arguments)
    {
        if (arguments.Named("write_options") is not byte[] bytes || bytes.Length == 0)
        {
            return new WriteOptions();
        }

        return EmbeddedIpc.Decode<WriteOptions>(bytes);
    }
}
