namespace QueryFarm.Vgi.Catalog;

/// <summary>
/// One column's declared statistics — the worker-side input to <see cref="Internal.ColumnStatisticsCodec"/>.
/// Mirrors vgi-python's <c>ColumnStatisticsInput</c>: <see cref="Min"/>/<see cref="Max"/> accept a
/// bare CLR scalar (<see cref="long"/>/<see cref="double"/>/<see cref="string"/>/<see cref="bool"/>/
/// <see cref="byte"/>[]) matching the column's own type — <see langword="null"/> means "unknown",
/// not "the column has no rows". Used both by <see cref="CatalogTable.Statistics"/> (fixed,
/// declared-at-registration stats) and any worker code building the
/// <c>table_function_statistics</c>/<c>catalog_table_column_statistics_get</c> RPC response by hand.
/// </summary>
public sealed class ColumnStatisticsInput
{
    public object? Min { get; init; }

    public object? Max { get; init; }

    public bool HasNull { get; init; }

    public bool HasNotNull { get; init; } = true;

    public long? DistinctCount { get; init; }

    /// <summary>String-typed columns only — <see langword="null"/> for every other column type.</summary>
    public bool? ContainsUnicode { get; init; }

    /// <summary>String-typed columns only — <see langword="null"/> for every other column type.</summary>
    public long? MaxStringLength { get; init; }
}
