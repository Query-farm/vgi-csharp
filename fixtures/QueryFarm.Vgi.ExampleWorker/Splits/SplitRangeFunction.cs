using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.ExampleWorker.Cache;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;

namespace QueryFarm.Vgi.ExampleWorker.Splits;

/// <summary>
/// A pure range-partitioned split scan over <c>n := 0..(n-1)</c>: <c>plan()</c> divides
/// <c>[0, n)</c> into <c>splits</c> pieces (by default evenly — see <see cref="SplitRanges.Even"/>
/// — overridable via <paramref name="rangesFactory"/>) and each split's <c>init</c> replays its
/// own slice via <see cref="RangeProducer"/>. One flexible class backs eight of the
/// <c>test/sql/integration/splits/*</c> fixtures, which differ only in how ranges are computed or
/// in a couple of plan/catalog knobs:
///
/// <list type="bullet">
/// <item><c>split_sequence</c>/<c>split_many</c> — the plain case (<c>parity.test</c>,
/// <c>more_splits_than_threads.test</c>).</item>
/// <item><c>split_zero</c> — <paramref name="alwaysEmpty"/>: a split-capable plan with zero
/// splits, always (<c>zero_splits.test</c>).</item>
/// <item><c>split_stale_plan</c> — <paramref name="catalogVersionOverride"/>: pins every split's
/// token to a catalog version the live worker will never agree with, so redemption throws
/// <c>SPLIT_SNAPSHOT_EXPIRED</c> (<c>expired_token.test</c>).</item>
/// <item><c>split_short_ttl</c> — <paramref name="splitTokenTtlSecondsOverride"/>: declares a
/// 1-second split-token lifetime via <see cref="ITableFunction.SplitTokenTtlSeconds"/>, refused at
/// PLAN time by the client's TTL floor (<c>ttl_floor.test</c>).</item>
/// <item><c>split_skewed</c> — <paramref name="rangesFactory"/> puts ~99% of the rows in one split
/// (<c>skew.test</c>).</item>
/// <item><c>split_empty_ranges</c> — <paramref name="rangesFactory"/> interleaves zero-row splits
/// with non-empty ones (<c>zero_row_split.test</c>).</item>
/// <item><c>split_cacheable</c> — <paramref name="cacheable"/>: attaches
/// <see cref="CacheMetadata.Ttl"/> to every emitted batch, so the whole scan is a cache candidate
/// like any other (<c>cancel_midsplit.test</c>, <c>cache_interaction.test</c>).</item>
/// </list>
/// </summary>
public sealed class SplitRangeFunction(
    string name,
    string description,
    bool alwaysEmpty = false,
    long? catalogVersionOverride = null,
    long? splitTokenTtlSecondsOverride = null,
    Func<long, long, List<(long Start, long End)>>? rangesFactory = null,
    bool cacheable = false) : ITableFunction
{
    public string Name => name;

    public string Description => description;

    public bool SupportsSplits => true;

    public int? MaxWorkers => 8;

    public long? SplitTokenTtlSeconds => splitTokenTtlSecondsOverride;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Named("n", Int64Type.Default), TableArgFields.Named("splits", Int64Type.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public PlanResult Plan(TableBindParams bindParams, PlanRequest request)
    {
        if (alwaysEmpty)
        {
            return PlanResult.Empty;
        }

        var n = bindParams.Arguments.Int64Named("n", 0);
        var splits = bindParams.Arguments.Int64Named("splits", 1);
        var ranges = (rangesFactory ?? SplitRanges.Even)(n, splits);
        var scanSplits = ranges
            .Select((r, i) => ScanSplit.Of(SplitPayloadCodec.Encode(i, r.Start, r.End)))
            .ToList();

        return new PlanResult
        {
            Splits = scanSplits,
            EstimatedTotalSplits = scanSplits.Count,
            CatalogVersion = catalogVersionOverride,
        };
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var payloads = SplitOnlyGuard.RequireSingle(initParams, name);
        var (_, start, end) = SplitPayloadCodec.Decode(payloads[0]);
        return new RangeProducer(
            start, end, initParams.OutputSchema,
            metadata: cacheable ? () => CacheMetadata.Ttl(60) : null);
    }
}
