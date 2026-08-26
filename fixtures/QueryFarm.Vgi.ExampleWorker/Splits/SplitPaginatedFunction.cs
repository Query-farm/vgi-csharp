using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;

namespace QueryFarm.Vgi.ExampleWorker.Splits;

/// <summary>
/// <c>split_paginated(n, splits)</c> — enumerates its splits across several <c>plan()</c> pages
/// (a fixed <see cref="PageSize"/> splits per call, regardless of anything the client asks for) so
/// the client's cursor-follow loop is genuinely exercised: <c>cursor_pagination.test</c>'s "every
/// split exactly once across N pages" claim, and the control <c>plan_bounds.test</c> needs before
/// its page-cap assertion means anything.
/// </summary>
public sealed class SplitPaginatedFunction : ITableFunction
{
    private const int PageSize = 4;

    public string Name => "split_paginated";

    public string Description => "Split scan whose plan is enumerated across several cursor-following pages";

    public bool SupportsSplits => true;

    public int? MaxWorkers => 8;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Named("n", Int64Type.Default), TableArgFields.Named("splits", Int64Type.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public PlanResult Plan(TableBindParams bindParams, PlanRequest request)
    {
        var n = bindParams.Arguments.Int64Named("n", 0);
        var splits = bindParams.Arguments.Int64Named("splits", 1);
        var allRanges = SplitRanges.Even(n, splits);

        var alreadyEmitted = SplitCursorCodec.Decode(request.Cursor);
        var page = allRanges.Skip((int)alreadyEmitted).Take(PageSize).ToList();
        var scanSplits = page
            .Select((r, i) => ScanSplit.Of(SplitPayloadCodec.Encode(alreadyEmitted + i, r.Start, r.End)))
            .ToList();
        var nowEmitted = alreadyEmitted + page.Count;

        return new PlanResult
        {
            Splits = scanSplits,
            EstimatedTotalSplits = allRanges.Count,
            NextCursors = nowEmitted < allRanges.Count ? [SplitCursorCodec.Encode(nowEmitted)] : null,
        };
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var payloads = SplitOnlyGuard.RequireSingle(initParams, Name);
        var (_, start, end) = SplitPayloadCodec.Decode(payloads[0]);
        return new RangeProducer(start, end, initParams.OutputSchema);
    }
}

/// <summary>
/// <c>split_endless_cursor(n, splits)</c> — a worker that hands back one split and a fresh cursor
/// on EVERY page, forever. <c>plan_bounds.test</c>'s proof that the client's page cap throws
/// rather than truncating a partial enumeration into a silent subset; never actually redeemed (the
/// client always hits the cap first), so <see cref="CreateProducer"/> exists only for completeness.
/// </summary>
public sealed class SplitEndlessCursorFunction : ITableFunction
{
    public string Name => "split_endless_cursor";

    public string Description => "A worker that paginates forever — proves the client's scan-planning page cap";

    public bool SupportsSplits => true;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Named("n", Int64Type.Default), TableArgFields.Named("splits", Int64Type.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public PlanResult Plan(TableBindParams bindParams, PlanRequest request) => new()
    {
        Splits = [ScanSplit.Of(SplitPayloadCodec.Encode(0, 0, 0))],
        NextCursors = [Guid.NewGuid().ToByteArray()],
    };

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var payloads = SplitOnlyGuard.RequireSingle(initParams, Name);
        var (_, start, end) = SplitPayloadCodec.Decode(payloads[0]);
        return new RangeProducer(start, end, initParams.OutputSchema);
    }
}
