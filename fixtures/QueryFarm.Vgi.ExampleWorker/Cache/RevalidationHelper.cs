namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary>
/// Shared conditional-request check for the "always-revalidate" (<c>vgi.cache.ttl=0 +
/// vgi.cache.etag + vgi.cache.revalidatable="1"</c>) fixtures — <c>cache_revalidatable</c>
/// (producer-mode) and <c>cached_reval_echo</c>/<c>cached_reval_double</c> (table-in-out exchange
/// mode). The C++ extension sends <c>vgi.cache.if_none_match</c> on the incoming tick/input-batch
/// metadata (<see cref="QueryFarm.VgiRpc.Streaming.OutputCollector.InputMetadata"/>) once a stored
/// entry is stale-but-revalidatable; a worker whose current ETag still matches replies with a 0-row
/// batch tagged <see cref="CacheMetadata.NotModified"/> instead of recomputing.
/// </summary>
internal static class RevalidationHelper
{
    /// <summary>True when the incoming metadata carries <c>vgi.cache.if_none_match</c> matching
    /// <paramref name="etag"/> — i.e. this turn should reply with a 0-row <c>not_modified</c> batch
    /// instead of real data.</summary>
    public static bool IsNotModified(IReadOnlyDictionary<string, string>? inputMetadata, string etag) =>
        inputMetadata is not null
        && inputMetadata.TryGetValue("vgi.cache.if_none_match", out var ifNoneMatch)
        && ifNoneMatch == etag;
}
