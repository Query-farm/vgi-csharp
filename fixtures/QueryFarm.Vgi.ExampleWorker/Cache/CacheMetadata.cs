namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary>
/// Shared <c>vgi.cache.*</c> custom_metadata builders for the M8 (test/sql/integration/cache/*)
/// fixture surface — see <c>~/Development/vgi/src/include/vgi_cache_control.hpp</c> for the ground
/// truth on every key/value format. Every boolean key parses on the LITERAL string <c>"1"</c> (not
/// <c>"true"</c>) — see <c>ParseVgiCacheControl</c> in <c>vgi_result_cache.cpp</c>.
///
/// <see cref="ITableFunction"/>/<see cref="ITableInOutFunction"/>/<see cref="ITableBufferingFunction"/>
/// have no static <c>CacheControlMetadata</c> convenience property the way <see cref="IScalarFunction"/>
/// does — a producer/processor must build this dictionary itself and pass it to
/// <c>OutputCollector.Emit(batch, metadata)</c>. Only the FIRST data batch of a call is actually
/// parsed for these keys C++-side, but re-sending the same dict on every batch is harmless (and
/// required for the distinct per-batch <c>vgi_partition_values#b64</c> key — see
/// <see cref="Internal.PartitionValuesCodec"/> — which callers merge in separately).
/// </summary>
internal static class CacheMetadata
{
    /// <summary>Plain "cache this for N seconds" advertisement — <c>vgi.cache.ttl</c> is the only key
    /// required for <c>Cacheable()</c> to actually latch (see the header's doc comment on that
    /// gotcha, already discovered by M2's <c>per_value.test</c> fix).</summary>
    public static Dictionary<string, string> Ttl(long seconds) => new() { ["vgi.cache.ttl"] = seconds.ToString() };

    /// <summary>Opt into the C++ extension's per-distinct-VALUE exchange memoization tier (scalar /
    /// batched-correlated-LATERAL / streaming table-in-out maps) — requires a TTL to actually store,
    /// not just latch the opt-in (see <c>PerValueCache</c> in <c>Scalar/CachedScalarFunctions.cs</c>
    /// for the identical scalar-side gotcha).</summary>
    public static Dictionary<string, string> PerValue(long ttlSeconds) => new()
    {
        ["vgi.cache.ttl"] = ttlSeconds.ToString(),
        ["vgi.cache.per_value"] = "1",
    };

    /// <summary>Opt into the per-partition result cache (SINGLE_VALUE_PARTITIONS functions only) —
    /// additive to the whole-scan cache.</summary>
    public static Dictionary<string, string> PartitionScope(long ttlSeconds) => new()
    {
        ["vgi.cache.ttl"] = ttlSeconds.ToString(),
        ["vgi.cache.partition_scope"] = "1",
    };

    /// <summary>Explicit "never cache" advertisement — <c>Cacheable()</c> returns false regardless
    /// of any TTL also present, so a bare <c>no_store</c> (no TTL at all) is enough.</summary>
    public static Dictionary<string, string> NoStore() => new() { ["vgi.cache.no_store"] = "1" };

    /// <summary>The "always-revalidate" contract: immediately-stale (ttl=0) + a stable validator +
    /// opted into conditional revalidation. A worker advertising this MUST also implement the
    /// <c>vgi.cache.if_none_match</c> conditional-request check (see <see cref="RevalidationHelper"/>)
    /// — advertising <c>revalidatable</c> alone is a promise the C++ side takes at face value; failing
    /// to honor it just means every "revalidation" round-trip degrades to a plain re-fetch (still
    /// correct, just not exercising the 304 path the test pins).</summary>
    public static Dictionary<string, string> Revalidatable(string etag) => new()
    {
        ["vgi.cache.ttl"] = "0",
        ["vgi.cache.etag"] = etag,
        ["vgi.cache.revalidatable"] = "1",
    };

    /// <summary>Fold DuckDB's own transaction id into the cache key — purely an advertised string;
    /// the worker needs no transaction-awareness of its own (see <c>transaction_scope.test</c>).</summary>
    public static Dictionary<string, string> TransactionScoped(long ttlSeconds) => new()
    {
        ["vgi.cache.ttl"] = ttlSeconds.ToString(),
        ["vgi.cache.scope"] = "transaction",
    };

    /// <summary>The 304-equivalent reply to a matched <c>vgi.cache.if_none_match</c> — carried on a
    /// 0-row batch (see <see cref="RevalidationHelper"/>).</summary>
    public static Dictionary<string, string> NotModified() => new() { ["vgi.cache.not_modified"] = "1" };
}
