using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>cached_double_scalar</c>/<c>cached_add_const</c>/<c>cached_label</c> — plain, pure,
/// deterministic scalar functions exercised by <c>dedup.test</c>/<c>per_value.test</c>/
/// <c>per_value_edge.test</c> to prove the C++ extension's input-dedup and per-value result-cache
/// layers give correct answers regardless of caching. Each advertises
/// <c>vgi.cache.per_value = "1"</c> (see <see cref="IScalarFunction.CacheControlMetadata"/>) to opt
/// into the C++ extension's per-distinct-value memoization tier — <c>per_value.test</c>'s
/// zero-worker-call assertions require this; the plain VALUE assertions in the other two tests
/// pass either way.
/// </summary>
public sealed class CachedDoubleScalarFunction : ScalarFn
{
    public override string Name => "cached_double_scalar";

    public override string Description => "Doubles a value (cache-control fixture)";

    public override IReadOnlyDictionary<string, string>? CacheControlMetadata => PerValueCache.Metadata;

    private void Compute([Param] Int64Array x, Int64Array.Builder result)
    {
        for (var i = 0; i < x.Length; i++)
        {
            result.Append(x.IsNull(i) ? null : x.GetValue(i)!.Value * 2);
        }
    }
}

public sealed class CachedAddConstFunction : ScalarFn
{
    public override string Name => "cached_add_const";

    public override string Description => "Adds a constant to a value (cache-control fixture)";

    public override IReadOnlyDictionary<string, string>? CacheControlMetadata => PerValueCache.Metadata;

    private void Compute([Param] Int64Array v, [ConstParam] long addend, Int64Array.Builder result)
    {
        for (var i = 0; i < v.Length; i++)
        {
            result.Append(v.IsNull(i) ? null : v.GetValue(i)!.Value + addend);
        }
    }
}

public sealed class CachedLabelFunction : ScalarFn
{
    public override string Name => "cached_label";

    public override string Description => "Labels non-negative values (cache-control fixture)";

    public override IReadOnlyDictionary<string, string>? CacheControlMetadata => PerValueCache.Metadata;

    private void Compute([Param] Int64Array x, StringArray.Builder result)
    {
        for (var i = 0; i < x.Length; i++)
        {
            if (x.IsNull(i) || x.GetValue(i)!.Value < 0)
            {
                result.AppendNull();
                continue;
            }

            result.Append($"lbl-{x.GetValue(i)!.Value}");
        }
    }
}

/// <summary>Shared <c>vgi.cache.per_value</c>/<c>vgi.cache.ttl</c> advertisement — see
/// <c>~/Development/vgi/src/include/vgi_cache_control.hpp</c>. <c>vgi.cache.per_value</c> requires
/// the literal string <c>"1"</c> (not <c>"true"</c>), per <c>vgi_result_cache.cpp</c>'s parser.
/// <c>vgi.cache.ttl</c> (plain integer seconds) is NOT optional despite the name suggesting a
/// freshness hint: <c>VgiCacheControl::Cacheable()</c> gates the actual per-value STORE step (not
/// just the opt-in latch) on a TTL or an <c>expires</c> timestamp being present — without one, the
/// C++ side arms the opt-in flag but never memoizes a single value, so a later call still has to
/// round-trip the worker once to seed the cache. Set generously (1 hour) since these fixtures are
/// pure/deterministic — a positive value is required, not the actual result's real-world lifetime.</summary>
internal static class PerValueCache
{
    public static readonly IReadOnlyDictionary<string, string> Metadata = new Dictionary<string, string>
    {
        ["vgi.cache.per_value"] = "1",
        ["vgi.cache.ttl"] = "3600",
    };
}
