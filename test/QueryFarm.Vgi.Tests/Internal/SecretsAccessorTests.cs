using QueryFarm.Vgi.Internal;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// Exercises <see cref="SecretsAccessor"/>'s two-phase dynamic-secret-lookup protocol: the FIRST bind
/// attempt (<c>isRetry: false</c>) always registers a pending lookup and returns <c>null</c> from
/// <see cref="SecretsAccessor.Get"/>, regardless of what (if anything) is already resolved; a RETRY
/// (<c>isRetry: true</c>) never registers a new pending lookup, and reads whatever was resolved.
/// </summary>
public class SecretsAccessorTests
{
    [Fact]
    public void FirstAttempt_Get_AlwaysRegistersPendingAndReturnsNull()
    {
        var accessor = new SecretsAccessor(secretsBytes: null, isRetry: false);

        var result = accessor.Get("vgi_example");

        Assert.Null(result);
        Assert.True(accessor.NeedsResolution);
        var lookup = Assert.Single(accessor.PendingLookups);
        Assert.Equal("vgi_example", lookup.SecretType);
        Assert.Null(lookup.Scope);
        Assert.Null(lookup.SecretName);
    }

    [Fact]
    public void FirstAttempt_ScopeAndNameArePassedThroughToThePendingLookup()
    {
        var accessor = new SecretsAccessor(secretsBytes: null, isRetry: false);

        accessor.Get("vgi_example", scope: "s3://bucket-a/", name: "my_secret");

        var lookup = Assert.Single(accessor.PendingLookups);
        Assert.Equal("vgi_example", lookup.SecretType);
        Assert.Equal("s3://bucket-a/", lookup.Scope);
        Assert.Equal("my_secret", lookup.SecretName);
    }

    [Fact]
    public void FirstAttempt_MultipleGetCallsRegisterMultiplePendingLookups()
    {
        var accessor = new SecretsAccessor(secretsBytes: null, isRetry: false);

        accessor.Get("vgi_example", scope: "s3://bucket-a/");
        accessor.Get("vgi_example", scope: "s3://bucket-b/");

        Assert.Equal(2, accessor.PendingLookups.Count);
        Assert.True(accessor.NeedsResolution);
    }

    [Fact]
    public void Retry_NeverRegistersPending_EvenWhenNothingResolved()
    {
        // is_retry=true with no secrets bytes at all — the "genuinely no matching secret" case
        // (secret_no_secret.test): must NOT loop back into a second scope request.
        var accessor = new SecretsAccessor(secretsBytes: null, isRetry: true);

        var result = accessor.Get("vgi_example");

        Assert.Null(result);
        Assert.False(accessor.NeedsResolution);
        Assert.Empty(accessor.PendingLookups);
    }

    [Fact]
    public void Resolved_IsEmptyWhenNoSecretsBytesGiven()
    {
        var accessor = new SecretsAccessor(secretsBytes: null, isRetry: true);
        Assert.Empty(accessor.Resolved);
    }
}
