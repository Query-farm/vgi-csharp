using QueryFarm.Vgi.Internal;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// Exercises <see cref="SplitToken"/>'s build/open round trip and its three refusal modes —
/// wrong bind fingerprint, stale consistency anchor (<c>SPLIT_SNAPSHOT_EXPIRED</c>, the one kind
/// meaning "re-run the query" — see <c>splits/expired_token.test</c>), and a malformed/foreign
/// envelope. This worker never seals (stdio has no signing key — see <see cref="SplitToken"/>'s
/// own doc comment), so <see cref="SplitToken.Open"/>'s "flags != 0" refusal is the local stand-in
/// for vgi-java's alg:none downgrade check.
/// </summary>
public class SplitTokenTests
{
    private static readonly byte[] Fingerprint = SplitToken.BindFingerprint("main", "split_sequence", [1, 2, 3], null);
    private static readonly byte[] Anchor = SplitToken.Anchor(1);

    [Fact]
    public void BuildThenOpen_RoundTripsThePayload()
    {
        byte[] payload = [9, 8, 7, 6];

        var token = SplitToken.Build(payload, Fingerprint, Anchor);
        var opened = SplitToken.Open(token, Fingerprint, Anchor);

        Assert.Equal(payload, opened);
    }

    [Fact]
    public void BuildThenOpen_NullPayload_RoundTripsAsEmpty()
    {
        var token = SplitToken.Build(null, Fingerprint, Anchor);
        var opened = SplitToken.Open(token, Fingerprint, Anchor);

        Assert.Empty(opened);
    }

    [Fact]
    public void Open_SkipsFingerprintCheck_WhenExpectedIsNull()
    {
        var token = SplitToken.Build([1], Fingerprint, Anchor);
        var opened = SplitToken.Open(token, expectedFingerprint: null, Anchor);

        Assert.Equal([1], opened);
    }

    [Fact]
    public void Open_SkipsAnchorCheck_WhenCurrentIsNull()
    {
        var token = SplitToken.Build([1], Fingerprint, Anchor);
        var opened = SplitToken.Open(token, Fingerprint, currentAnchor: null);

        Assert.Equal([1], opened);
    }

    [Fact]
    public void Open_MismatchedFingerprint_ThrowsInvalidKind()
    {
        var token = SplitToken.Build([1], Fingerprint, Anchor);
        var otherFingerprint = SplitToken.BindFingerprint("main", "split_many", [1, 2, 3], null);

        var ex = Assert.Throws<SplitToken.SplitTokenException>(() => SplitToken.Open(token, otherFingerprint, Anchor));
        Assert.Equal(SplitToken.KindInvalid, ex.Kind);
    }

    [Fact]
    public void Open_StaleAnchor_ThrowsExpiredKind_MentioningReRunTheQuery()
    {
        var token = SplitToken.Build([1], Fingerprint, Anchor);
        var liveAnchor = SplitToken.Anchor(999);

        var ex = Assert.Throws<SplitToken.SplitTokenException>(() => SplitToken.Open(token, Fingerprint, liveAnchor));

        Assert.Equal(SplitToken.KindExpired, ex.Kind);
        Assert.Contains("re-run the query", ex.Message);
        Assert.Contains(SplitToken.KindExpired, ex.Message);
    }

    [Fact]
    public void Open_TooShort_ThrowsInvalidKind()
    {
        var ex = Assert.Throws<SplitToken.SplitTokenException>(() => SplitToken.Open([1, 2, 3], Fingerprint, Anchor));
        Assert.Equal(SplitToken.KindInvalid, ex.Kind);
    }

    [Fact]
    public void Open_UnsupportedFormatVersion_ThrowsInvalidKind()
    {
        var token = SplitToken.Build([1], Fingerprint, Anchor);
        token[0] = 2; // format_version this worker doesn't speak

        var ex = Assert.Throws<SplitToken.SplitTokenException>(() => SplitToken.Open(token, Fingerprint, Anchor));
        Assert.Equal(SplitToken.KindInvalid, ex.Kind);
    }

    [Fact]
    public void Open_SetFlagBits_ThrowsInvalidKind()
    {
        // This worker never seals, so flags is always 0 in anything it minted — a set bit names a
        // token this process could not have produced (the local stand-in for alg:none).
        var token = SplitToken.Build([1], Fingerprint, Anchor);
        token[1] = 0x01;

        var ex = Assert.Throws<SplitToken.SplitTokenException>(() => SplitToken.Open(token, Fingerprint, Anchor));
        Assert.Equal(SplitToken.KindInvalid, ex.Kind);
    }

    [Fact]
    public void Open_TruncatedAnchor_ThrowsInvalidKind()
    {
        var token = SplitToken.Build([1], Fingerprint, Anchor);
        var truncated = token[..(4 + 16 + 2)]; // header + fingerprint, anchor_len says 8 more bytes that aren't there

        var ex = Assert.Throws<SplitToken.SplitTokenException>(() => SplitToken.Open(truncated, Fingerprint, Anchor));
        Assert.Equal(SplitToken.KindInvalid, ex.Kind);
    }

    [Fact]
    public void BindFingerprint_DiffersByFunctionName()
    {
        var a = SplitToken.BindFingerprint("main", "split_sequence", [1], null);
        var b = SplitToken.BindFingerprint("main", "split_many", [1], null);

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void BindFingerprint_IsSixteenBytes()
    {
        Assert.Equal(16, Fingerprint.Length);
    }

    [Fact]
    public void Anchor_RoundTripsAsLittleEndianEightBytes()
    {
        var anchor = SplitToken.Anchor(1);
        Assert.Equal(8, anchor.Length);
        Assert.Equal(1, anchor[0]);
        Assert.All(anchor[1..], b => Assert.Equal(0, b));
    }
}
