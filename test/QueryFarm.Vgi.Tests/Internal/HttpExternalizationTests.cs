using QueryFarm.Vgi.Internal;
using QueryFarm.VgiRpc.External;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>The HTTP server's blob-vendor wiring and the <c>VGI-Arguments-Ref-Bytes</c> capability
/// signal (vgi <c>docs/large-values-3.4.md</c> §5): sent exactly when upload URLs v2 and an object
/// store are configured, unless the test switch is set.</summary>
public sealed class HttpExternalizationTests
{
    private static Func<string, string?> Env(params (string Name, string Value)[] values)
    {
        var map = values.ToDictionary(v => v.Name, v => v.Value);
        return name => map.GetValueOrDefault(name);
    }

    private static readonly (string, string)[] s_vendor =
    [
        (BlobVendorEnvironment.Url, "http://127.0.0.1:9"),
        (BlobVendorEnvironment.Token, "test-token"),
        (BlobVendorEnvironment.MaxRequestBytes, "32768"),
    ];

    [Fact]
    public void NoVendorNoHeader()
    {
        Assert.Null(HttpExternalization.FromEnvironment(Env()));
        var capped = HttpExternalization.FromEnvironment(Env((BlobVendorEnvironment.MaxRequestBytes, "32768")));
        Assert.NotNull(capped);
        Assert.Null(capped.CapabilityHeaders);
    }

    [Fact]
    public void AVendorAdvertisesArgumentReferences()
    {
        var options = HttpExternalization.FromEnvironment(Env(s_vendor));
        Assert.NotNull(options);
        Assert.IsType<VendorStorage>(options.UploadUrlProvider);
        Assert.Equal("4194304", options.CapabilityHeaders![HttpExternalization.ArgumentsRefBytesHeader]);

        var tuned = HttpExternalization.FromEnvironment(Env([.. s_vendor, (HttpExternalization.ArgumentsRefBytesVariable, "65536")]));
        Assert.Equal("65536", tuned!.CapabilityHeaders![HttpExternalization.ArgumentsRefBytesHeader]);
    }

    [Fact]
    public void TheTestSwitchDropsOnlyTheHeader()
    {
        var options = HttpExternalization.FromEnvironment(Env([.. s_vendor, (HttpExternalization.DisableArgumentsRefVariable, "1")]));
        Assert.NotNull(options!.UploadUrlProvider);
        Assert.Null(options.CapabilityHeaders);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-5")]
    [InlineData("012")]
    [InlineData("12345678901234567890")]
    [InlineData("4MiB")]
    public void AMalformedOverrideRefusesToStart(string value) =>
        Assert.Throws<ArgumentException>(() => HttpExternalization.ArgumentsRefBytes(Env((HttpExternalization.ArgumentsRefBytesVariable, value))));
}
