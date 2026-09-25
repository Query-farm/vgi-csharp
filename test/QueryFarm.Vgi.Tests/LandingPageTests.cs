using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using QueryFarm.Vgi.Http;
using QueryFarm.VgiRpc.Http;
using Xunit;

namespace QueryFarm.Vgi.Tests;

public sealed class LandingPageTests
{
    [Theory]
    [InlineData("")]
    [InlineData("/vgi")]
    public async Task AssetsRevalidateAndRespectAuthentication(string prefix)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        await using var app = builder.Build();
        var reject = false;
        app.MapVgiLandingPage("demo", "server-id", prefix, authenticate: _ =>
            reject ? throw new AuthFailure(AuthReason.MissingCredential) : Task.CompletedTask);
        await app.StartAsync();
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(app.Urls.Single() + prefix + "/") };
            var page = await client.GetStringAsync("");
            Assert.Contains("vgi-landing-asset v5", page);
            Assert.Contains("vgi-client.js?v=5", page);
            using var status = JsonDocument.Parse(await client.GetStringAsync("?format=json"));
            Assert.Equal("csharp", status.RootElement.GetProperty("lang").GetString());
            Assert.Equal("server-id", status.RootElement.GetProperty("server_id").GetString());
            foreach (var path in new[] { "", "vgi-client.js?v=5" })
            {
                using var asset = await client.GetAsync(path);
                Assert.Equal(HttpStatusCode.OK, asset.StatusCode);
                Assert.True(asset.Headers.CacheControl!.NoCache);
                using var conditional = new HttpRequestMessage(HttpMethod.Get, path);
                conditional.Headers.TryAddWithoutValidation("If-None-Match", "W/" + asset.Headers.ETag);
                using var cached = await client.SendAsync(conditional);
                Assert.Equal(HttpStatusCode.NotModified, cached.StatusCode);
                Assert.Empty(await cached.Content.ReadAsByteArrayAsync());
                using var head = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, path));
                Assert.Equal(HttpStatusCode.OK, head.StatusCode);
                Assert.Empty(await head.Content.ReadAsByteArrayAsync());
            }
            reject = true;
            using var refusedPage = await client.GetAsync("");
            using var refusedBundle = await client.GetAsync("vgi-client.js");
            Assert.Equal(HttpStatusCode.Unauthorized, refusedPage.StatusCode);
            Assert.Equal(HttpStatusCode.Unauthorized, refusedBundle.StatusCode);
        }
        finally { await app.StopAsync(); }
    }
}
