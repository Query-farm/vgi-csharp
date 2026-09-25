using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using QueryFarm.VgiRpc.Http;
using QueryFarm.VgiRpc.Identity;

namespace QueryFarm.Vgi.Http;

/// <summary>The shared, same-origin VGI browser catalog and its bundled client.</summary>
public static class LandingPage
{
    private sealed record Asset(byte[] Bytes, string ContentType)
    {
        public string ETag { get; } = '"' + Convert.ToHexString(SHA256.HashData(Bytes)) + '"';
    }

    private static readonly Lazy<Asset> Html = new(() => Load("landing.html", "text/html; charset=utf-8"));
    private static readonly Lazy<Asset> Client = new(() => Load("vgi-client.js", "text/javascript; charset=utf-8"));

    private static Asset Load(string name, string contentType)
    {
        using var stream = typeof(LandingPage).Assembly.GetManifestResourceStream($"QueryFarm.Vgi.Http.{name}")
            ?? throw new InvalidOperationException($"Missing landing asset: {name}");
        using var bytes = new MemoryStream();
        stream.CopyTo(bytes);
        return new Asset(bytes.ToArray(), contentType);
    }

    /// <summary>Mounts the page, JSON worker identity and browser bundle beneath an RPC prefix.</summary>
    public static void MapVgiLandingPage(this IEndpointRouteBuilder endpoints, string name,
        string serverId, string prefix = "", string doc = "",
        RpcHttpEndpoints.AuthenticateDelegate? authenticate = null)
    {
        prefix = prefix.TrimEnd('/');
        var version = typeof(LandingPage).Assembly.GetName().Version?.ToString(3) ?? "unknown";
        var status = JsonSerializer.SerializeToUtf8Bytes(new
        {
            status = "ok",
            server_id = serverId,
            protocol = "vgi.v2",
            worker = name,
            doc,
            version,
            lang = "csharp",
            oauth = false,
            cupola_base = "https://cupola.query-farm.services"
        });
        async Task Serve(HttpContext context, bool bundle)
        {
            if (authenticate is not null)
            {
                try { await authenticate(context).ConfigureAwait(false); }
                catch (AuthFailure failure)
                {
                    await UnauthorizedResponseWriter.WriteAsync(context, failure.Reason, failure.Detail, null,
                        cancellationToken: context.RequestAborted).ConfigureAwait(false);
                    return;
                }
                catch (PeerIdentityUnavailableException unavailable)
                {
                    context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                    context.Response.Headers.CacheControl = "no-store";
                    context.Response.Headers.RetryAfter = unavailable.RetryAfterSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    return;
                }
                catch (Exception)
                {
                    await UnauthorizedResponseWriter.WriteAsync(context, AuthReason.Unauthorized, "", null,
                        cancellationToken: context.RequestAborted).ConfigureAwait(false);
                    return;
                }
            }
            var accept = context.Request.Headers.Accept.ToString();
            var json = !bundle && (context.Request.Query["format"] == "json"
                || (accept.Contains("application/json", StringComparison.OrdinalIgnoreCase)
                    && !accept.Contains("text/html", StringComparison.OrdinalIgnoreCase)));
            var asset = bundle ? Client.Value : Html.Value;
            context.Response.ContentType = json ? "application/json" : asset.ContentType;
            context.Response.Headers.CacheControl = json ? "no-store" : "private, no-cache";
            if (!json)
            {
                context.Response.Headers.ETag = asset.ETag;
                if (context.Request.Headers.IfNoneMatch.ToString().Split(',')
                    .Any(value => value.Trim().Replace("W/", "", StringComparison.Ordinal) == asset.ETag || value.Trim() == "*"))
                {
                    context.Response.StatusCode = StatusCodes.Status304NotModified;
                    return;
                }
            }
            var bytes = json ? status : asset.Bytes;
            context.Response.ContentLength = bytes.Length;
            if (!HttpMethods.IsHead(context.Request.Method))
                await context.Response.Body.WriteAsync(bytes, context.RequestAborted).ConfigureAwait(false);
        }
        endpoints.MapMethods(prefix + "/", ["GET", "HEAD"], context => Serve(context, false));
        endpoints.MapMethods(prefix + "/vgi-client.js", ["GET", "HEAD"], context => Serve(context, true));
    }
}
