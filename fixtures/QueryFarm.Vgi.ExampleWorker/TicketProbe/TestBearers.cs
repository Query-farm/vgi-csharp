using Microsoft.AspNetCore.Http;
using QueryFarm.VgiRpc.Http;
using QueryFarm.VgiRpc.Identity;
using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi.ExampleWorker.TicketProbe;

/// <summary>
/// The fixture HTTP server's optional test bearers, mirroring vgi-python's
/// <c>vgi-fixture-http</c> (vgi-attach-tickets.md §7.1). <b>Test-only.</b>
/// </summary>
/// <remarks>
/// <para><c>Bearer vgi-test-alice</c> authenticates as <c>alice</c> and <c>Bearer vgi-test-bob</c> as
/// <c>bob</c> (domain <c>bearer</c>), each as a <em>fresh</em> login: the <c>auth_time</c> claim is
/// stamped now, so a client can <c>issue_grant</c> with nothing but a bearer DuckDB can send.</para>
/// <para>When grant keys are configured, a sealed grant (<c>Bearer vgig1.…</c>) is not this
/// authenticator's: it moves the chain on to the grant authenticator the worker composes after it.
/// Any other bearer, or none, stays anonymous, as before.</para>
/// </remarks>
public static class TestBearers
{
    private static readonly Dictionary<string, string> s_tokens = new(StringComparer.Ordinal)
    {
        ["vgi-test-alice"] = "alice",
        ["vgi-test-bob"] = "bob",
    };

    /// <summary>The authenticator; <paramref name="grantsConfigured"/> says whether a grant
    /// authenticator follows it.</summary>
    public static RpcHttpEndpoints.AuthenticateDelegate Create(bool grantsConfigured) =>
        context => Authenticate(context, grantsConfigured);

    /// <summary>Whether this process was given grant keys (<c>VGI_RPC_GRANT_KEYS</c> or
    /// <c>--grant-key</c>).</summary>
    public static bool GrantsConfigured(string[] args) =>
        args.Contains("--grant-key", StringComparer.Ordinal)
        || !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(GrantKeys.KeysEnvironmentVariable));

    private static Task Authenticate(HttpContext context, bool grantsConfigured)
    {
        var header = context.Request.Headers.Authorization.ToString();
        if (!header.StartsWith("Bearer ", StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        var token = header["Bearer ".Length..].Trim();
        if (grantsConfigured && token.StartsWith(SealedGrants.TokenPrefix, StringComparison.Ordinal))
        {
            throw new AuthFailure(AuthReason.InvalidCredential, "a sealed grant: not a test bearer");
        }

        if (s_tokens.TryGetValue(token, out var principal))
        {
            PeerIdentityAuthentication.SetAuth(context, new AuthContext("bearer", true, principal,
                new Dictionary<string, object?> { ["auth_time"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds() }));
        }

        return Task.CompletedTask;
    }
}
