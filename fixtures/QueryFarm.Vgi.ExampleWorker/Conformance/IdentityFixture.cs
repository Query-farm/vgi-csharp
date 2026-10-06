using Microsoft.AspNetCore.Http;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.Http;
using QueryFarm.VgiRpc.Identity;
using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi.ExampleWorker.Conformance;

/// <summary>
/// The fixed <c>vgi_rpc.Identity.v1</c> policy of vgi-rpc's
/// <c>tools/cross-port/specs/IDENTITY_CONFORMANCE_FIXTURE.md</c>, for
/// <c>vgi-rpc-test-hosted --identity</c>. Opted into with <c>--identity</c> (or
/// <c>VGI_FIXTURE_IDENTITY=1</c>) on <c>--http</c> only. <b>Test-only</b>: its authentication is
/// two request headers, trivially spoofable.
/// </summary>
public static class IdentityFixture
{
    public const string PrincipalHeader = "X-Conformance-Principal";
    public const string AuthTimeHeader = "X-Conformance-Auth-Time";
    public const string IntrospectorPrincipal = "conformance-introspector";
    public const double MaxAuthAge = 900.0;
    public const string SubjectPrincipal = "subject@conformance.example";
    public const string SubjectTokenName = "conformance-subject";
    public const long SubjectTtl = 300;
    public const string UnknownToken = "conformance-unknown-token";
    public const string UnavailableToken = "conformance-unavailable-token";
    public const string AuthUnavailableToken = "conformance-auth-unavailable-token";
    public const string ZeroTtlToken = "conformance-zero-ttl-token";
    public const string MinimalToken = "conformance-minimal-token";
    public const string PaddedProbeToken = "  conformance-padded-probe  ";
    public const string PaddedProbeName = "conformance-padded";
    public const string GrantTokenPrefix = "conformance-grant-for:";
    public const double GrantExpiresAt = 1893456000.0;
    public const string GrantId = "conformance-grant-id";
    public const string RefusedPurpose = "conformance-refused";
    public const string MinimalPurpose = "conformance-minimal";
    public const string AuthUnavailablePurpose = "conformance-auth-unavailable";

    /// <summary>Whether this process was asked to opt in.</summary>
    public static bool Requested(string[] args) =>
        args.Contains("--identity", StringComparer.Ordinal)
        || Environment.GetEnvironmentVariable("VGI_FIXTURE_IDENTITY") is "1" or "true" or "yes";

    /// <summary>Resolves under the fixed policy. The auth-unavailable token raises the
    /// <em>transport-auth</em> error (not the identity one) with a 7-second hint; the framework,
    /// not this fixture, must translate it to <c>identity_unavailable</c>.</summary>
    public static TokenIdentity? ResolveToken(string token) => token switch
    {
        UnavailableToken => throw new IdentityUnavailableException("conformance: mapping store unreachable", retryAfterSeconds: 5),
        AuthUnavailableToken => throw new AuthUnavailableException("conformance: authority unreachable", 7),
        UnknownToken => null,
        ZeroTtlToken => new TokenIdentity(SubjectPrincipal, SubjectTokenName, ttlSeconds: 0),
        MinimalToken => new TokenIdentity(SubjectPrincipal),
        PaddedProbeToken => new TokenIdentity(SubjectPrincipal, PaddedProbeName, SubjectTtl),
        _ => new TokenIdentity(SubjectPrincipal, SubjectTokenName, SubjectTtl),
    };

    /// <summary>Mints under the fixed policy; a pure function of its arguments.</summary>
    public static IssuedGrant MintGrant(string principal, string purpose, List<string> scopes, long ttlSeconds)
    {
        _ = ttlSeconds;
        if (purpose == AuthUnavailablePurpose)
        {
            throw new AuthUnavailableException("conformance: grant store unreachable", 7);
        }

        if (purpose == RefusedPurpose)
        {
            throw new GrantRefusedException("conformance: this purpose is refused");
        }

        var token = GrantTokenPrefix + principal + "|" + string.Join(",", scopes);
        return purpose == MinimalPurpose
            ? new IssuedGrant(token, GrantExpiresAt)
            : new IssuedGrant(token, GrantExpiresAt, GrantId);
    }

    /// <summary>The caller's identity from <see cref="PrincipalHeader"/> and
    /// <see cref="AuthTimeHeader"/> (passed through verbatim as the <c>auth_time</c> claim). An
    /// absent principal leaves the request unauthenticated rather than rejected, so reflection
    /// and <c>/health</c> stay reachable.</summary>
    public static Task Authenticate(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var principal = context.Request.Headers[PrincipalHeader].ToString();
        if (string.IsNullOrEmpty(principal))
        {
            // A bearer: not ours. Fall through to the identity bearer authenticators (sealed grants,
            // then resolve_token) -- IDENTITY_CONFORMANCE_FIXTURE.md §10.
            if (!string.IsNullOrEmpty(context.Request.Headers.Authorization.ToString()))
            {
                throw new AuthFailure(AuthReason.InvalidCredential, "no conformance principal header");
            }

            return Task.CompletedTask;
        }

        var claims = new Dictionary<string, object?>(StringComparer.Ordinal);
        var authTime = context.Request.Headers[AuthTimeHeader].ToString();
        if (authTime.Length > 0)
        {
            claims["auth_time"] = authTime;
        }

        PeerIdentityAuthentication.SetAuth(context, new AuthContext("conformance", authenticated: true, principal, claims));
        return Task.CompletedTask;
    }

    /// <summary>Opts <paramref name="worker"/> into the policy.</summary>
    public static Worker OptIn(Worker worker) => worker
        .Identity(ResolveToken, MintGrant, [IntrospectorPrincipal], MaxAuthAge)
        .HttpAuthenticate(Authenticate);
}
