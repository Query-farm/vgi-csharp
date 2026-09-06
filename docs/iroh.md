# Iroh

The high-level worker can serve either bridge upstream:

```csharp
await worker.RunIrohTcpUpstreamAsync(
    "127.0.0.1", 9400, "production");

await worker.RunHttpAsync(
    host: "127.0.0.1",
    port: 9401,
    irohBridge: new IrohBridgeOptions("production"));
```

`RunFromArgsAsync` accepts the common `--iroh-raw-upstream`, `--iroh-issuer`,
`--iroh-trusted-proxy`, and `--iroh-observe` flags; `--http` plus an issuer
enables the HTTP bridge identity provider. The RPC client packages provide
native `iroh://` and `httpi://` transports and expose private relay, direct
address, stable key, cancellation, and timeout options.

