// Deliberately-incompatible-protocol-version fixture worker — drives
// ~/Development/vgi's test/sql/integration/protocol_version/version_mismatch.test.
//
// IMPORTANT: stdout is the wire channel — never Console.WriteLine here; use Console.Error for
// diagnostics only.
//
// Built and pointed at by DuckDB via:
//
//     ATTACH 'badproto' AS badproto (TYPE vgi, LOCATION '<path to this executable>');
//
// Advertises protocol_version 99.0.0 (a deliberate major bump past whatever the real C++
// extension declares) so the vgi-rpc framework's dispatch-boundary check refuses the very first
// RPC the extension issues — see QueryFarm.VgiRpc.Server.RpcServer's expectedProtocolVersion
// doc comment and Worker.ProtocolVersion's doc comment.

using QueryFarm.Vgi;

var worker = new Worker()
    .CatalogName("badproto")
    .ProtocolVersion("99.0.0");

await worker.RunFromArgsAsync(args);
