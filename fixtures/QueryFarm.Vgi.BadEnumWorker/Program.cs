// Deliberately-unrecognized-wire-enum-value fixture worker — drives ~/Development/vgi's
// test/sql/integration/bad_enum.test.
//
// IMPORTANT: stdout is the wire channel — never Console.WriteLine here; use Console.Error for
// diagnostics only.
//
// Built and pointed at by DuckDB via:
//
//     ATTACH 'example' AS badenum (TYPE vgi, LOCATION '<path to this executable>');
//
// Bypasses Worker.RunStdioAsync entirely: the normal hosting path constructs a sealed
// VgiServiceImpl directly with no override hook, and this fixture needs to corrupt exactly ONE
// discovery response field (the `double` scalar's null_handling) that no core-library API
// exposes a way to set outside its two legal enum members. Instead: build a real CatalogRegistry
// + VgiServiceImpl exactly as Worker would, wrap that instance in BadEnumVgiService (forwards
// everything except catalog_schema_contents_functions, which it patches), and host the wrapper
// via the same RpcServer + StdioTransport primitives Worker.RunStdioAsync itself uses — see that
// method for the pattern this mirrors. Stdio only (bare-path LOCATION, not launch:) — this
// fixture doesn't need the launcher/pooling transport.

using QueryFarm.Vgi;
using QueryFarm.Vgi.BadEnumWorker;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Transport;

var registry = new CatalogRegistry
{
    CatalogName = "example",
};
registry.RegisterScalar(new SimpleDoubleFunction());

var real = new VgiServiceImpl(registry);
var decorated = new BadEnumVgiService(real);

var server = new RpcServer(typeof(IVgiService), decorated, expectedProtocolVersion: Worker.DefaultProtocolVersion);
await server.ServeAsync(new StdioTransport(), CancellationToken.None);
