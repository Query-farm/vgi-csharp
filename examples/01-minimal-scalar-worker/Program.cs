// M1's minimal deliverable: serves upper_case(value: VARCHAR) -> VARCHAR over stdio.
//
// IMPORTANT: stdout is the wire channel — never Console.WriteLine here; use Console.Error for
// diagnostics only.
//
// Built and pointed at by DuckDB via:
//
//     ATTACH 'example' AS example (TYPE vgi, LOCATION '<path to this executable>');

using QueryFarm.Vgi;
using QueryFarm.Vgi.Examples.MinimalScalarWorker;

var worker = new Worker()
    .CatalogName("example")
    .DefaultSchema("main")
    .RegisterScalar(new UpperCaseFunction());

await worker.RunFromArgsAsync(args);
