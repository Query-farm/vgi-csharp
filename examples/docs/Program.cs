using QueryFarm.Vgi;
using QueryFarm.Vgi.DocsExamples;

var worker = new Worker()
    .CatalogName("demo")
    .DefaultSchema("main")
    .RegisterCatalog(new QueryFarm.Vgi.Protocol.CatalogInfo { Name = "demo" })
    .RegisterSchema("main", "Functions from the C# documentation")
    .RegisterSchema("catalog", "Catalog examples")
    .RegisterScalar(new UpperCaseFunction())
    .RegisterTableInOut(new EchoFunction())
    .RegisterAggregate(new SumFunction())
    .RegisterTableBuffering(new CollectFunction());

CatalogExample.Register(worker, new NumbersFunction());
await worker.RunFromArgsAsync(args);
