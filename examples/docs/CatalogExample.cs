using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.DocsExamples;

public static class CatalogExample
{
    public static void Register(Worker worker, NumbersFunction numbers)
    {
        worker.RegisterCatalogTable(new CatalogTable
        {
            Name = "first_five",
            SchemaName = "catalog",
            Comment = "The integers zero through four",
            ScanFunction = numbers,
            ScanArguments = [5L],
        });

        worker.RegisterView(new CatalogView
        {
            Name = "evens",
            SchemaName = "catalog",
            Definition = "SELECT * FROM first_five WHERE n % 2 = 0",
            Comment = "Even values from catalog.first_five",
        });

        worker.RegisterMacro(new CatalogMacro
        {
            Name = "triple",
            SchemaName = "catalog",
            MacroType = MacroType.Scalar,
            Definition = "value * 3",
            Parameters = ["value"],
            Comment = "Multiply a value by three",
        });
    }
}
