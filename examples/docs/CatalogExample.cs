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
            // The worker attests `n` is never NULL in any row it can return (ENFORCED + ALL_ROWS),
            // so it states that fact; the SDK then serves `n` non-nullable. Fields no NOT NULL /
            // PRIMARY KEY record names are served nullable, whatever their Arrow flag says.
            Constraints = ConstraintRecord.For(["catalog"], "first_five").NotNull("n").Build(),
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
