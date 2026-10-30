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
            // `n` is a non-nullable Arrow field, so the table states the matching fact. The worker
            // attests it: every row it can return has a value, hence ENFORCED + ALL_ROWS.
            Constraints =
            [
                ConstraintRecord.Build(new ConstraintRecordSpec
                {
                    SchemaPath = ["catalog"],
                    Table = "first_five",
                    Id = "n_not_null",
                    Kind = ConstraintKind.NotNull,
                    Columns = [["n"]],
                    Enforcement = ConstraintEnforcement.Enforced,
                    Validation = ConstraintValidation.AllRows,
                }),
            ],
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
