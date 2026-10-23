using Apache.Arrow;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

internal static class MacroFamilies
{
    internal static List<MacroOverload> FromDescriptor(CatalogMacro macro)
    {
        var kind = macro.MacroType == MacroType.Scalar ? "scalar" : "table";
        var owner = macro.EffectiveSchemaPath.ToList();
        if (macro.Overloads is { } family)
        {
            if (family.Count == 0 || macro.Parameters.Count != 0 || macro.Definition.Length != 0
                || macro.ParameterDefaults is not null || macro.ParameterDocs.Count != 0)
                throw new ArgumentException("A nonempty macro family cannot coexist with simple-definition fields.");
            if (family.Any(o => o.MacroType != kind || !o.SchemaPath.SequenceEqual(owner)))
                throw new ArgumentException("Macro overload kind and owner must equal the parent macro.");
            if (family.Any(o => o.Parameters.Any(p => p.DefaultExpression is not null)))
                throw new ArgumentException("Discovery macro defaults must be resolved values.");
            return family;
        }
        if (string.IsNullOrWhiteSpace(macro.Definition) || macro.Definition.Contains('\0'))
            throw new ArgumentException("A macro requires a nonempty SQL definition without NUL.");
        var defaults = macro.ParameterDefaults;
        if (defaults is not null && defaults.Length != 1)
            throw new ArgumentException("Macro defaults require exactly one row.");
        var defaultFields = defaults?.Schema.FieldsList ?? [];
        foreach (var name in defaultFields.Select(f => f.Name).Concat(macro.ParameterDocs.Keys))
            if (!macro.Parameters.Contains(name)) throw new ArgumentException($"Unknown macro parameter '{name}'.");
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var parameters = new List<MacroParameter>();
        var seenDefault = false;
        foreach (var name in macro.Parameters)
        {
            if (name.Length == 0 || name.Contains('\0') || !names.Add(name))
                throw new ArgumentException("Invalid or duplicate macro parameter.");
            var matching = defaultFields.Select((field, index) => (field, index)).Where(p => p.field.Name == name).ToList();
            if (matching.Count > 1) throw new ArgumentException("Duplicate macro default.");
            var hasDefault = matching.Count == 1;
            if (seenDefault && !hasDefault) throw new ArgumentException("Required macro parameter cannot follow a default.");
            seenDefault |= hasDefault;
            byte[]? value = null;
            if (hasDefault)
            {
                var (field, index) = matching[0];
                value = RecordBatchIpc.Write(new RecordBatch(new Schema([field], defaults!.Schema.Metadata), [defaults!.Column(index)], 1));
            }
            parameters.Add(new MacroParameter { Name = name, DeclaredType = null,
                DefaultValue = value, DefaultExpression = null, Documentation = macro.ParameterDocs.GetValueOrDefault(name) });
        }
        return [new MacroOverload { Parameters = parameters, Definition = macro.Definition,
            Profile = "duckdb.macro.v1", SchemaPath = owner, MacroType = kind,
            EvaluationContext = new QueryEvaluationContext { Timezone = "UTC", Collation = "binary",
                DefaultOrder = "asc", DefaultNullOrder = "nulls_last", PreserveInsertionOrder = true } }];
    }
}
