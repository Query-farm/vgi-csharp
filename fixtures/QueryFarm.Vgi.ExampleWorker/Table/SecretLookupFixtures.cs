using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Arguments;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

// Secret-lookup conformance fixtures: the five table functions the shared
// test/sql/integration/secret/secret_lookup_*.test files run against every SDK's example worker.
// The contract (names, declared requirements, output columns, value rendering) is vgi
// docs/secret_lookup_fixtures.md; the reference implementation is vgi-python
// vgi/_test_fixtures/secret_lookup.py. Keep this file in step with both.
//
// Every function selects through the SDK's public selection API (SecretValues.FindByType / Get /
// Require / ForScope / Resolved), never by indexing the secrets batch: the point is to exercise
// what worker authors call.

/// <summary>Shared constants and helpers for the secret-lookup fixtures.</summary>
internal static class SecretLookup
{
    /// <summary>The secret type every value fixture reads (registered by this worker).</summary>
    public const string SecretType = "vgi_example";

    /// <summary>Nested static scopes declared by <c>secret_value_for_scope</c>.</summary>
    public const string ScopeA = "vgi-conformance://a/";

    public const string ScopeAb = "vgi-conformance://a/b/";

    /// <summary>The foreign type and scope <c>secret_types_seen</c> requires. DuckDB's <c>s3</c>
    /// secrets default to the scope <c>s3://</c>, so an unscoped lookup would never match one.</summary>
    public const string S3Type = "s3";

    public const string S3Scope = "s3://vgi-conformance/";

    /// <summary>The simplest declaration form C# offers for "a secret of type T".</summary>
    public static RequiredSecret OfType(string secretType, string? scope = null) =>
        new() { SecretType = secretType, Scope = scope };

    public static Schema ValueSchema { get; } = new(
        [
            new Field("found", BooleanType.Default, nullable: true),
            new Field("secret_name", StringType.Default, nullable: true),
            new Field("value", StringType.Default, nullable: true),
        ],
        metadata: null);

    /// <summary>Renders a secret field as text: NULL stays NULL, booleans are lowercase, numbers
    /// plain invariant decimal.</summary>
    public static string? Render(object? value) => value switch
    {
        null => null,
        bool b => b ? "true" : "false",
        string s => s,
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString(),
    };

    /// <summary>A producer emitting the one <c>(found, secret_name, value)</c> row.</summary>
    public static ITableFunctionProducer ValueRow(Secret? secret, string field, Schema outputSchema) =>
        new OneBatchProducer(new RecordBatch(
            outputSchema,
            [
                new BooleanArray.Builder().Append(secret is not null).Build(),
                AppendNullable(new StringArray.Builder(), secret is null ? null : Render(secret.GetValue("name"))).Build(),
                AppendNullable(new StringArray.Builder(), secret is null ? null : Render(secret.GetValue(field))).Build(),
            ],
            1));

    public static StringArray.Builder AppendNullable(StringArray.Builder builder, string? value) =>
        value is null ? builder.AppendNull() : builder.Append(value);

    /// <summary>Emits one batch (nothing when <see langword="null"/>), then finishes.</summary>
    public sealed class OneBatchProducer(RecordBatch? batch) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted && batch is not null)
            {
                output.Emit(batch);
            }

            _emitted = true;
            output.Finish();
        }
    }
}

/// <summary><c>secret_value_by_type(field)</c> — static requirement in the simplest form (type
/// <c>vgi_example</c>, no name, no scope); selected by TYPE, so an unnamed
/// <c>__default_vgi_example</c> and a secret named unlike its type are both found.</summary>
public sealed class SecretValueByTypeFunction : ITableFunction
{
    public string Name => "secret_value_by_type";

    public string Description => "Conformance: a field of the vgi_example secret, selected by type";

    public IReadOnlyList<string> Categories => ["secret", "testing"];

    public IReadOnlyList<FunctionExample> Examples { get; } =
        [new FunctionExample { Sql = "SELECT * FROM secret_value_by_type('api_key')", Description = "By type" }];

    public IReadOnlyList<RequiredSecret> RequiredSecrets { get; } = [SecretLookup.OfType(SecretLookup.SecretType)];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("field", StringType.Default)], metadata: null);

    public Schema OutputSchema => SecretLookup.ValueSchema;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        SecretLookup.ValueRow(
            initParams.SecretValues.FindByType(SecretLookup.SecretType),
            initParams.Arguments.StringPositional(0),
            initParams.OutputSchema);
}

/// <summary><c>secret_value_required(field)</c> — as <c>secret_value_by_type</c>, but the bind fails
/// (via <see cref="SecretValues.Require"/>) when no <c>vgi_example</c> secret was resolved.</summary>
public sealed class SecretValueRequiredFunction : ITableFunction
{
    public string Name => "secret_value_required";

    public string Description => "Conformance: a field of the required vgi_example secret; errors when absent";

    public IReadOnlyList<string> Categories => ["secret", "testing"];

    public IReadOnlyList<FunctionExample> Examples { get; } =
        [new FunctionExample { Sql = "SELECT * FROM secret_value_required('api_key')", Description = "Required" }];

    public IReadOnlyList<RequiredSecret> RequiredSecrets { get; } = [SecretLookup.OfType(SecretLookup.SecretType)];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("field", StringType.Default)], metadata: null);

    public Schema OutputSchema => SecretLookup.ValueSchema;

    public void Bind(TableBindParams bindParams) => bindParams.SecretValues.Require(SecretLookup.SecretType);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        SecretLookup.ValueRow(
            initParams.SecretValues.FindByType(SecretLookup.SecretType),
            initParams.Arguments.StringPositional(0),
            initParams.OutputSchema);
}

/// <summary><c>secret_value_by_name(name, field)</c> — requests type <c>vgi_example</c> with secret
/// name <c>name</c> at bind (two-phase), then selects by name, only if that secret's type is
/// <c>vgi_example</c>.</summary>
public sealed class SecretValueByNameFunction : ITableFunction
{
    public string Name => "secret_value_by_name";

    public string Description => "Conformance: a field of the named vgi_example secret";

    public IReadOnlyList<string> Categories => ["secret", "testing"];

    public IReadOnlyList<FunctionExample> Examples { get; } =
        [new FunctionExample { Sql = "SELECT * FROM secret_value_by_name('my_secret', 'api_key')", Description = "By name" }];

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("name", StringType.Default), TableArgFields.Positional("field", StringType.Default)],
        metadata: null);

    public Schema OutputSchema => SecretLookup.ValueSchema;

    public void Bind(TableBindParams bindParams) =>
        bindParams.SecretValues.Get(SecretLookup.SecretType, name: bindParams.Arguments.StringPositional(0));

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        SecretLookup.ValueRow(
            initParams.SecretValues.Get(SecretLookup.SecretType, name: initParams.Arguments.StringPositional(0)),
            initParams.Arguments.StringPositional(1),
            initParams.OutputSchema);
}

/// <summary><c>secret_value_for_scope(path, field)</c> — three static <c>vgi_example</c> requirements
/// (unscoped, <c>vgi-conformance://a/</c>, <c>vgi-conformance://a/b/</c>); selects worker-side the
/// delivered secret whose scope is the longest prefix of <c>path</c>, else an unscoped one, else
/// none.</summary>
public sealed class SecretValueForScopeFunction : ITableFunction
{
    public string Name => "secret_value_for_scope";

    public string Description => "Conformance: a field of the vgi_example secret whose scope best matches a path";

    public IReadOnlyList<string> Categories => ["secret", "testing"];

    public IReadOnlyList<FunctionExample> Examples { get; } =
    [
        new FunctionExample
        {
            Sql = "SELECT * FROM secret_value_for_scope('vgi-conformance://a/b/x', 'api_key')",
            Description = "Longest-prefix scope selection",
        },
    ];

    public IReadOnlyList<RequiredSecret> RequiredSecrets { get; } =
    [
        SecretLookup.OfType(SecretLookup.SecretType),
        SecretLookup.OfType(SecretLookup.SecretType, SecretLookup.ScopeA),
        SecretLookup.OfType(SecretLookup.SecretType, SecretLookup.ScopeAb),
    ];

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("path", StringType.Default), TableArgFields.Positional("field", StringType.Default)],
        metadata: null);

    public Schema OutputSchema => SecretLookup.ValueSchema;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        SecretLookup.ValueRow(
            initParams.SecretValues.ForScope(initParams.Arguments.StringPositional(0), SecretLookup.SecretType),
            initParams.Arguments.StringPositional(1),
            initParams.OutputSchema);
}

/// <summary><c>secret_types_seen()</c> — requires <c>vgi_example</c> (unscoped) and <c>s3</c> scoped
/// to <c>s3://vgi-conformance/</c>; emits one <c>(secret_name, secret_type, scope)</c> row per
/// secret delivered.</summary>
public sealed class SecretTypesSeenFunction : ITableFunction
{
    public string Name => "secret_types_seen";

    public string Description => "Conformance: the secrets delivered for a vgi_example + s3 requirement";

    public IReadOnlyList<string> Categories => ["secret", "testing"];

    public IReadOnlyList<FunctionExample> Examples { get; } =
        [new FunctionExample { Sql = "SELECT * FROM secret_types_seen()", Description = "Delivered secrets" }];

    public IReadOnlyList<RequiredSecret> RequiredSecrets { get; } =
    [
        SecretLookup.OfType(SecretLookup.SecretType),
        SecretLookup.OfType(SecretLookup.S3Type, SecretLookup.S3Scope),
    ];

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("secret_name", StringType.Default, nullable: true),
            new Field("secret_type", StringType.Default, nullable: true),
            new Field("scope", StringType.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var secrets = initParams.SecretValues.Resolved.Values.OrderBy(s => s.Name, StringComparer.Ordinal).ToList();
        if (secrets.Count == 0)
        {
            return new SecretLookup.OneBatchProducer(null);
        }

        var names = new StringArray.Builder();
        var types = new StringArray.Builder();
        var scopes = new StringArray.Builder();
        foreach (var secret in secrets)
        {
            names.Append(secret.Name);
            SecretLookup.AppendNullable(types, secret.Type);
            scopes.Append(secret.Scope ?? "");
        }

        return new SecretLookup.OneBatchProducer(
            new RecordBatch(initParams.OutputSchema, [names.Build(), types.Build(), scopes.Build()], secrets.Count));
    }
}
