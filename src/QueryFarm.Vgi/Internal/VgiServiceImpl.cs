using System.Collections.Concurrent;
using System.Text;
using Apache.Arrow;
using QueryFarm.Vgi.Aggregate;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The real <see cref="IVgiService"/> implementation a <see cref="Worker"/> serves — dispatches
/// the handful of methods this worker needs for real (bind/init/attach/detach/schemas/functions,
/// plus the three standalone table-buffering unary RPCs) against a <see cref="CatalogRegistry"/>;
/// every other method falls through to <see cref="IVgiService"/>'s own default-interface-method
/// bodies.
///
/// Multi-catalog-identity support (<c>same_name_catalogs.test</c>): the SAME worker process can be
/// <c>ATTACH</c>ed under different names (<see cref="CatalogAttachRequest.Name"/>) and serve a
/// disjoint function set per attach — see <see cref="CatalogRegistry"/>'s doc comment. The identity
/// travels as UTF-8 bytes in <c>AttachOpaqueData</c>, echoed back on every subsequent RPC that
/// carries it (<c>BindRequest.AttachOpaqueData</c>, the two <c>attach_opaque_data</c> catalog RPCs).
///
/// Table-in-out FINALIZE correlation: a table-in-out function's FINALIZE phase
/// (<see cref="VgiInitPhase.Finalize"/>) runs on the SAME connection as its INPUT phase, right after
/// the INPUT phase's EOS — so the <see cref="ITableInOutProcessor"/> created at INPUT-phase init is
/// cached here (keyed by substream id) and handed straight back rather than re-created. This is
/// per-CONNECTION (one <see cref="VgiServiceImpl"/> instance per <c>Worker.RunStdioAsync</c> call),
/// which is exactly the scope a substream's INPUT→FINALIZE pair needs.
/// </summary>
public sealed class VgiServiceImpl(CatalogRegistry catalog) : IVgiService
{
    // ConcurrentDictionary, not Dictionary: a table-in-out function with MaxWorkers>1 (parallel
    // LATERAL substreams, e.g. cache/per_value_concurrent.test) opens multiple concurrent
    // INPUT-phase inits — and later, FINALIZE inits — on independent connections within the SAME
    // worker process. A plain Dictionary's indexer/Remove are not thread-safe against concurrent
    // callers; under real concurrency this intermittently threw (or silently corrupted internal
    // state), a real bug found via the flaky cache/per_value_concurrent.test failure.
    private readonly ConcurrentDictionary<string, ITableInOutProcessor> _tableInOutProcessors = new(StringComparer.Ordinal);

    /// <summary>The catalog counter this worker reports, and the consistency anchor every split
    /// token is stamped with AND checked against — read from ONE place by both
    /// <see cref="TableFunctionPlanAsync"/> (mint) and <see cref="OpenSplitTokens"/> (verify) on
    /// purpose. Minting from a different value than redemption compares against refuses every
    /// token unconditionally, and the documented response to that
    /// (<see cref="SplitToken.KindExpired"/>, "re-run the query") re-plans, mints the same
    /// mismatch, and fails again — a livelock, not a transient error. This worker has no catalog
    /// versioning of its own yet (matches <see cref="CatalogVersionAsync"/>'s own fixed
    /// <c>Version = 1</c> default), so the constant is fixed rather than live — a future catalog
    /// with real DDL-driven versioning would thread its live counter through both call sites
    /// instead.</summary>
    private const long CatalogVersion = 1;

    public Task<BindResponse> BindAsync(BindRequest request, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(request.AttachOpaqueData);
        var schemaName = request.SchemaName ?? catalog.DefaultSchema;

        // A table/table-in-out/table-buffering Bind() that requested a dynamic (call-argument-
        // derived) secret scope via SecretsAccessor.Get() throws instead of returning normally (see
        // ThrowIfSecretsPending) — caught here and turned into a secret-scope-request BindResponse
        // (not a normal one) so the C++ extension resolves the secret and retries this bind with
        // resolved_secrets_provided=true. See SecretsAccessor's doc comment for the full two-phase
        // protocol this implements.
        try
        {
            var outputSchema = request.FunctionType == FunctionType.Table
                ? BindAnyTable(identity, schemaName, request)
                : BindScalar(identity, schemaName, request);

            return Task.FromResult(new BindResponse
            {
                OutputSchema = SchemaIpc.WriteSchemaOnly(outputSchema),
                OpaqueData = null,
                LookupSecretTypes = [],
                LookupScopes = [],
                LookupNames = [],
            });
        }
        catch (SecretScopeRequestException ex)
        {
            return Task.FromResult(new BindResponse
            {
                OutputSchema = SchemaIpc.WriteSchemaOnly(new Apache.Arrow.Schema([], null)),
                OpaqueData = null,
                LookupSecretTypes = ex.Lookups.Select(l => l.SecretType).ToList(),
                LookupScopes = ex.Lookups.Select(l => l.Scope ?? "").ToList(),
                LookupNames = ex.Lookups.Select(l => l.SecretName ?? "").ToList(),
            });
        }
    }

    public Task<RpcStream<StreamState>> InitAsync(InitRequest request, ICallContext? ctx = null)
    {
        var bindRequest = EmbeddedIpc.Decode<BindRequest>(request.BindCall);
        var identity = DecodeIdentity(bindRequest.AttachOpaqueData);
        var schemaName = bindRequest.SchemaName ?? catalog.DefaultSchema;
        var name = bindRequest.FunctionName;

        if (bindRequest.FunctionType != FunctionType.Table)
        {
            return Task.FromResult(InitScalar(identity, schemaName, bindRequest));
        }

        if (catalog.FindTable(identity, schemaName, name, TableArgCodec.Decode(bindRequest.Arguments)) is { } table)
        {
            return Task.FromResult(InitTable(table, bindRequest, request));
        }

        if (catalog.FindTableInOut(identity, schemaName, name, DecodeInputSchema(bindRequest.InputSchema)) is { } tableInOut)
        {
            return Task.FromResult(InitTableInOut(tableInOut, bindRequest, request));
        }

        if (catalog.FindTableBuffering(identity, schemaName, name) is { } buffering)
        {
            return Task.FromResult(InitTableBuffering(buffering, bindRequest, request));
        }

        throw new InvalidOperationException($"Unknown table function '{schemaName}.{name}' (identity '{identity}').");
    }

    /// <summary>Scan planning (splits). Only ever called by the C++ client when the target table
    /// function's catalog metadata advertises <c>supports_splits=true</c> — see
    /// <see cref="ITableFunction.Plan"/>'s doc comment for the gate this relies on.</summary>
    public Task<TableFunctionPlanResult> TableFunctionPlanAsync(TableFunctionPlanRequest request, ICallContext? ctx = null)
    {
        if (request.BindCall is not { Length: > 0 } bindCallBytes)
        {
            return Task.FromResult(DefaultSingleSplitPlan());
        }

        var bindRequest = EmbeddedIpc.Decode<BindRequest>(bindCallBytes);
        var identity = DecodeIdentity(bindRequest.AttachOpaqueData);
        var schemaName = bindRequest.SchemaName ?? catalog.DefaultSchema;

        if (catalog.FindTable(identity, schemaName, bindRequest.FunctionName, TableArgCodec.Decode(bindRequest.Arguments)) is not { } function || !function.SupportsSplits)
        {
            return Task.FromResult(DefaultSingleSplitPlan());
        }

        var arguments = TableArgCodec.Decode(bindRequest.Arguments);
        var inputSchema = bindRequest.InputSchema is { Length: > 0 } schemaBytes ? SchemaIpc.ReadSchemaOnly(schemaBytes) : null;
        var bindParams = new TableBindParams
        {
            FunctionName = bindRequest.FunctionName,
            ArgumentsBytes = bindRequest.Arguments,
            Arguments = arguments,
            Settings = bindRequest.Settings,
            Secrets = new SecretsAccessor(bindRequest.Secrets, bindRequest.ResolvedSecretsProvided),
            InputSchema = inputSchema,
            AttachOpaqueData = bindRequest.AttachOpaqueData ?? [],
            TransactionOpaqueData = bindRequest.TransactionOpaqueData ?? [],
            AtUnit = bindRequest.AtUnit,
            AtValue = bindRequest.AtValue,
        };

        var planRequest = new PlanRequest
        {
            PushdownFilters = request.PushdownFilters,
            ProjectionIds = request.ProjectionIds,
            Cursor = request.Cursor,
            MinSplits = request.MinSplits,
            TargetSplitBytes = request.TargetSplitBytes,
            MaxSplitsPerResponse = request.MaxSplitsPerResponse,
            FiltersComplete = request.FiltersComplete,
        };

        var planResult = function.Plan(bindParams, planRequest);
        // A worker that names its own catalog version is taken at its word — that is precisely
        // how expired_token.test manufactures SPLIT_SNAPSHOT_EXPIRED (pin to a version the live
        // catalog will never agree with). One that leaves it unset gets the LIVE version.
        var catalogVersion = planResult.CatalogVersion ?? CatalogVersion;

        var response = new TableFunctionPlanResult
        {
            Splits = [],
            NextCursors = planResult.NextCursors?.ToList(),
            MaxWorkers = planResult.MaxWorkers,
            EstimatedTotalSplits = planResult.EstimatedTotalSplits,
            EstimatedTotalRows = planResult.EstimatedTotalRows,
            CatalogVersion = catalogVersion,
        };

        if (planResult.Splits.Count == 0)
        {
            return Task.FromResult(response);
        }

        var fingerprint = SplitToken.BindFingerprint(
            bindRequest.SchemaName ?? "", bindRequest.FunctionName, bindRequest.Arguments, bindRequest.Settings);
        var anchor = SplitToken.Anchor(catalogVersion);

        var blobs = new List<byte[]>(planResult.Splits.Count);
        foreach (var split in planResult.Splits)
        {
            var token = SplitToken.Build(split.Payload, fingerprint, anchor);
            blobs.Add(EmbeddedIpc.Encode(new ScanSplitWire
            {
                Payload = [],
                Token = token,
                EstimatedRows = split.EstimatedRows,
                RowsExact = split.RowsExact,
                EstimatedBytes = split.EstimatedBytes,
                PartitionBounds = split.PartitionBounds,
                ColumnStatistics = split.ColumnStatistics,
                StartPosition = split.StartPosition,
                EndPosition = split.EndPosition,
            }));
        }

        response.Splits = blobs;
        return Task.FromResult(response);
    }

    /// <summary>The not-split-capable answer: a single split standing for the whole scan, its
    /// token stamped against an empty bind identity. Reached only when the client somehow calls
    /// this RPC without the target actually being a split-capable, resolvable table function (see
    /// <see cref="TableFunctionPlanAsync"/>'s doc comment on why the C++ client never does this in
    /// practice — <c>supports_splits</c> gates the whole call site).</summary>
    private static TableFunctionPlanResult DefaultSingleSplitPlan()
    {
        var fingerprint = SplitToken.BindFingerprint("", "", [], null);
        var token = SplitToken.Build(null, fingerprint, SplitToken.Anchor(CatalogVersion));
        return new TableFunctionPlanResult
        {
            Splits = [EmbeddedIpc.Encode(new ScanSplitWire { Payload = [], Token = token })],
        };
    }

    /// <summary>Best-effort cardinality estimate — see <see cref="ITableFunction.Cardinality"/>.</summary>
    public Task<TableFunctionCardinalityResult> TableFunctionCardinalityAsync(TableFunctionCardinalityRequest request, ICallContext? ctx = null)
    {
        if (request.BindCall is not { Length: > 0 } bindCallBytes)
        {
            return Task.FromResult(new TableFunctionCardinalityResult());
        }

        var bindRequest = EmbeddedIpc.Decode<BindRequest>(bindCallBytes);
        var identity = DecodeIdentity(bindRequest.AttachOpaqueData);
        var schemaName = bindRequest.SchemaName ?? catalog.DefaultSchema;

        if (catalog.FindTable(identity, schemaName, bindRequest.FunctionName, TableArgCodec.Decode(bindRequest.Arguments)) is not { } function)
        {
            return Task.FromResult(new TableFunctionCardinalityResult());
        }

        var bindParams = new TableBindParams
        {
            FunctionName = bindRequest.FunctionName,
            ArgumentsBytes = bindRequest.Arguments,
            Arguments = TableArgCodec.Decode(bindRequest.Arguments),
            Settings = bindRequest.Settings,
            Secrets = new SecretsAccessor(bindRequest.Secrets, bindRequest.ResolvedSecretsProvided),
            InputSchema = bindRequest.InputSchema is { Length: > 0 } schemaBytes ? SchemaIpc.ReadSchemaOnly(schemaBytes) : null,
            AttachOpaqueData = bindRequest.AttachOpaqueData ?? [],
            TransactionOpaqueData = bindRequest.TransactionOpaqueData ?? [],
            AtUnit = bindRequest.AtUnit,
            AtValue = bindRequest.AtValue,
        };

        var estimate = function.Cardinality(bindParams);
        return Task.FromResult(new TableFunctionCardinalityResult { Estimate = estimate, Max = estimate });
    }

    /// <summary>Per-output-column statistics — see <see cref="ITableFunction.Statistics"/>.</summary>
    public Task<byte[]?> TableFunctionStatisticsAsync(TableFunctionStatisticsRequest request, ICallContext? ctx = null)
    {
        if (request.BindCall is not { Length: > 0 } bindCallBytes)
        {
            return Task.FromResult<byte[]?>(null);
        }

        var bindRequest = EmbeddedIpc.Decode<BindRequest>(bindCallBytes);
        var identity = DecodeIdentity(bindRequest.AttachOpaqueData);
        var schemaName = bindRequest.SchemaName ?? catalog.DefaultSchema;

        if (catalog.FindTable(identity, schemaName, bindRequest.FunctionName, TableArgCodec.Decode(bindRequest.Arguments)) is not { } function)
        {
            return Task.FromResult<byte[]?>(null);
        }

        var bindParams = new TableBindParams
        {
            FunctionName = bindRequest.FunctionName,
            ArgumentsBytes = bindRequest.Arguments,
            Arguments = TableArgCodec.Decode(bindRequest.Arguments),
            Settings = bindRequest.Settings,
            Secrets = new SecretsAccessor(bindRequest.Secrets, bindRequest.ResolvedSecretsProvided),
            InputSchema = bindRequest.InputSchema is { Length: > 0 } schemaBytes ? SchemaIpc.ReadSchemaOnly(schemaBytes) : null,
            AttachOpaqueData = bindRequest.AttachOpaqueData ?? [],
            TransactionOpaqueData = bindRequest.TransactionOpaqueData ?? [],
            AtUnit = bindRequest.AtUnit,
            AtValue = bindRequest.AtValue,
        };

        var stats = function.Statistics(bindParams);
        if (stats is null || stats.Count == 0)
        {
            return Task.FromResult<byte[]?>(null);
        }

        var outputSchema = function.ResolveOutputSchema(bindParams);
        var rows = new List<(string, Catalog.ColumnStatisticsInput)>();
        foreach (var field in outputSchema.FieldsList)
        {
            if (stats.TryGetValue(field.Name, out var columnStats))
            {
                rows.Add((field.Name, columnStats));
            }
        }

        return Task.FromResult<byte[]?>(rows.Count == 0 ? null : ColumnStatisticsCodec.Encode(rows));
    }

    /// <summary>Per-parallel-scan-thread EXPLAIN ANALYZE diagnostics — see
    /// <see cref="ITableFunction.DynamicToString"/>.</summary>
    public Task<TableFunctionDynamicToStringResult> TableFunctionDynamicToStringAsync(
        TableFunctionDynamicToStringRequest request, ICallContext? ctx = null)
    {
        if (request.BindCall is not { Length: > 0 } bindCallBytes)
        {
            return Task.FromResult(new TableFunctionDynamicToStringResult());
        }

        var bindRequest = EmbeddedIpc.Decode<BindRequest>(bindCallBytes);
        var identity = DecodeIdentity(bindRequest.AttachOpaqueData);
        var schemaName = bindRequest.SchemaName ?? catalog.DefaultSchema;

        if (catalog.FindTable(identity, schemaName, bindRequest.FunctionName, TableArgCodec.Decode(bindRequest.Arguments)) is not { } function)
        {
            return Task.FromResult(new TableFunctionDynamicToStringResult());
        }

        var bindParams = new TableBindParams
        {
            FunctionName = bindRequest.FunctionName,
            ArgumentsBytes = bindRequest.Arguments,
            Arguments = TableArgCodec.Decode(bindRequest.Arguments),
            Settings = bindRequest.Settings,
            Secrets = new SecretsAccessor(bindRequest.Secrets, bindRequest.ResolvedSecretsProvided),
            InputSchema = bindRequest.InputSchema is { Length: > 0 } schemaBytes ? SchemaIpc.ReadSchemaOnly(schemaBytes) : null,
            AttachOpaqueData = bindRequest.AttachOpaqueData ?? [],
            TransactionOpaqueData = bindRequest.TransactionOpaqueData ?? [],
            AtUnit = bindRequest.AtUnit,
            AtValue = bindRequest.AtValue,
        };

        var mapping = function.DynamicToString(bindParams, request.GlobalExecutionId);
        var result = new TableFunctionDynamicToStringResult();
        foreach (var (key, value) in mapping)
        {
            result.Keys.Add(key);
            result.Values.Add(value);
        }

        return Task.FromResult(result);
    }

    /// <summary>Per-column statistics for a real catalog table — see
    /// <see cref="Catalog.CatalogTable.Statistics"/>.</summary>
    public Task<byte[]?> CatalogTableColumnStatisticsGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);
        var table = catalog.FindCatalogTable(identity, schemaName, name);
        if (table is null || table.Statistics.Count == 0)
        {
            return Task.FromResult<byte[]?>(null);
        }

        var columns = table.ResolveColumns();
        var rows = new List<(string, Catalog.ColumnStatisticsInput)>();
        foreach (var field in columns.FieldsList)
        {
            if (table.Statistics.TryGetValue(field.Name, out var stats))
            {
                rows.Add((field.Name, stats));
            }
        }

        return Task.FromResult<byte[]?>(rows.Count == 0 ? null : ColumnStatisticsCodec.Encode(rows, table.StatisticsCacheMaxAgeSeconds));
    }

    /// <summary>Verifies and strips the split envelopes an <c>init</c> carries, returning the
    /// worker's own payloads (see <see cref="TableInitParams.SplitPayloads"/>) — runs before any
    /// user code, so an unverified token's payload can never be acted on. Returns
    /// <see langword="null"/> for every ordinary (non-split) init.</summary>
    private static IReadOnlyList<byte[]>? OpenSplitTokens(InitRequest request, BindRequest bindRequest)
    {
        var tokens = request.SplitTokens;
        if (tokens is null || tokens.Count == 0)
        {
            return null;
        }

        var fingerprint = SplitToken.BindFingerprint(
            bindRequest.SchemaName ?? "", bindRequest.FunctionName, bindRequest.Arguments, bindRequest.Settings);
        var anchor = SplitToken.Anchor(CatalogVersion);

        var payloads = new List<byte[]>(tokens.Count);
        foreach (var token in tokens)
        {
            payloads.Add(SplitToken.Open(token, fingerprint, anchor));
        }

        return payloads;
    }

    public Task<TableBufferingProcessResult> TableBufferingProcessAsync(TableBufferingProcessRequest request, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(request.AttachOpaqueData);
        var schemaName = request.SchemaName ?? catalog.DefaultSchema;
        var function = ResolveTableBuffering(identity, schemaName, request.FunctionName);

        var storage = new FunctionStorage(request.ExecutionId);
        var bindContext = ReadBindContext(storage);

        var stateId = function.Process(request.InputBatch, new TableBufferingProcessParams
        {
            FunctionName = request.FunctionName,
            ExecutionId = request.ExecutionId,
            Arguments = bindContext.Arguments,
            Settings = bindContext.Settings,
            Secrets = bindContext.Secrets,
            CopyTo = bindContext.CopyTo,
            AttachOpaqueData = request.AttachOpaqueData,
            TransactionId = request.TransactionId,
            BatchIndex = request.BatchIndex,
            Storage = storage,
            Ctx = ctx,
        });

        return Task.FromResult(new TableBufferingProcessResult { StateId = stateId });
    }

    public Task<TableBufferingCombineResult> TableBufferingCombineAsync(TableBufferingCombineRequest request, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(request.AttachOpaqueData);
        var schemaName = request.SchemaName ?? catalog.DefaultSchema;
        var function = ResolveTableBuffering(identity, schemaName, request.FunctionName);

        var storage = new FunctionStorage(request.ExecutionId);
        var bindContext = ReadBindContext(storage);

        var finalizeStateIds = function.Combine(request.StateIds, new TableBufferingCombineParams
        {
            FunctionName = request.FunctionName,
            ExecutionId = request.ExecutionId,
            Arguments = bindContext.Arguments,
            Settings = bindContext.Settings,
            Secrets = bindContext.Secrets,
            CopyTo = bindContext.CopyTo,
            InputSchema = bindContext.InputSchema,
            AttachOpaqueData = request.AttachOpaqueData,
            TransactionId = request.TransactionId,
            Storage = storage,
            Ctx = ctx,
        });

        return Task.FromResult(new TableBufferingCombineResult { FinalizeStateIds = finalizeStateIds.ToList() });
    }

    public Task<TableBufferingDestructorResult> TableBufferingDestructorAsync(TableBufferingDestructorRequest request, ICallContext? ctx = null)
    {
        FunctionStorage.DeleteAll(request.ExecutionId);
        return Task.FromResult(new TableBufferingDestructorResult());
    }

    public Task<AggregateBindResult> AggregateBindAsync(AggregateBindRequest request, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(request.AttachOpaqueData);
        var schemaName = request.SchemaName ?? catalog.DefaultSchema;
        var function = ResolveAggregate(identity, schemaName, request.FunctionName);

        var arguments = TableArgCodec.Decode(request.Arguments);
        var inputSchema = request.InputSchema is { Length: > 0 } bytes ? SchemaIpc.ReadSchemaOnly(bytes) : null;

        var bindParams = new AggregateBindParams
        {
            FunctionName = request.FunctionName,
            Arguments = arguments,
            InputSchema = inputSchema,
            Settings = request.Settings,
            Secrets = request.Secrets,
        };

        function.Bind(bindParams);
        var outputSchema = function.ResolveOutputSchema(bindParams);

        var executionId = Guid.NewGuid().ToByteArray();
        if (request.Arguments.Length > 0)
        {
            new AggregateStateStore(new FunctionStorage(executionId)).SaveArgs(request.Arguments);
        }

        return Task.FromResult(new AggregateBindResult
        {
            OutputSchema = SchemaIpc.WriteSchemaOnly(outputSchema),
            ExecutionId = executionId,
        });
    }

    public Task<AggregateUpdateResult> AggregateUpdateAsync(AggregateUpdateRequest request, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(request.AttachOpaqueData);
        var schemaName = request.SchemaName ?? catalog.DefaultSchema;
        var function = ResolveAggregate(identity, schemaName, request.FunctionName);

        var batch = request.InputBatch;
        var gidIndex = batch.Schema.GetFieldIndex(AggregateGroupIdColumn);
        if (gidIndex < 0)
        {
            throw new InvalidOperationException("aggregate_update: input_batch is missing the '__vgi_group_id' column.");
        }

        var count = batch.Length;
        var groupIds = ReadInt64Column((Int64Array)batch.Column(gidIndex), count);

        var remainingFields = new List<Field>();
        var remainingArrays = new List<IArrowArray>();
        for (var i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            if (i == gidIndex)
            {
                continue;
            }

            remainingFields.Add(batch.Schema.GetFieldByIndex(i));
            remainingArrays.Add(batch.Column(i));
        }

        var inputColumns = new RecordBatch(new Schema(remainingFields, null), remainingArrays, count);

        var stateStore = new AggregateStateStore(new FunctionStorage(request.ExecutionId));
        var states = new Dictionary<long, byte[]>();
        foreach (var gid in groupIds.Distinct())
        {
            if (stateStore.ReadState(gid) is { } existing)
            {
                states[gid] = existing;
            }
        }

        var callParams = BuildAggregateCallParams(request.FunctionName, request.ExecutionId, stateStore, ctx);
        function.Update(inputColumns, groupIds, states, callParams);

        foreach (var (gid, bytes) in states)
        {
            stateStore.WriteState(gid, bytes);
        }

        return Task.FromResult(new AggregateUpdateResult());
    }

    public Task<AggregateCombineResult> AggregateCombineAsync(AggregateCombineRequest request, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(request.AttachOpaqueData);
        var schemaName = request.SchemaName ?? catalog.DefaultSchema;
        var function = ResolveAggregate(identity, schemaName, request.FunctionName);

        var batch = request.MergeBatch;
        var srcIndex = batch.Schema.GetFieldIndex("source_group_id");
        var tgtIndex = batch.Schema.GetFieldIndex("target_group_id");
        if (srcIndex < 0 || tgtIndex < 0)
        {
            throw new InvalidOperationException(
                "aggregate_combine: merge_batch is missing 'source_group_id'/'target_group_id'.");
        }

        var count = batch.Length;
        var sources = ReadInt64Column((Int64Array)batch.Column(srcIndex), count);
        var targets = ReadInt64Column((Int64Array)batch.Column(tgtIndex), count);

        var stateStore = new AggregateStateStore(new FunctionStorage(request.ExecutionId));
        var callParams = BuildAggregateCallParams(request.FunctionName, request.ExecutionId, stateStore, ctx);
        var dirty = new Dictionary<long, byte[]>();

        for (var i = 0; i < count; i++)
        {
            var src = sources[i];
            var tgt = targets[i];
            var srcState = dirty.TryGetValue(src, out var s) ? s : stateStore.ReadState(src);
            if (srcState is null)
            {
                // The source never accumulated anything (e.g. an all-NULL partition under a
                // parallel worker) — nothing to fold into the target, and the target's own state
                // (if any) is already correct as-is.
                continue;
            }

            var tgtState = dirty.TryGetValue(tgt, out var t) ? t : stateStore.ReadState(tgt);
            dirty[tgt] = function.Combine(srcState, tgtState, callParams);
        }

        foreach (var (gid, bytes) in dirty)
        {
            stateStore.WriteState(gid, bytes);
        }

        return Task.FromResult(new AggregateCombineResult());
    }

    public Task<AggregateFinalizeResult> AggregateFinalizeAsync(AggregateFinalizeRequest request, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(request.AttachOpaqueData);
        var schemaName = request.SchemaName ?? catalog.DefaultSchema;
        var function = ResolveAggregate(identity, schemaName, request.FunctionName);

        var gidBatch = request.GroupIdsBatch;
        var gidIndex = gidBatch.Schema.GetFieldIndex("group_id");
        if (gidIndex < 0)
        {
            gidIndex = 0;
        }

        var count = gidBatch.Length;
        var groupIds = ReadInt64Column((Int64Array)gidBatch.Column(gidIndex), count);

        var outputSchema = SchemaIpc.ReadSchemaOnly(request.OutputSchema);
        var stateStore = new AggregateStateStore(new FunctionStorage(request.ExecutionId));
        var states = new byte[]?[count];
        for (var i = 0; i < count; i++)
        {
            states[i] = stateStore.ReadState(groupIds[i]);
        }

        var callParams = BuildAggregateCallParams(request.FunctionName, request.ExecutionId, stateStore, ctx);
        var resultArray = function.Finalize(groupIds, states, outputSchema, callParams);
        if (resultArray.Length != count)
        {
            throw new InvalidOperationException(
                $"aggregate_finalize: '{function.Name}'.Finalize returned {resultArray.Length} values but {count} group ids were requested.");
        }

        var resultBatch = new RecordBatch(outputSchema, [resultArray], count);
        return Task.FromResult(new AggregateFinalizeResult { ResultBatch = resultBatch });
    }

    public Task<AggregateDestructorResult> AggregateDestructorAsync(AggregateDestructorRequest request, ICallContext? ctx = null)
    {
        FunctionStorage.DeleteAll(request.ExecutionId);
        return Task.FromResult(new AggregateDestructorResult());
    }

    private Apache.Arrow.Schema BindScalar(string identity, string schemaName, BindRequest request)
    {
        var inputSchema = DecodeInputSchema(request.InputSchema);
        var function = ResolveScalar(identity, schemaName, request.FunctionName, request.Arguments, inputSchema);

        function.Bind(new ScalarBindParams
        {
            FunctionName = request.FunctionName,
            Arguments = request.Arguments,
            Settings = request.Settings,
            Secrets = request.Secrets,
            InputSchema = inputSchema,
        });

        return function.ResolveOutputSchema(inputSchema);
    }

    private Apache.Arrow.Schema BindAnyTable(string identity, string schemaName, BindRequest request)
    {
        var name = request.FunctionName;

        if (catalog.FindTable(identity, schemaName, name, TableArgCodec.Decode(request.Arguments)) is { } table)
        {
            return BindTable(table, request);
        }

        if (catalog.FindTableInOut(identity, schemaName, name, DecodeInputSchema(request.InputSchema)) is { } tableInOut)
        {
            RequireTableInOutInputSchema(tableInOut.Name, request.InputSchema);
            var bindParams = DecodeTableInOutBindParams(request);
            tableInOut.Bind(bindParams);
            ThrowIfSecretsPending(bindParams.Secrets);
            return tableInOut.ResolveOutputSchema(bindParams);
        }

        if (catalog.FindTableBuffering(identity, schemaName, name) is { } buffering)
        {
            var bindParams = DecodeTableInOutBindParams(request);
            buffering.Bind(bindParams);
            ThrowIfSecretsPending(bindParams.Secrets);
            return buffering.ResolveOutputSchema(bindParams);
        }

        throw new InvalidOperationException($"Unknown table function '{schemaName}.{name}' (identity '{identity}').");
    }

    private static Apache.Arrow.Schema BindTable(ITableFunction function, BindRequest request)
    {
        RequirePlainTableNoInputSchema(function.Name, request.InputSchema);

        var arguments = TableArgCodec.Decode(request.Arguments);
        var inputSchema = request.InputSchema is { Length: > 0 } bytes ? SchemaIpc.ReadSchemaOnly(bytes) : null;

        var bindParams = new TableBindParams
        {
            FunctionName = request.FunctionName,
            ArgumentsBytes = request.Arguments,
            Arguments = arguments,
            Settings = request.Settings,
            Secrets = new SecretsAccessor(request.Secrets, request.ResolvedSecretsProvided),
            InputSchema = inputSchema,
            AttachOpaqueData = request.AttachOpaqueData ?? [],
            TransactionOpaqueData = request.TransactionOpaqueData ?? [],
            CopyFrom = request.CopyFrom,
            AtUnit = request.AtUnit,
            AtValue = request.AtValue,
        };

        function.Bind(bindParams);
        ThrowIfSecretsPending(bindParams.Secrets);

        return function.ResolveOutputSchema(bindParams);
    }

    /// <summary>Aborts an in-progress bind dispatch with a <see cref="SecretScopeRequestException"/>
    /// when this call's <see cref="SecretsAccessor"/> registered any pending dynamic secret lookups —
    /// caught by <see cref="BindAsync"/>, which sends a secret-scope-request response instead of a
    /// normal one, triggering the C++ extension's two-phase bind retry. A no-op when nothing is
    /// pending (the overwhelmingly common case — no dynamic secret lookup, or already resolved on a
    /// retry).</summary>
    private static void ThrowIfSecretsPending(SecretsAccessor secrets)
    {
        if (secrets.NeedsResolution)
        {
            throw new SecretScopeRequestException(secrets.PendingLookups);
        }
    }

    /// <summary>Function-shape dispatch guard (bind/init side of a two-part fix — see
    /// <see cref="RequirePlainTableNoInputSchema"/> and <see cref="RequirePlainTableNoInitPhase"/>
    /// for the mirror image). A table-in-out function requires an input row stream to mean
    /// anything — its <c>ResolveOutputSchema</c>/processor both need a REAL
    /// <see cref="BindRequest.InputSchema"/>, not a stand-in. <see langword="null"/> here means the
    /// caller drove this call via the plain-producer RPC path (<c>table_function()</c>) instead of
    /// the exchange path this function's shape requires (<c>table_in_out_function(input=...)</c>).
    ///
    /// This was found live, against a real deployed worker, as a SILENT, NON-TERMINATING HANG, not
    /// a clean error: the call sites this guards used to substitute an empty
    /// <c>Apache.Arrow.Schema([], null)</c> for a missing input schema (see
    /// <see cref="DecodeTableInOutBindParams"/>'s and <see cref="InitTableInOut"/>'s own
    /// <c>inputSchema</c> ternaries) — which let bind/init succeed, but then both sides deadlocked:
    /// the server only stops on the processor's own <c>Finish()</c> (never reached — a row-transform/
    /// table-in-out function is designed to consume input rows that this call shape never sends),
    /// and the plain-producer client only stops when the server stops sending a continuation token
    /// (which it never does either, since this dispatch path was never designed to reach that
    /// state). Rejecting the missing schema here, before either side commits to that dance, turns
    /// the hang into an immediate, actionable error naming the function and the fix.
    ///
    /// A present-but-zero-column schema (<c>Schema([])</c>, non-null but empty) is NOT the same
    /// thing and must not be conflated with a missing one: a blended/varargs row-transform function
    /// legitimately gets called with zero real input columns (e.g. a childless <c>row_sum()</c>
    /// call) — that is a real, deliberately-empty schema the caller negotiated, not evidence of the
    /// wrong RPC method. Only a genuinely absent (<see langword="null"/>) wire field means "no input
    /// schema was negotiated at all".</summary>
    private static void RequireTableInOutInputSchema(string functionName, byte[]? inputSchema)
    {
        if (inputSchema is null)
        {
            throw new InvalidOperationException(
                $"'{functionName}' is a table-in-out function (it requires an input row stream) but no " +
                "input schema was supplied -- call it via table_in_out_function(input=...), not " +
                "table_function().");
        }
    }

    /// <summary>Mirror image of <see cref="RequireTableInOutInputSchema"/>, for the BIND-time shape
    /// of the same confusion in the other direction: a plain (producer-only) table function never
    /// legitimately receives an input schema at all (see <see cref="CatalogRegistry.FindTable"/>'s
    /// doc comment — "table calls carry no InputSchema at all"). A non-<see langword="null"/> value
    /// here means the caller drove this call via <c>table_in_out_function()</c> instead of the
    /// plain-producer path this function's shape requires (<c>table_function()</c>).</summary>
    private static void RequirePlainTableNoInputSchema(string functionName, byte[]? inputSchema)
    {
        if (inputSchema is not null)
        {
            throw new InvalidOperationException(
                $"'{functionName}' is a plain table function (it takes no input row stream) but an " +
                "input schema was supplied -- call it via table_function(), not table_in_out_function().");
        }
    }

    /// <summary>Mirror image of <see cref="RequireTableInOutInputSchema"/>, for the INIT-time shape
    /// of the same confusion: a plain table function's producer stream never carries an
    /// <see cref="InitRequest.Phase"/> (that field only means something for table-in-out/
    /// table-buffering dispatch). A non-<see langword="null"/> phase here means the caller drove
    /// this call via <c>table_in_out_function()</c> instead of <c>table_function()</c>.</summary>
    private static void RequirePlainTableNoInitPhase(string functionName, VgiInitPhase? phase)
    {
        if (phase is not null)
        {
            throw new InvalidOperationException(
                $"'{functionName}' is a plain table function (it takes no input row stream) but was " +
                $"called with init phase '{phase}' set -- call it via table_function(), not " +
                "table_in_out_function().");
        }
    }

    private static TableInOutBindParams DecodeTableInOutBindParams(BindRequest request)
    {
        var arguments = TableArgCodec.Decode(request.Arguments);
        var inputSchema = request.InputSchema is { Length: > 0 } bytes
            ? SchemaIpc.ReadSchemaOnly(bytes)
            : new Apache.Arrow.Schema([], null);

        return new TableInOutBindParams
        {
            FunctionName = request.FunctionName,
            ArgumentsBytes = request.Arguments,
            Arguments = arguments,
            Settings = request.Settings,
            Secrets = new SecretsAccessor(request.Secrets, request.ResolvedSecretsProvided),
            InputSchema = inputSchema,
            AttachOpaqueData = request.AttachOpaqueData ?? [],
            CopyTo = request.CopyTo,
        };
    }

    private RpcStream<StreamState> InitScalar(string identity, string schemaName, BindRequest bindRequest)
    {
        var inputSchema = DecodeInputSchema(bindRequest.InputSchema);
        var function = ResolveScalar(identity, schemaName, bindRequest.FunctionName, bindRequest.Arguments, inputSchema);
        var outputSchema = function.ResolveOutputSchema(inputSchema);

        var header = new GlobalInitResponse
        {
            ExecutionId = Guid.NewGuid().ToByteArray(),
            OpaqueData = null,
            MaxWorkers = 1,
        };

        var state = new ScalarStreamState(function, outputSchema, bindRequest.Arguments, bindRequest.Settings, bindRequest.Secrets);
        return new RpcStream<StreamState>(outputSchema, state, InputSchema: null, Header: header);
    }

    private static RpcStream<StreamState> InitTable(ITableFunction function, BindRequest bindRequest, InitRequest request)
    {
        // Defense-in-depth mirror of BindTable's own bind-time guard: a plain table function's
        // init RPC never carries a table-in-out/table-buffering phase. A non-null phase here means
        // this call actually came from the wrong Client method (table_in_out_function() instead of
        // table_function()) -- reject it immediately rather than silently ignoring the phase and
        // running as an ordinary producer while the caller is left feeding input rows nobody reads.
        RequirePlainTableNoInitPhase(function.Name, request.Phase);

        var arguments = TableArgCodec.Decode(bindRequest.Arguments);
        var inputSchema = bindRequest.InputSchema is { Length: > 0 } schemaBytes ? SchemaIpc.ReadSchemaOnly(schemaBytes) : null;
        var bindParams = new TableBindParams
        {
            FunctionName = bindRequest.FunctionName,
            ArgumentsBytes = bindRequest.Arguments,
            Arguments = arguments,
            Settings = bindRequest.Settings,
            Secrets = new SecretsAccessor(bindRequest.Secrets, bindRequest.ResolvedSecretsProvided),
            InputSchema = inputSchema,
            AttachOpaqueData = bindRequest.AttachOpaqueData ?? [],
            TransactionOpaqueData = bindRequest.TransactionOpaqueData ?? [],
            CopyFrom = bindRequest.CopyFrom,
        };
        var outputSchema = function.ResolveOutputSchema(bindParams);

        // The execution id every parallel connection for THIS logical scan shares: the primary
        // connection (no incoming ExecutionId yet) mints one and returns it via GlobalInitResponse;
        // every secondary connection DuckDB opens (up to MaxWorkers) echoes that same id back on
        // its own InitRequest.ExecutionId. A function whose producers coordinate shared state
        // across connections (e.g. a work-queue-partitioned scan) keys that state off THIS value —
        // computed once here so TableInitParams.ExecutionId and the header response always agree
        // (using the raw, possibly-still-null incoming request.ExecutionId for that would give the
        // primary connection a different, effectively per-call-random key than every secondary).
        var executionId = request.ExecutionId is { Length: > 0 } existing ? existing : Guid.NewGuid().ToByteArray();

        // Opened before any user code runs, so an unverified token's payload can never be acted
        // on — see OpenSplitTokens' doc comment. null for every ordinary (non-split) init.
        var splitPayloads = OpenSplitTokens(request, bindRequest);

        var initParams = new TableInitParams
        {
            FunctionName = bindRequest.FunctionName,
            Arguments = arguments,
            Settings = bindRequest.Settings,
            Secrets = bindRequest.Secrets,
            OutputSchema = outputSchema,
            ProjectionIds = request.ProjectionIds,
            PushdownFilters = request.PushdownFilters,
            JoinKeys = request.JoinKeys,
            RowLimit = request.RowLimit,
            OrderByColumnName = request.OrderByColumnName,
            OrderByDirection = request.OrderByDirection,
            OrderByNullOrder = request.OrderByNullOrder,
            OrderByLimit = request.OrderByLimit,
            TablesamplePercentage = request.TablesamplePercentage,
            TablesampleSeed = request.TablesampleSeed,
            ExecutionId = executionId,
            AttachOpaqueData = bindRequest.AttachOpaqueData ?? [],
            TransactionOpaqueData = bindRequest.TransactionOpaqueData ?? [],
            SplitPayloads = splitPayloads,
            CopyFrom = bindRequest.CopyFrom,
            AtUnit = bindRequest.AtUnit,
            AtValue = bindRequest.AtValue,
        };

        var producer = function.CreateProducer(initParams);

        var header = new GlobalInitResponse
        {
            ExecutionId = executionId,
            OpaqueData = null,
            MaxWorkers = function.MaxWorkers ?? 1,
        };

        var state = new TableProducerStreamState(producer);
        return new RpcStream<StreamState>(initParams.ProjectedSchema, state, InputSchema: null, Header: header);
    }

    /// <summary>Handles both phases a streaming table-in-out function's <c>init</c> RPC can carry:
    /// <see cref="VgiInitPhase.Input"/> (the default — creates a fresh per-substream processor and
    /// opens its exchange stream) and <see cref="VgiInitPhase.Finalize"/> (looks up the SAME
    /// processor by substream id and opens its producer stream instead).</summary>
    private RpcStream<StreamState> InitTableInOut(ITableInOutFunction function, BindRequest bindRequest, InitRequest request)
    {
        // Defense-in-depth mirror of BindAnyTable's own bind-time guard (RequireTableInOutInputSchema):
        // only meaningful for the INPUT-phase (exchange) path below -- a FINALIZE init reuses the
        // SAME bind_call the INPUT phase already validated, on the same connection, so it carries
        // no new shape-mismatch risk of its own. Checked BEFORE any of the (below) silent
        // `?? new Schema([], null)` substitution -- see that guard's doc comment for the live
        // hang this prevents.
        if (request.Phase != VgiInitPhase.Finalize)
        {
            RequireTableInOutInputSchema(function.Name, bindRequest.InputSchema);
        }

        var arguments = TableArgCodec.Decode(bindRequest.Arguments);
        var inputSchema = bindRequest.InputSchema is { Length: > 0 } schemaBytes
            ? SchemaIpc.ReadSchemaOnly(schemaBytes)
            : new Apache.Arrow.Schema([], null);
        var bindParams = new TableInOutBindParams
        {
            FunctionName = bindRequest.FunctionName,
            ArgumentsBytes = bindRequest.Arguments,
            Arguments = arguments,
            Settings = bindRequest.Settings,
            Secrets = new SecretsAccessor(bindRequest.Secrets, bindRequest.ResolvedSecretsProvided),
            InputSchema = inputSchema,
            AttachOpaqueData = bindRequest.AttachOpaqueData ?? [],
            CopyTo = bindRequest.CopyTo,
        };
        var outputSchema = function.ResolveOutputSchema(bindParams);
        var executionId = request.ExecutionId is { Length: > 0 } existing ? existing : Guid.NewGuid().ToByteArray();
        var substreamKey = SubstreamKey(request, executionId);

        // The batched correlated-LATERAL operator (PhysicalVgiLateralBatch) VALIDATES the wire
        // schema strictly against whatever projection it negotiated — unlike a plain table
        // function's producer stream, declaring the FULL schema here when the client actually
        // requested a subset crashes the C++ side (ArrowToDuckDB indexing past the narrowed
        // expected-types vector) rather than harmlessly over-fetching. Only meaningful when the
        // function advertised ProjectionPushdown; request.ProjectionIds is null otherwise.
        var projectedSchema = request.ProjectionIds is null
            ? outputSchema
            : new Apache.Arrow.Schema(request.ProjectionIds.Select(i => outputSchema.GetFieldByIndex((int)i)), metadata: null);

        if (request.Phase == VgiInitPhase.Finalize)
        {
            if (!_tableInOutProcessors.TryRemove(substreamKey, out var processor))
            {
                throw new InvalidOperationException(
                    $"table-in-out FINALIZE init for '{function.Name}' arrived with no prior INPUT-phase " +
                    $"processor for substream '{substreamKey}' — INPUT and FINALIZE must share one connection.");
            }

            var finalizeHeader = new GlobalInitResponse { ExecutionId = executionId, OpaqueData = null, MaxWorkers = 1 };
            var finalizeState = new TableInOutFinalizeStreamState(processor);
            return new RpcStream<StreamState>(projectedSchema, finalizeState, InputSchema: null, Header: finalizeHeader);
        }

        var initParams = new TableInOutInitParams
        {
            FunctionName = bindRequest.FunctionName,
            Arguments = arguments,
            Settings = bindRequest.Settings,
            Secrets = bindRequest.Secrets,
            InputSchema = inputSchema,
            OutputSchema = outputSchema,
            ProjectionIds = request.ProjectionIds,
            ExecutionId = executionId,
            SubstreamId = request.SubstreamId,
            AttachOpaqueData = bindRequest.AttachOpaqueData ?? [],
        };
        var inputProcessor = function.CreateProcessor(initParams);
        _tableInOutProcessors[substreamKey] = inputProcessor;

        var header = new GlobalInitResponse { ExecutionId = executionId, OpaqueData = null, MaxWorkers = function.MaxWorkers ?? 1 };
        var state = new TableInOutExchangeStreamState(inputProcessor);
        return new RpcStream<StreamState>(projectedSchema, state, InputSchema: inputSchema, Header: header);
    }

    /// <summary>Handles both phases a table-buffering function's <c>init</c> RPC can carry:
    /// <see cref="VgiInitPhase.TableBuffering"/> (the Sink-init connection — mints/persists the
    /// execution id and its bind context, then opens a stream the client immediately closes with no
    /// batches) and <see cref="VgiInitPhase.TableBufferingFinalize"/> (the Source phase — builds a
    /// fresh finalize producer for one <c>finalize_state_id</c> and opens its producer stream).</summary>
    private RpcStream<StreamState> InitTableBuffering(ITableBufferingFunction function, BindRequest bindRequest, InitRequest request)
    {
        var executionId = request.ExecutionId is { Length: > 0 } existing ? existing : Guid.NewGuid().ToByteArray();

        if (request.Phase == VgiInitPhase.TableBufferingFinalize)
        {
            var arguments = TableArgCodec.Decode(bindRequest.Arguments);
            var inputSchema = bindRequest.InputSchema is { Length: > 0 } schemaBytes
                ? SchemaIpc.ReadSchemaOnly(schemaBytes)
                : new Apache.Arrow.Schema([], null);
            var bindParams = new TableInOutBindParams
            {
                FunctionName = bindRequest.FunctionName,
                ArgumentsBytes = bindRequest.Arguments,
                Arguments = arguments,
                Settings = bindRequest.Settings,
                Secrets = new SecretsAccessor(bindRequest.Secrets, bindRequest.ResolvedSecretsProvided),
                InputSchema = inputSchema,
            };
            var outputSchema = function.ResolveOutputSchema(bindParams);
            var finalizeStateId = request.FinalizeStateId ?? [];
            var storage = new FunctionStorage(executionId);

            var finalizeParams = new TableBufferingFinalizeParams
            {
                FunctionName = bindRequest.FunctionName,
                ExecutionId = executionId,
                FinalizeStateId = finalizeStateId,
                Arguments = arguments,
                Settings = bindRequest.Settings,
                OutputSchema = outputSchema,
                ProjectionIds = request.ProjectionIds,
                PushdownFilters = request.PushdownFilters,
                JoinKeys = request.JoinKeys,
                Storage = storage,
                AttachOpaqueData = bindRequest.AttachOpaqueData,
            };
            var producer = function.CreateFinalizeProducer(finalizeStateId, finalizeParams);

            var finalizeHeader = new GlobalInitResponse { ExecutionId = executionId, OpaqueData = null, MaxWorkers = 1 };
            var finalizeState = new TableProducerStreamState(producer);
            // Declare the NARROWED (projection-pushdown-aware) schema here, not the full
            // `outputSchema` — mirrors InitTableInOut's `projectedSchema` handling. A producer that
            // honors ProjectionPushdown emits batches shaped like finalizeParams.ProjectedSchema (see
            // that property's/TableInitParams.ProjectedSchema's doc comments); declaring the full
            // schema here while the client only asked for a subset mismatches what ArrowToDuckDB
            // expects to read on the C++ side.
            return new RpcStream<StreamState>(finalizeParams.ProjectedSchema, finalizeState, InputSchema: null, Header: finalizeHeader);
        }

        // phase == TableBuffering (Sink-init): persist the bind context (this call's `bind_call`
        // DOES carry the full BindRequest bytes) so the standalone table_buffering_process/combine
        // unary RPCs — which carry neither arguments nor settings on the wire — can recover them
        // regardless of which pooled worker process ends up serving those calls.
        new FunctionStorage(executionId).WriteSingle(BindContextNamespace, BindContextKey, EmbeddedIpc.Encode(bindRequest));

        var sinkBindParams = DecodeTableInOutBindParams(bindRequest);
        var sinkOutputSchema = function.ResolveOutputSchema(sinkBindParams);
        var sinkInputSchema = bindRequest.InputSchema is { Length: > 0 } sinkSchemaBytes
            ? SchemaIpc.ReadSchemaOnly(sinkSchemaBytes)
            : new Apache.Arrow.Schema([], null);

        var sinkHeader = new GlobalInitResponse { ExecutionId = executionId, OpaqueData = null, MaxWorkers = function.MaxWorkers ?? 1 };
        var sinkState = new NoOpExchangeStreamState();
        return new RpcStream<StreamState>(sinkOutputSchema, sinkState, InputSchema: sinkInputSchema, Header: sinkHeader);
    }

    /// <summary><b>table/transaction_storage.test — implemented.</b> Real per-transaction
    /// cross-process storage: <see cref="Protocol.CatalogAttachResult.SupportsTransactions"/> is now
    /// <see langword="true"/> for the <c>"example"</c> catalog identity ONLY (scoped by
    /// <see cref="CatalogAttachRequest.Name"/>, not a blanket toggle — every other identity this
    /// worker serves, accumulate/narrow_bind/projection_repro/twin_a/twin_b, keeps
    /// <see langword="false"/>, so this change cannot alter their currently-passing tests' RPC
    /// traffic at all). This makes the C++ extension call <see cref="CatalogTransactionBeginAsync"/>
    /// at the start of EVERY transaction against <c>example</c> (autocommit statements included —
    /// each is its own implicit transaction, minting its own throwaway id) and
    /// <see cref="CatalogTransactionCommitAsync"/>/<see cref="CatalogTransactionRollbackAsync"/> at
    /// the end, threading the minted <c>transaction_opaque_data</c> onto every subsequent
    /// <see cref="Protocol.BindRequest.TransactionOpaqueData"/> for that transaction (now exposed on
    /// <see cref="Table.TableBindParams.TransactionOpaqueData"/>/<see cref="Table.TableInitParams.TransactionOpaqueData"/>).
    /// The transaction id IS the storage key: <see cref="FunctionStorage"/> is already
    /// cross-process/durable (file-backed under the OS temp dir, keyed by an opaque
    /// <see langword="byte"/>[]) — originally built for table-buffering's execution-scoped state —
    /// so reusing it keyed by <c>transaction_opaque_data</c> instead of an execution id needs no new
    /// storage machinery, just <see cref="FunctionStorage.DeleteAll"/> on commit/rollback to clear
    /// it. See <c>ExampleWorker.Table.TxCachedValueFunction</c> for the fixture this backs.</summary>
    public Task<CatalogAttachResult> CatalogAttachAsync(CatalogAttachRequest request, ICallContext? ctx = null)
    {
        // See Worker.OnAttach's doc comment: a registered handler may throw (propagates as the
        // ATTACH failure, unchanged by anything below) or return an AttachContext customizing the
        // result — a null handler, or one that returns null, keeps every field below at today's
        // defaults.
        var attachContext = catalog.OnAttach?.Invoke(request);

        return Task.FromResult(new CatalogAttachResult
        {
            AttachOpaqueData = EncodeIdentity(attachContext?.Identity ?? request.Name, attachContext?.ExtraOpaqueData),
            SupportsTransactions = request.Name == "example",
            SupportsTimeTravel = true,
            CatalogVersionFrozen = true,
            CatalogVersion = 1,
            AttachOpaqueDataRequired = false,
            DefaultSchema = catalog.DefaultSchema,
            Settings = catalog.Settings.Select(EmbeddedIpc.Encode).ToList(),
            SecretTypes = catalog.SecretTypes.Select(EmbeddedIpc.Encode).ToList(),
            AttachCatalogs = [],
            Comment = catalog.DatabaseComment,
            Tags = catalog.DatabaseTags,
            SupportsColumnStatistics = true,
            GlobalFunctions = BuildGlobalFunctionInfos(),
            GlobalFunctionPrefix = catalog.GlobalFunctionPrefix,
            ResolvedDataVersion = attachContext?.ResolvedDataVersion,
            ResolvedImplementationVersion = attachContext?.ResolvedImplementationVersion,
        });
    }

    /// <summary>Mints a fresh per-transaction id (only ever called for a catalog identity whose
    /// <see cref="CatalogAttachAsync"/> response set <c>SupportsTransactions</c> — see that method's
    /// doc comment) — a plain random GUID is sufficient: it needs no relationship to
    /// <paramref name="attachOpaqueData"/> or to any other transaction, only global uniqueness so
    /// <see cref="FunctionStorage"/> never aliases two different transactions' state.</summary>
    public Task<TransactionBeginResponse> CatalogTransactionBeginAsync(byte[] attachOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult(new TransactionBeginResponse { TransactionOpaqueData = Guid.NewGuid().ToByteArray() });

    /// <summary>Clears every namespace/key this transaction wrote via <see cref="FunctionStorage"/>
    /// (e.g. <c>TxCachedValueFunction</c>'s per-key cache) — a plain <see cref="FunctionStorage.DeleteAll"/>
    /// keyed by the transaction id, since that class is already an opaque-byte[]-keyed cross-process
    /// store regardless of whether the key came from an execution id or a transaction id.</summary>
    public Task CatalogTransactionCommitAsync(byte[] attachOpaqueData, byte[] transactionOpaqueData, ICallContext? ctx = null)
    {
        FunctionStorage.DeleteAll(transactionOpaqueData);
        return Task.CompletedTask;
    }

    /// <summary>Same cleanup as <see cref="CatalogTransactionCommitAsync"/> — a rolled-back
    /// transaction's cached values must not leak into whatever transaction reuses this connection
    /// next.</summary>
    public Task CatalogTransactionRollbackAsync(byte[] attachOpaqueData, byte[] transactionOpaqueData, ICallContext? ctx = null)
    {
        FunctionStorage.DeleteAll(transactionOpaqueData);
        return Task.CompletedTask;
    }

    /// <summary>Serializes every <see cref="CatalogRegistry.GlobalFunctions"/> entry to a
    /// <see cref="FunctionInfo"/> via the same per-kind builder every schema-discovery RPC uses —
    /// global publication reuses the identical wire shape, just delivered on the attach result
    /// instead of a <c>catalog_schema_contents_functions</c> item.</summary>
    private List<byte[]> BuildGlobalFunctionInfos() =>
        catalog.GlobalFunctions.Select(fn => fn switch
        {
            IScalarFunction f => BuildFunctionInfo(f),
            ITableInOutFunction f => BuildFunctionInfo(f),
            ITableBufferingFunction f => BuildFunctionInfo(f),
            ITableFunction f => BuildFunctionInfo(f),
            IAggregateFunction f => BuildFunctionInfo(f),
            _ => throw new InvalidOperationException($"RegisterGlobalFunction: unsupported function kind '{fn.GetType()}'."),
        })
        .Select(EmbeddedIpc.Encode)
        .ToList();

    public Task CatalogDetachAsync(byte[] attachOpaqueData, ICallContext? ctx = null) => Task.CompletedTask;

    /// <summary>Pre-<c>ATTACH</c> discovery — <c>vgi_catalogs('&lt;location&gt;')</c> — see
    /// <see cref="CatalogRegistry.Catalogs"/>'s doc comment.</summary>
    public Task<ItemsResponse> CatalogCatalogsAsync(ICallContext? ctx = null) =>
        Task.FromResult(new ItemsResponse { Items = catalog.Catalogs.Select(EmbeddedIpc.Encode).ToList() });

    public Task<ItemsResponse> CatalogSchemasAsync(byte[] attachOpaqueData, byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);
        var schemaNames = catalog.SchemaNamesFor(identity);

        var items = schemaNames
            .Select(name => BuildSchemaInfo(identity, name))
            .Select(EmbeddedIpc.Encode)
            .ToList();

        return Task.FromResult(new ItemsResponse { Items = items });
    }

    public Task<ItemsResponse> CatalogSchemaGetAsync(
        byte[] attachOpaqueData, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);
        if (!catalog.SchemaNamesFor(identity).Contains(name))
        {
            return Task.FromResult(new ItemsResponse());
        }

        return Task.FromResult(new ItemsResponse { Items = [EmbeddedIpc.Encode(BuildSchemaInfo(identity, name))] });
    }

    /// <summary>Zero-or-one-item lookup for a single table by <c>(schemaName, name)</c>. Only ever
    /// called by the C++ extension when the query actually carries an <c>AT (VERSION =&gt; ...)</c>/
    /// <c>AT (TIMESTAMP =&gt; ...)</c> clause (a plain, AT-less lookup uses the long-lived cached
    /// entry from <c>catalog_schema_contents_tables</c> instead — see <c>storage/vgi_table_set.cpp</c>'s
    /// <c>VgiTableSet::GetEntry</c>). <c>atUnit</c>/<c>atValue</c> is therefore never both null/empty
    /// here in practice. A table that doesn't advertise <see cref="Catalog.CatalogTable.SupportsTimeTravel"/>
    /// refuses with a clear error (<c>table/time_travel.test</c>'s "AT clause on non-time-travel
    /// table"); a multi-branch table (<see cref="Catalog.CatalogTable.Branches"/> non-null) is passed
    /// through unchanged instead, letting the C++ extension's own multi-branch-specific AT refusal
    /// fire once it resolves the scan (<c>catalog/multi_branch_scan.test</c>) — this worker refusing
    /// first would surface the wrong error message.</summary>
    public Task<ItemsResponse> CatalogTableGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string? atUnit, string? atValue,
        byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);
        var table = catalog.FindCatalogTable(identity, schemaName, name);
        if (table is null)
        {
            return Task.FromResult(new ItemsResponse());
        }

        if (!string.IsNullOrEmpty(atUnit) && table.Branches is null)
        {
            if (!table.SupportsTimeTravel)
            {
                throw new InvalidOperationException($"Table '{schemaName}.{name}' does not support time travel queries.");
            }

            if (table.ResolveAtClause is { } resolve)
            {
                table = resolve(atUnit, atValue ?? "");
            }
        }

        return Task.FromResult(new ItemsResponse { Items = [EmbeddedIpc.Encode(BuildTableInfo(table))] });
    }

    /// <summary>Resolves a table's scan branches — see <see cref="IVgiService.CatalogTableScanBranchesGetAsync"/>'s
    /// doc comment. A table declaring an explicit <see cref="CatalogTable.Branches"/> list reports
    /// exactly that (even when empty — the C++ parser is the one that loud-fails on zero branches,
    /// not this worker); an ordinary <see cref="CatalogTable.ScanFunction"/>-backed table is
    /// reported as a single synthesized branch wrapping that same function with no extra arguments,
    /// no filter, not writable — the same shape <c>BuildInlineScanFunction</c> already gives the
    /// inline <c>TableInfo.scan_function</c> field for its real scan path.</summary>
    public Task<ScanBranchesResult> CatalogTableScanBranchesGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string? atUnit, string? atValue,
        byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);
        var table = catalog.FindCatalogTable(identity, schemaName, name)
            ?? throw new InvalidOperationException($"Unknown table: '{schemaName}.{name}'.");

        List<ScanBranch> branches;
        List<string> requiredExtensions;
        if (table.Branches is { } declared)
        {
            branches = declared.Select(BuildScanBranch).ToList();
            requiredExtensions = table.RequiredExtensions.ToList();
        }
        else if (table.ScanFunction is { } scan)
        {
            var (positional, named) = !string.IsNullOrEmpty(atUnit) && table.ResolveScanArguments is { } resolve
                ? resolve(atUnit, atValue ?? "")
                : (table.ScanArguments, table.ScanNamedArguments);
            branches = [new ScanBranch { FunctionName = scan.Name, Arguments = ScanArgsCodec.Encode(positional, named) }];
            requiredExtensions = [];
        }
        else
        {
            throw new InvalidOperationException(
                $"Catalog table '{schemaName}.{name}' declares neither a ScanFunction nor Branches to answer catalog_table_scan_branches_get with.");
        }

        return Task.FromResult(new ScanBranchesResult
        {
            Branches = branches.Select(EmbeddedIpc.Encode).ToList(),
            RequiredExtensions = requiredExtensions,
        });
    }

    private static ScanBranch BuildScanBranch(ScanBranchSpec spec) => new()
    {
        FunctionName = spec.FunctionName ?? "",
        Arguments = spec.FunctionName is not null
            ? ScanArgsCodec.Encode(spec.PositionalArguments, spec.NamedArguments)
            : [],
        BranchFilter = spec.BranchFilter,
        Writable = spec.Writable,
        SourceCatalog = spec.SourceCatalog,
        SourceSchema = spec.SourceSchema,
        SourceTable = spec.SourceTable,
        FormatName = spec.FormatName,
        FormatLocations = spec.FormatLocations?.ToList(),
        FormatOptions = spec.FormatOptions is { } opts ? ScanArgsCodec.Encode([], opts) : null,
    };

    public Task<ItemsResponse> CatalogViewGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);
        var view = catalog.FindView(identity, schemaName, name);
        return Task.FromResult(view is null
            ? new ItemsResponse()
            : new ItemsResponse { Items = [EmbeddedIpc.Encode(BuildViewInfo(view))] });
    }

    public Task<ItemsResponse> CatalogSchemaContentsTablesAsync(
        byte[] attachOpaqueData, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);
        var items = catalog.CatalogTablesFor(identity)
            .Where(table => string.Equals(table.SchemaName, name, StringComparison.Ordinal))
            .Select(BuildTableInfo)
            .Select(EmbeddedIpc.Encode)
            .ToList();

        return Task.FromResult(new ItemsResponse { Items = items });
    }

    public Task<ItemsResponse> CatalogSchemaContentsViewsAsync(
        byte[] attachOpaqueData, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);
        var items = catalog.CatalogViewsFor(identity)
            .Where(view => string.Equals(view.SchemaName, name, StringComparison.Ordinal))
            .Select(BuildViewInfo)
            .Select(EmbeddedIpc.Encode)
            .ToList();

        return Task.FromResult(new ItemsResponse { Items = items });
    }

    public Task<ItemsResponse> CatalogSchemaContentsMacrosAsync(
        byte[] attachOpaqueData, string name, SchemaObjectType type, byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);
        var wantScalar = type != SchemaObjectType.TableMacro;
        var wantTable = type != SchemaObjectType.ScalarMacro;

        var items = catalog.CatalogMacrosFor(identity)
            .Where(macro => string.Equals(macro.SchemaName, name, StringComparison.Ordinal))
            .Where(macro => macro.MacroType == Protocol.MacroType.Scalar ? wantScalar : wantTable)
            .Select(BuildMacroInfo)
            .Select(EmbeddedIpc.Encode)
            .ToList();

        return Task.FromResult(new ItemsResponse { Items = items });
    }

    /// <summary>Advertises COPY TO/FROM custom formats — despite the RPC's historical name, this
    /// covers BOTH directions (see <see cref="CopyFromFormatInfo.Direction"/>).</summary>
    public Task<ItemsResponse> CatalogCopyFromFormatsAsync(
        byte[] attachOpaqueData, byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);
        var items = catalog.CopyFormatsFor(identity).Select(BuildCopyFromFormatInfo).Select(EmbeddedIpc.Encode).ToList();
        return Task.FromResult(new ItemsResponse { Items = items });
    }

    private static CopyFromFormatInfo BuildCopyFromFormatInfo(Catalog.CopyFormat format) => new()
    {
        Comment = format.Comment,
        Tags = format.Tags,
        FormatName = format.FormatName,
        Handler = format.Handler,
        Options = SchemaIpc.WriteSchemaOnly(format.Options),
        Direction = format.Direction,
        Description = format.Description,
        Ordered = format.Ordered,
    };

    public Task<ItemsResponse> CatalogMacroGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);
        var macro = catalog.FindMacro(identity, schemaName, name);
        return Task.FromResult(macro is null
            ? new ItemsResponse()
            : new ItemsResponse { Items = [EmbeddedIpc.Encode(BuildMacroInfo(macro))] });
    }

    public Task<ItemsResponse> CatalogSchemaContentsFunctionsAsync(
        byte[] attachOpaqueData, string name, SchemaObjectType type, byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var identity = DecodeIdentity(attachOpaqueData);

        IEnumerable<byte[]> items = type switch
        {
            SchemaObjectType.ScalarFunction => catalog.ScalarFunctionsFor(identity)
                .Where(function => string.Equals(function.SchemaName, name, StringComparison.Ordinal))
                .Select(BuildFunctionInfo)
                .Select(EmbeddedIpc.Encode),
            // DuckDB's catalog doesn't distinguish a plain source table function from a streaming
            // table-in-out function or a table-buffering function at this level — all three are
            // "table functions" from the client's point of view (the TABLE-typed argument, present
            // only on the latter two, is what makes the C++ side register them differently).
            SchemaObjectType.TableFunction => catalog.TableFunctionsFor(identity)
                .Where(function => string.Equals(function.SchemaName, name, StringComparison.Ordinal))
                .Select(BuildFunctionInfo)
                .Concat(catalog.TableInOutFunctionsFor(identity)
                    .Where(function => string.Equals(function.SchemaName, name, StringComparison.Ordinal))
                    .Select(BuildFunctionInfo))
                .Concat(catalog.TableBufferingFunctionsFor(identity)
                    .Where(function => string.Equals(function.SchemaName, name, StringComparison.Ordinal))
                    .Select(BuildFunctionInfo))
                .Select(EmbeddedIpc.Encode),
            SchemaObjectType.AggregateFunction => catalog.AggregateFunctionsFor(identity)
                .Where(function => string.Equals(function.SchemaName, name, StringComparison.Ordinal))
                .Select(BuildFunctionInfo)
                .Select(EmbeddedIpc.Encode),
            _ => [],
        };

        return Task.FromResult(new ItemsResponse { Items = items.ToList() });
    }

    /// <summary>Decodes <c>BindRequest.InputSchema</c> — the concrete per-call argument shape
    /// DuckDB's binder resolved for this call site, used both to feed
    /// <c>ResolveOutputSchema</c>/<c>Bind</c> and (when a name has more than one registered
    /// overload) to disambiguate which candidate a bind/init call meant — see
    /// <see cref="OverloadResolver"/>.</summary>
    private static Apache.Arrow.Schema? DecodeInputSchema(byte[]? bytes) =>
        bytes is { Length: > 0 } ? SchemaIpc.ReadSchemaOnly(bytes) : null;

    private IScalarFunction ResolveScalar(string identity, string schemaName, string name, byte[] constArguments, Apache.Arrow.Schema? paramSchema) =>
        catalog.FindScalar(identity, schemaName, name, constArguments, paramSchema)
        ?? throw new InvalidOperationException($"Unknown scalar function '{schemaName}.{name}' (identity '{identity}').");

    private ITableBufferingFunction ResolveTableBuffering(string identity, string schemaName, string name) =>
        catalog.FindTableBuffering(identity, schemaName, name)
        ?? throw new InvalidOperationException($"Unknown table-buffering function '{schemaName}.{name}' (identity '{identity}').");

    private IAggregateFunction ResolveAggregate(string identity, string schemaName, string name) =>
        catalog.FindAggregate(identity, schemaName, name)
        ?? throw new InvalidOperationException($"Unknown aggregate function '{schemaName}.{name}' (identity '{identity}').");

    /// <summary>Synthetic column the C++ side prepends to every <c>aggregate_update</c>
    /// <c>input_batch</c> — see <c>VgiAggregateUpdate</c>'s <c>__vgi_group_id</c> field.</summary>
    private const string AggregateGroupIdColumn = "__vgi_group_id";

    private static long[] ReadInt64Column(Int64Array array, int count)
    {
        var values = new long[count];
        for (var i = 0; i < count; i++)
        {
            values[i] = array.GetValue(i) ?? throw new InvalidOperationException(
                "aggregate RPC: group id column must not contain NULL.");
        }

        return values;
    }

    /// <summary>Builds the per-call params every <c>aggregate_update</c>/<c>_combine</c>/
    /// <c>_finalize</c> handler passes to <see cref="IAggregateFunction"/> — recovers the bind-time
    /// const arguments from storage since none of those three standalone unary RPCs carry them on
    /// the wire (see <see cref="Aggregate.AggregateCallParams"/>'s doc comment).</summary>
    private static Aggregate.AggregateCallParams BuildAggregateCallParams(
        string functionName, byte[] executionId, AggregateStateStore stateStore, ICallContext? ctx) =>
        new()
        {
            FunctionName = functionName,
            ExecutionId = executionId,
            Arguments = TableArgCodec.Decode(stateStore.LoadArgs()),
            Ctx = ctx,
        };

    /// <summary>Separates the deterministic identity-name portion of an encoded
    /// <c>attach_opaque_data</c> blob from its trailing per-attach random session bytes (see
    /// <see cref="EncodeIdentity"/>) — a catalog name is never empty, so a NUL byte can't appear
    /// inside it, making it a safe, unambiguous separator.</summary>
    private const byte IdentitySeparator = 0;

    private static string DecodeIdentity(byte[]? attachOpaqueData)
    {
        if (attachOpaqueData is not { Length: > 0 })
        {
            return CatalogRegistry.DefaultIdentity;
        }

        var separatorIndex = System.Array.IndexOf(attachOpaqueData, IdentitySeparator);
        var nameBytes = separatorIndex >= 0 ? attachOpaqueData[..separatorIndex] : attachOpaqueData;
        return Encoding.UTF8.GetString(nameBytes);
    }

    /// <summary>Encodes the routing identity (unchanged: still the sole input to
    /// <see cref="DecodeIdentity"/>, so same-name function-registry routing — <c>same_name_catalogs.test</c>
    /// — is unaffected), a fresh random per-<c>ATTACH</c>-call GUID, and an optional worker-supplied
    /// <paramref name="extra"/> payload (<see cref="Protocol.AttachContext.ExtraOpaqueData"/>) —
    /// <c>&lt;identity&gt;\0&lt;16-byte GUID&gt;&lt;extra&gt;</c>. No second separator is needed
    /// before <paramref name="extra"/>: the GUID is fixed-length, so a function that wants it back
    /// (see <c>Worker.OnAttach</c>'s doc comment) just skips the first NUL plus 16 bytes.
    ///
    /// The GUID exists so the FULL blob (see every <c>*Params.AttachOpaqueData</c> property) is
    /// safe to use as a genuinely unique per-attach-SESSION storage key — e.g. two independent
    /// <c>ATTACH</c>s of the very same catalog name (two parallel test files against this same
    /// worker binary, or two attaches in one session) never collide, unlike the identity alone
    /// (which is deliberately deterministic and therefore NOT unique per attach) — <paramref
    /// name="extra"/> doesn't change that: appending worker-chosen bytes after an already-random
    /// GUID can only make two blobs MORE distinguishable, never less.</summary>
    private static byte[] EncodeIdentity(string identity, byte[]? extra = null)
    {
        var nameBytes = Encoding.UTF8.GetBytes(identity);
        var suffix = Guid.NewGuid().ToByteArray();
        var extraLength = extra?.Length ?? 0;
        var encoded = new byte[nameBytes.Length + 1 + suffix.Length + extraLength];
        nameBytes.CopyTo(encoded, 0);
        encoded[nameBytes.Length] = IdentitySeparator;
        suffix.CopyTo(encoded, nameBytes.Length + 1);
        extra?.CopyTo(encoded, nameBytes.Length + 1 + suffix.Length);
        return encoded;
    }

    /// <summary>Correlates a table-in-out substream's INPUT and FINALIZE <c>init</c> calls (always
    /// the SAME connection) — keyed by substream id when present (every table-in-out connection
    /// mints one, INPUT phase or not), falling back to execution id for the rare case it's absent.</summary>
    private static string SubstreamKey(InitRequest request, byte[] executionId) =>
        request.SubstreamId is { Length: > 0 } substreamId ? Convert.ToHexString(substreamId) : Convert.ToHexString(executionId);

    private const string BindContextNamespace = "__system__";
    private const string BindContextKey = "bind_context";

    /// <summary>Recovers a table-buffering execution's bind-time arguments/settings from durable
    /// storage — see <see cref="TableBufferingProcessParams"/>'s doc comment for why the standalone
    /// process/combine unary RPCs can't just decode them off their own wire request.</summary>
    private static (TableArguments Arguments, byte[]? Settings, byte[]? Secrets, CopyToContext? CopyTo, Apache.Arrow.Schema? InputSchema) ReadBindContext(FunctionStorage storage)
    {
        var bytes = storage.ReadSingle(BindContextNamespace, BindContextKey)
            ?? throw new InvalidOperationException(
                "table_buffering_process/combine called before init(phase=TABLE_BUFFERING) established this execution's bind context.");
        var bindRequest = EmbeddedIpc.Decode<BindRequest>(bytes);
        var inputSchema = bindRequest.InputSchema is { Length: > 0 } schemaBytes ? SchemaIpc.ReadSchemaOnly(schemaBytes) : null;
        return (TableArgCodec.Decode(bindRequest.Arguments), bindRequest.Settings, bindRequest.Secrets, bindRequest.CopyTo, inputSchema);
    }

    private static FunctionInfo BuildFunctionInfo(IScalarFunction function) => new()
    {
        Comment = function.Comment,
        Tags = function.Tags.ToDictionary(),
        Name = function.Name,
        SchemaName = function.SchemaName,
        FunctionType = FunctionType.Scalar,
        Arguments = SchemaIpc.WriteSchemaOnly(function.ArgumentsSchema),
        OutputSchema = SchemaIpc.WriteSchemaOnly(function.OutputSchema),
        Stability = function.Stability,
        NullHandling = function.NullHandling,
        Description = function.Description,
        RequiredSettings = function.RequiredSettings.ToList(),
        RequiredSecrets = function.RequiredSecrets.ToList(),
    };

    private static FunctionInfo BuildFunctionInfo(ITableFunction function) => new()
    {
        Comment = function.Comment,
        Tags = function.Tags.ToDictionary(),
        Name = function.Name,
        SchemaName = function.SchemaName,
        FunctionType = FunctionType.Table,
        Arguments = SchemaIpc.WriteSchemaOnly(function.ArgumentsSchema),
        OutputSchema = SchemaIpc.WriteSchemaOnly(function.OutputSchema),
        Stability = function.Stability,
        NullHandling = null,
        Description = function.Description,
        Categories = function.Categories.ToList(),
        ProjectionPushdown = function.ProjectionPushdown,
        FilterPushdown = function.FilterPushdown,
        SamplingPushdown = function.SamplingPushdown,
        LateMaterialization = function.LateMaterialization,
        SupportedExpressionFilters = function.SupportedExpressionFilters.ToList(),
        OrderPreservation = function.OrderPreservation,
        MaxWorkers = function.MaxWorkers,
        SupportsBatchIndex = function.SupportsBatchIndex,
        SupportsSplits = function.SupportsSplits,
        FiltersExactlyApplied = function.FiltersExactlyApplied,
        SupportsPositions = function.SupportsPositions,
        SplitTokenTtlSeconds = function.SplitTokenTtlSeconds,
        PartitionKind = function.PartitionKind,
        RequiredSettings = function.RequiredSettings.ToList(),
        RequiredSecrets = function.RequiredSecrets.ToList(),
    };

    /// <summary>A streaming table-in-out function advertises <c>function_type='table'</c> — same as
    /// a plain source table function — the C++ side tells the two apart by whether
    /// <see cref="ITableInOutFunction.ArgumentsSchema"/> declares a TABLE-typed argument (see
    /// <see cref="Table.TableArgFields.Table"/>).</summary>
    private static FunctionInfo BuildFunctionInfo(ITableInOutFunction function) => new()
    {
        Comment = function.Comment,
        Tags = function.Tags.ToDictionary(),
        Name = function.Name,
        SchemaName = function.SchemaName,
        FunctionType = FunctionType.Table,
        Arguments = SchemaIpc.WriteSchemaOnly(function.ArgumentsSchema),
        OutputSchema = SchemaIpc.WriteSchemaOnly(function.OutputSchema),
        Stability = function.Stability,
        NullHandling = null,
        Description = function.Description,
        Categories = function.Categories.ToList(),
        ProjectionPushdown = function.ProjectionPushdown,
        MaxWorkers = function.MaxWorkers,
        HasFinalize = function.HasFinalize,
        InputFromArgs = function.InputFromArgs,
        RequiredSettings = function.RequiredSettings.ToList(),
        RequiredSecrets = function.RequiredSecrets.ToList(),
    };

    /// <summary>A table-buffering function advertises <c>function_type='table_buffering'</c> — this
    /// is what actually routes it through the C++ Sink+Source operator (see
    /// <c>ParseVgiFunctionType</c>'s "table"/"table_buffering" distinction).</summary>
    private static FunctionInfo BuildFunctionInfo(ITableBufferingFunction function) => new()
    {
        Comment = function.Comment,
        Tags = function.Tags.ToDictionary(),
        Name = function.Name,
        SchemaName = function.SchemaName,
        FunctionType = FunctionType.TableBuffering,
        Arguments = SchemaIpc.WriteSchemaOnly(function.ArgumentsSchema),
        OutputSchema = SchemaIpc.WriteSchemaOnly(function.OutputSchema),
        Stability = function.Stability,
        NullHandling = null,
        Description = function.Description,
        Categories = function.Categories.ToList(),
        ProjectionPushdown = function.ProjectionPushdown,
        FilterPushdown = function.FilterPushdown,
        MaxWorkers = function.MaxWorkers,
        SourceOrderDependent = function.SourceOrderDependent,
        SinkOrderDependent = function.SinkOrderDependent,
        RequiresInputBatchIndex = function.RequiresInputBatchIndex,
        RequiredSettings = function.RequiredSettings.ToList(),
        RequiredSecrets = function.RequiredSecrets.ToList(),
    };

    private static FunctionInfo BuildFunctionInfo(IAggregateFunction function) => new()
    {
        Comment = function.Comment,
        Tags = function.Tags.ToDictionary(),
        Name = function.Name,
        SchemaName = function.SchemaName,
        FunctionType = FunctionType.Aggregate,
        Arguments = SchemaIpc.WriteSchemaOnly(function.ArgumentsSchema),
        OutputSchema = SchemaIpc.WriteSchemaOnly(function.OutputSchema),
        Stability = function.Stability,
        NullHandling = null,
        Description = function.Description,
        Categories = function.Categories.ToList(),
        OrderDependent = function.OrderDependent,
        DistinctDependent = function.DistinctDependent,
        RequiredSettings = function.RequiredSettings.ToList(),
        RequiredSecrets = function.RequiredSecrets.ToList(),
    };

    /// <summary>Builds a schema's <see cref="ItemsResponse"/> item, including an accurate
    /// per-kind <see cref="SchemaInfo.EstimatedObjectCount"/> — the C++ extension treats a
    /// reported <c>0</c> for a kind as a HARD guarantee (<c>vgi_trust_empty_kinds</c>, default
    /// true) that it may skip every RPC for that kind entirely (<c>catalog/zero_count_bypass.test</c>),
    /// and uses a non-zero count against <c>vgi_eager_load_threshold</c> to decide bulk-load vs.
    /// per-name RPCs (<c>catalog/eager_load_threshold.test</c>) — so this must never over- or
    /// under-report. Kind names are each catalog-set subclass's OWN <c>CacheKindName()</c> string
    /// (<c>vgi_{scalar,table,aggregate}_function_set.hpp</c>, <c>vgi_table_set.hpp</c>, etc.) —
    /// function kinds are NOT combined: "scalar_function", "table_function" (covers table AND
    /// table-in-out AND table-buffering functions — they share one DuckDB-side catalog set),
    /// "aggregate_function", "table", "view", "macro", "index". This worker registers no
    /// indexes, so that kind is always 0.
    ///
    /// <para><b>catalog/zero_count_bypass.test — confirmed C++-side gap, not fixable here.</b>
    /// The test's first assertion expects a <c>catalog.entry_cache</c> log line with
    /// <c>set_kind=macro</c>/<c>outcome=kind_empty</c> after
    /// <c>EXPLAIN SELECT * FROM ex.data.ten_thousand_table</c>, on the theory that resolving the
    /// table's underlying scan-function name falls through SCALAR → AGGREGATE → MACRO before
    /// landing on TABLE_FUNCTION (<c>vgi_table_entry.cpp</c>'s
    /// <c>catalog_.GetEntry&lt;TableFunctionCatalogEntry&gt;(...)</c> bind-time lookup). Verified by
    /// temporarily instrumenting every catalog RPC handler (<see cref="CatalogSchemasAsync"/>,
    /// <see cref="CatalogSchemaGetAsync"/>, <see cref="CatalogSchemaContentsMacrosAsync"/>,
    /// <see cref="CatalogMacroGetAsync"/>) plus this method with stderr logging and re-running the
    /// test under <c>SUBPROCESS=1 VGI_WORKER_STDERR_PASSTHROUGH=1</c>: this worker sends
    /// <c>estimated_object_count["macro"] = 0</c> for the <c>data</c> schema correctly (confirmed
    /// via the ONE <see cref="CatalogSchemasAsync"/> call at ATTACH time — the wire data is exactly
    /// right), yet ZERO further catalog RPCs of any kind fire during the EXPLAIN — no
    /// <see cref="CatalogMacroGetAsync"/>, no <see cref="CatalogSchemaContentsMacrosAsync"/>, and
    /// (necessarily, since it isn't an RPC) no C++-side macro <c>GetEntry</c> call either. The
    /// likely reason: <c>ten_thousand_table</c>'s <see cref="Catalog.CatalogTable.ScanFunction"/>
    /// (a <c>StaticRowsFunction</c> named <c>"ten_thousand_table"</c>, matching the table's own
    /// name) is ALSO independently registered as a callable table function in schema <c>data</c>
    /// (<see cref="CatalogRegistry.RegisterCatalogTable"/> registers every
    /// <see cref="Catalog.CatalogTable.ScanFunction"/> this way — a deliberate, widely-relied-upon
    /// convention: see <c>large_sequence</c>/<c>funny_numbers</c> reusing the shared
    /// <c>sequence</c> function specifically to exercise this dedup). So
    /// <c>GetEntry&lt;TableFunctionCatalogEntry&gt;(context, "data", "ten_thousand_table", ...)</c>
    /// finds a real TABLE_FUNCTION_ENTRY match immediately and never needs to fall through to
    /// MACRO at all — unlike (presumably) the canonical reference workers' fixture, whose
    /// equivalent scan function is apparently NOT independently table-function-callable under that
    /// exact name, forcing the fallthrough the test expects. Making <c>ten_thousand_table</c>
    /// match would require a way to register a <see cref="Catalog.CatalogTable.ScanFunction"/>
    /// WITHOUT also exposing it as a standalone callable function — no such opt-out exists on
    /// <see cref="Catalog.CatalogTable"/> today, and adding one risks the exact-count
    /// <c>table/function_registration.test</c> (162 expected) for a single-test diagnostic-log
    /// assertion. Deferred.</para></summary>
    private SchemaInfo BuildSchemaInfo(string identity, string name)
    {
        var (comment, tags) = catalog.SchemaMetadataFor(identity, name);

        return new SchemaInfo
        {
            Comment = comment,
            Tags = tags,
            AttachOpaqueData = [],
            Name = name,
            EstimatedObjectCount = new Dictionary<string, long?>
            {
                ["table"] = catalog.CatalogTablesFor(identity).Count(t => t.SchemaName == name),
                ["view"] = catalog.CatalogViewsFor(identity).Count(v => v.SchemaName == name),
                ["scalar_function"] = catalog.ScalarFunctionsFor(identity).Count(f => f.SchemaName == name),
                ["table_function"] =
                    catalog.TableFunctionsFor(identity).Count(f => f.SchemaName == name) +
                    catalog.TableInOutFunctionsFor(identity).Count(f => f.SchemaName == name) +
                    catalog.TableBufferingFunctionsFor(identity).Count(f => f.SchemaName == name),
                ["aggregate_function"] = catalog.AggregateFunctionsFor(identity).Count(f => f.SchemaName == name),
                ["macro"] = catalog.CatalogMacrosFor(identity).Count(m => m.SchemaName == name),
                ["index"] = 0,
            },
        };
    }

    private static MacroInfo BuildMacroInfo(Catalog.CatalogMacro macro) => new()
    {
        Comment = macro.Comment,
        Tags = macro.Tags,
        Name = macro.Name,
        SchemaName = macro.SchemaName,
        MacroType = macro.MacroType,
        Parameters = macro.Parameters.ToList(),
        ParameterDefaultValues = macro.ParameterDefaults is { } defaults ? RecordBatchIpc.Write(defaults) : null,
        Definition = macro.Definition,
        ArgumentsSchema = macro.Parameters.Count == 0 ? null : BuildMacroArgumentsSchema(macro),
    };

    /// <summary>One ANY-typed field per macro parameter (macros have no static parameter TYPE in
    /// DuckDB), carrying <c>vgi_doc</c> metadata for whichever ones <see cref="Catalog.CatalogMacro.ParameterDocs"/>
    /// documents — read by <c>vgi_function_arguments()</c> (<c>arg_description</c>).</summary>
    private static byte[] BuildMacroArgumentsSchema(Catalog.CatalogMacro macro)
    {
        var fields = macro.Parameters.Select(name =>
        {
            var metadata = new Dictionary<string, string> { [VgiWireMetadata.TypeKey] = VgiWireMetadata.TypeAnyValue };
            if (macro.ParameterDocs.TryGetValue(name, out var doc))
            {
                metadata[VgiWireMetadata.DocKey] = doc;
            }

            return new Field(name, Apache.Arrow.Types.NullType.Default, nullable: true, metadata);
        });

        return SchemaIpc.WriteSchemaOnly(new Schema(fields, metadata: null));
    }

    private static ViewInfo BuildViewInfo(CatalogView view) => new()
    {
        Comment = view.Comment,
        Tags = view.Tags,
        Name = view.Name,
        SchemaName = view.SchemaName,
        Definition = view.Definition,
        ColumnComments = view.ColumnComments,
    };

    private static TableInfo BuildTableInfo(CatalogTable table)
    {
        var columns = table.ResolveColumns();
        if (table.RowIdColumn is { } rowIdColumn)
        {
            columns = WithRowIdMetadata(columns, rowIdColumn);
        }

        if (table.ColumnComments.Count > 0 || table.ColumnDefaults.Count > 0 || table.GeneratedColumns.Count > 0)
        {
            columns = WithColumnMetadata(columns, table.ColumnComments, table.ColumnDefaults, table.GeneratedColumns, table.Name);
        }

        var byName = ColumnIndexLookup(columns, table.Name);

        return new TableInfo
        {
            Comment = table.Comment,
            Tags = table.Tags,
            Name = table.Name,
            SchemaName = table.SchemaName,
            Columns = SchemaIpc.WriteSchemaOnly(columns),
            NotNullConstraints = table.NotNullColumns.Select(c => (int?)byName(c)).ToList(),
            UniqueConstraints = table.UniqueColumns.Select(group => group.Select(c => (int?)byName(c)).ToList()).ToList(),
            CheckConstraints = table.CheckConstraints.ToList(),
            PrimaryKeyConstraints = table.PrimaryKeyColumns.Count == 0
                ? []
                : [table.PrimaryKeyColumns.Select(c => (int?)byName(c)).ToList()],
            ForeignKeyConstraints = table.ForeignKeys.Select(fk => EmbeddedIpc.Encode(new ForeignKeyInfo
            {
                FkColumns = fk.Columns.ToList(),
                PkColumns = fk.ReferencedColumns.ToList(),
                ReferencedTable = fk.ReferencedTable,
                ReferencedSchema = fk.ReferencedSchema ?? table.SchemaName,
            })).ToList(),
            SupportsInsert = table.SupportsInsert,
            SupportsUpdate = table.SupportsUpdate,
            SupportsDelete = table.SupportsDelete,
            SupportsReturning = table.SupportsReturning,
            SupportsColumnStatistics = table.Statistics.Count > 0,
            ScanFunction = table.ScanFunction is { } scan && table.InlineScanFunction
                ? BuildInlineScanFunction(scan.Name, table.ScanArguments, table.ScanNamedArguments)
                : null,
            InsertFunction = table.InsertFunction is { } insert ? BuildInlineScanFunction(insert.Name) : null,
            UpdateFunction = table.UpdateFunction is { } update ? BuildInlineScanFunction(update.Name) : null,
            DeleteFunction = table.DeleteFunction is { } delete ? BuildInlineScanFunction(delete.Name) : null,
            CardinalityEstimate = table.CardinalityEstimate,
            CardinalityMax = table.CardinalityMax,
            ColumnStatistics = null,
            BindResult = null,
            RequiredFilters = table.RequiredFilters.Select(group => group.ToList()).ToList(),
        };
    }

    /// <summary>A function-backed table's read/write delegate functions usually take NO extra
    /// arguments of their own — see <see cref="Protocol.ScanFunctionResult"/>'s doc comment for why
    /// an empty/zero-length <see cref="Protocol.ScanFunctionResult.Arguments"/> means exactly that,
    /// rather than needing a degenerate zero-field embedded struct batch. A <see cref="CatalogTable"/>
    /// declaring <see cref="CatalogTable.ScanArguments"/>/<see cref="CatalogTable.ScanNamedArguments"/>
    /// (e.g. a table backed by a function whose first argument is a required row count) instead
    /// bakes those fixed constants in, so every scan of the table binds with them.</summary>
    private static byte[] BuildInlineScanFunction(
        string functionName,
        IReadOnlyList<object?>? positionalArguments = null,
        IReadOnlyDictionary<string, object?>? namedArguments = null) => EmbeddedIpc.Encode(new ScanFunctionResult
        {
            FunctionName = functionName,
            Arguments = ScanArgsCodec.Encode(positionalArguments ?? [], namedArguments),
            RequiredExtensions = [],
        });

    private static Schema WithRowIdMetadata(Schema schema, string rowIdColumn)
    {
        var index = schema.GetFieldIndex(rowIdColumn);
        if (index < 0)
        {
            throw new InvalidOperationException($"RowIdColumn '{rowIdColumn}' is not a column of this table's schema.");
        }

        var fields = schema.FieldsList.ToList();
        var target = fields[index];
        var metadata = new Dictionary<string, string>(target.Metadata ?? new Dictionary<string, string>(StringComparer.Ordinal))
        {
            [VgiRowIdMetadata.Key] = VgiRowIdMetadata.Value,
        };
        fields[index] = new Field(target.Name, target.DataType, target.IsNullable, metadata);
        return new Schema(fields, schema.Metadata);
    }

    /// <summary>Merges <paramref name="comments"/>/<paramref name="defaults"/> (both keyed by column
    /// NAME) onto the matching field's Arrow metadata as the bare keys <c>"comment"</c>/<c>"default"</c>
    /// — the exact strings <c>vgi_catalog_api.cpp</c>'s column-metadata reader looks up (see
    /// <see cref="Catalog.CatalogTable.ColumnComments"/>'s doc comment).</summary>
    private static Schema WithColumnMetadata(
        Schema schema, IReadOnlyDictionary<string, string> comments, IReadOnlyDictionary<string, string> defaults,
        IReadOnlyDictionary<string, string> generatedColumns, string tableName)
    {
        var fields = schema.FieldsList.ToList();
        foreach (var name in comments.Keys.Concat(defaults.Keys).Concat(generatedColumns.Keys).Distinct(StringComparer.Ordinal))
        {
            var index = schema.GetFieldIndex(name);
            if (index < 0)
            {
                throw new InvalidOperationException($"Table '{tableName}': column comment/default/generated-expression references unknown column '{name}'.");
            }

            var target = fields[index];
            var metadata = new Dictionary<string, string>(target.Metadata ?? new Dictionary<string, string>(StringComparer.Ordinal));
            if (comments.TryGetValue(name, out var comment))
            {
                metadata["comment"] = comment;
            }

            if (defaults.TryGetValue(name, out var defaultExpr))
            {
                metadata["default"] = defaultExpr;
            }

            if (generatedColumns.TryGetValue(name, out var generatedExpr))
            {
                metadata[VgiWireMetadata.GeneratedExpressionKey] = generatedExpr;
            }

            fields[index] = new Field(target.Name, target.DataType, target.IsNullable, metadata);
        }

        return new Schema(fields, schema.Metadata);
    }

    private static Func<string, int> ColumnIndexLookup(Schema schema, string tableName) => columnName =>
    {
        var index = schema.GetFieldIndex(columnName);
        return index >= 0
            ? index
            : throw new InvalidOperationException($"Table '{tableName}': constraint references unknown column '{columnName}'.");
    };
}
