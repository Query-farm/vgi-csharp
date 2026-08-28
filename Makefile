DOTNET ?= dotnet

# Every vgi-csharp-owned project — scoped individually (not the whole .slnx) so `dotnet format`
# doesn't also try to reformat vgi-rpc-csharp's vendored third_party/apache-arrow-dotnet, which
# QueryFarm.Vgi transitively references.
OWN_PROJECTS := src/QueryFarm.Vgi/QueryFarm.Vgi.csproj \
	fixtures/QueryFarm.Vgi.ExampleWorker/QueryFarm.Vgi.ExampleWorker.csproj \
	fixtures/QueryFarm.Vgi.SimpleWritableWorker/QueryFarm.Vgi.SimpleWritableWorker.csproj \
	fixtures/QueryFarm.Vgi.BadProtocolWorker/QueryFarm.Vgi.BadProtocolWorker.csproj \
	test/QueryFarm.Vgi.Tests/QueryFarm.Vgi.Tests.csproj \
	examples/01-minimal-scalar-worker/Worker.csproj \
	examples/docs/QueryFarm.Vgi.DocsExamples.csproj

.PHONY: build test smoke docs_examples test_docs_examples test_integration test_integration_subprocess format format_check

build:
	$(DOTNET) build vgi-csharp.slnx

test:
	$(DOTNET) test test/QueryFarm.Vgi.Tests

smoke: build
	$(DOTNET) build examples/01-minimal-scalar-worker

docs_examples:
	$(DOTNET) build examples/docs/QueryFarm.Vgi.DocsExamples.csproj --configuration Release

test_docs_examples:
	examples/docs/verify.sh

format:
	@for p in $(OWN_PROJECTS); do $(DOTNET) format $$p || exit 1; done

format_check:
	@for p in $(OWN_PROJECTS); do $(DOTNET) format $$p --verify-no-changes || exit 1; done

# Runs the ~/Development/vgi sqllogictest suite against the C# fixture worker(s) as ONE
# `unittest` invocation over the pooled `launch:` (AF_UNIX) transport — dramatically
# faster than spawning `unittest` (and cold-starting the worker) per .test file. See
# scripts/run_tests.sh's own header comment for usage (category/single-file filtering,
# --no-build). Pass ARGS="scalar" etc. to scope it, e.g. `make test_integration ARGS=cache`.
test_integration:
	scripts/run_tests.sh $(ARGS)

# Same suite, but over the bare-subprocess transport DuckDB's LOCATION default uses —
# slower, but required for the few tests that assert on DuckDB's own local subprocess-pool
# behavior (e.g. vgi_worker_pool.test's PID-reuse check), which the launcher transport
# bypasses by design (the launcher, not DuckDB, owns the worker process in that mode).
test_integration_subprocess:
	SUBPROCESS=1 scripts/run_tests.sh $(ARGS)
