DOTNET ?= dotnet

# Every vgi-csharp-owned project — scoped individually (not the whole .slnx) so `dotnet format`
# doesn't also try to reformat vgi-rpc-csharp's vendored third_party/apache-arrow-dotnet, which
# QueryFarm.Vgi transitively references.
OWN_PROJECTS := src/QueryFarm.Vgi/QueryFarm.Vgi.csproj \
	fixtures/QueryFarm.Vgi.ExampleWorker/QueryFarm.Vgi.ExampleWorker.csproj \
	fixtures/QueryFarm.Vgi.SimpleWritableWorker/QueryFarm.Vgi.SimpleWritableWorker.csproj \
	fixtures/QueryFarm.Vgi.BadProtocolWorker/QueryFarm.Vgi.BadProtocolWorker.csproj \
	fixtures/QueryFarm.Vgi.VersionedWorker/QueryFarm.Vgi.VersionedWorker.csproj \
	fixtures/QueryFarm.Vgi.VersionedTablesWorker/QueryFarm.Vgi.VersionedTablesWorker.csproj \
	fixtures/QueryFarm.Vgi.BadEnumWorker/QueryFarm.Vgi.BadEnumWorker.csproj \
	test/QueryFarm.Vgi.Tests/QueryFarm.Vgi.Tests.csproj \
	examples/01-minimal-scalar-worker/Worker.csproj \
	examples/docs/QueryFarm.Vgi.DocsExamples.csproj

.PHONY: build test smoke docs_examples test_docs_examples test_integration test_integration_subprocess test_integration_gated format format_check

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

# ---------------------------------------------------------------------------
# Gated conformance lane — what `~/Development/vgi`'s `make test_languages` calls via
# `test_csharp`, mirroring the go/typescript/java lanes there (see the note by
# VGI_EXPECTED_SKIPS in that repo's Makefile).
#
# scripts/run_tests.sh above runs the suite as ONE `unittest` invocation and is the fast
# local dev loop, but a bare pass/fail from it can't distinguish "everything passed" from
# "a fixture worker silently dropped out and nothing ran" — exactly the failure mode that
# went unnoticed for months on another lane. `scripts/run_tests.py` in the vgi repo (one
# unittest subprocess per test file) adds the two gates that catch it:
#
#   --min-executed   a floor on tests that actually ran. Set a little under this lane's
#                    current count; if a drop is intentional, lower it in the same commit.
#   --allow-skip     the skip reasons this lane expects. An unlisted reason fails the run,
#                    so a newly-gated test cannot quietly leave the lane.
#
# C# now has fixture coverage for all five (Versioned/VersionedTables dedicated single-catalog
# binaries; AttachOptions/AttachOptionsRequired as extra catalogs on ExampleWorker; BadEnum a
# dedicated bypass-Worker binary) — see fixtures/QueryFarm.Vgi.{VersionedWorker,
# VersionedTablesWorker,BadEnumWorker}/, fixtures/QueryFarm.Vgi.ExampleWorker/AttachOptions/.
# Remaining --allow-skip entries below are either genuinely shared with every other lane (docker/
# iceberg/spatial/network/HTTP-only) or Python-lane-specific setup this lane was never meant to
# exercise (VGI_TEST_DEDICATED_WORKER/VGI_SCHEMA_RECONCILE_DB).
#
# C# runs 293 today (328 discovered, 35 expected skips).
CSHARP_MIN_EXECUTED ?= 288
CSHARP_COVERAGE_GATE := --min-executed $(CSHARP_MIN_EXECUTED) \
	--allow-skip 'require spatial' \
	--allow-skip 'require-env VGI_DOCKER_IMAGE' \
	--allow-skip 'require-env VGI_DOCKER_TCP_IMAGE' \
	--allow-skip 'require-env VGI_GITHUB_NETWORK_TESTS' \
	--allow-skip 'require-env VGI_TEST_ICEBERG' \
	--allow-skip 'require-env VGI_TEST_COMPANION_TARGET' \
	--allow-skip 'require-env VGI_TEST_BEARER_TOKEN' \
	--allow-skip 'require-env VGI_TEST_DEDICATED_WORKER' \
	--allow-skip 'require-env VGI_TEST_BRANCH_DIR' \
	--allow-skip 'require-env VGI_HTTP_TRANSPORT' \
	--allow-skip 'require-env VGI_HTTP_DISABLE_ZSTD' \
	--allow-skip 'require-env VGI_HTTP_NO_COMPRESSION' \
	--allow-skip 'require-env VGI_VERSIONED_HTTP_WORKER' \
	--allow-skip 'require-env VGI_VERSIONED_TABLES_HTTP_WORKER' \
	--allow-skip 'require-env VGI_WORKER_SUPPORTS_DYNAMIC_CODE' \
	--allow-skip 'require-env VGI_SCHEMA_RECONCILE_DB' \
	--allow-skip 'require-env VGI_RULES_WORKER' \
	--allow-skip 'require-env VGI_REQUIRE_LAUNCHER_TRANSPORT'

VGI_EXT_DIR             ?= $(HOME)/Development/vgi
CSHARP_EXAMPLE_BIN          := $(CURDIR)/fixtures/QueryFarm.Vgi.ExampleWorker/bin/Debug/net10.0/vgi-example-worker
CSHARP_SIMPLE_WRITABLE_BIN  := $(CURDIR)/fixtures/QueryFarm.Vgi.SimpleWritableWorker/bin/Debug/net10.0/vgi-simple-writable-worker
CSHARP_BAD_PROTOCOL_BIN     := $(CURDIR)/fixtures/QueryFarm.Vgi.BadProtocolWorker/bin/Debug/net10.0/vgi-bad-protocol-worker
CSHARP_VERSIONED_BIN        := $(CURDIR)/fixtures/QueryFarm.Vgi.VersionedWorker/bin/Debug/net10.0/vgi-versioned-worker
CSHARP_VERSIONED_TABLES_BIN := $(CURDIR)/fixtures/QueryFarm.Vgi.VersionedTablesWorker/bin/Debug/net10.0/vgi-versioned-tables-worker
CSHARP_BAD_ENUM_BIN         := $(CURDIR)/fixtures/QueryFarm.Vgi.BadEnumWorker/bin/Debug/net10.0/vgi-bad-enum-worker

test_integration_gated: build
	cd $(VGI_EXT_DIR) && \
	    VGI_TEST_WORKER="launch:$(CSHARP_EXAMPLE_BIN)" \
	    VGI_SIMPLE_WRITABLE_WORKER="launch:$(CSHARP_SIMPLE_WRITABLE_BIN)" \
	    VGI_BAD_PROTOCOL_WORKER="$(CSHARP_BAD_PROTOCOL_BIN)" \
	    VGI_VERSIONED_WORKER="launch:$(CSHARP_VERSIONED_BIN)" \
	    VGI_VERSIONED_TABLES_WORKER="launch:$(CSHARP_VERSIONED_TABLES_BIN)" \
	    VGI_ATTACH_OPTIONS_WORKER="launch:$(CSHARP_EXAMPLE_BIN)" \
	    VGI_ATTACH_OPTIONS_REQUIRED_WORKER="launch:$(CSHARP_EXAMPLE_BIN)" \
	    VGI_BAD_ENUM_WORKER="$(CSHARP_BAD_ENUM_BIN)" \
	    VGI_REQUIRE_LAUNCHER_TRANSPORT=1 \
	    python3 scripts/run_tests.py -j 6 $(CSHARP_COVERAGE_GATE) \
	        "test/sql/integration/*" "~test/sql/integration/writable/*"
