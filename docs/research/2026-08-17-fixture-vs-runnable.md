# Fixture-only versus Linux-runnable Increment 9 / foundation / runner

- Role: Dated research for Grok Bot first boot
- Status: Completed
- Owner: fol2
- Canonical language: English
- Date: 2026-08-17
- Related issue: [#9](https://github.com/fol2/newsroom-grok/issues/9)
- Map: [#1](https://github.com/fol2/newsroom-grok/issues/1) (not updated)

## Research question

Which Increment 9 / foundation / runner code in `fol2/newsroom-grok` is fixture-only (no credential, network, provider, or publication) versus actually runnable on Linux?

## Verdict

Almost all Increment 9 and foundation code on this fork is fixture-only: it validates canonical bytes, writes isolated or in-memory SQLite, and never opens a credential, network, provider, or publication path. The only Increment 9 module with a real network client is `newsroom/increment9/proving.py` (`scripts/increment9_proving_store.py fetch`). The only foundation path that talks to a live service is the optional Neo4j actual-service lane, gated by `NEWSROOM_NEO4J_SERVICE_REQUIRED=1`. `newsroom/runner.py` is the legacy OpenClaw Discord publisher and is a live credential/provider/publication path, not the accepted Increment 9 runtime.

Nothing in this tree is a bootable Linux Newsroom host. Increment 9 closeout preserved `BLOCKED_ACTIVE_COVERAGE`. The owner-approved plan still binds Hermes on macm4 (`OD-003`, `OD-007`, `CAPACITY_MACM4`). This fork is a trusted-operator port, not that control plane.

## Method

Primary sources only, current `main` (`d950336`):

- `newsroom/increment9/`
- `newsroom/authority/`
- `newsroom/runner.py`
- `CONTEXT.md`
- Increment 9 / 1C docs under `docs/operations/`, `docs/evaluation/`, `docs/adr/`, `docs/traceability/`
- Matching CLIs under `scripts/`

No implementation. No live fetch, Neo4j, or runner execution.

## How the two buckets are used

**Fixture-only** means the loaded module or CLI, as written, performs no credential lookup, network I/O, provider/model/embedding call, or publication. Local SQLite and digest checks are allowed.

**Linux-runnable** means a Python entry point on this computer can perform that work today if the operator supplies the missing host (public HTTPS, local Neo4j, or OpenClaw Gateway). Runnable is not the same as admitted. A path can run and still be prohibited for first boot.

## Increment 9

### Fixture-only

| Path | Why it is fixture-only |
|---|---|
| `newsroom/increment9/__init__.py`, `plan.py`, `shadow_plan_v1.json`, `agent_profiles_v1.json` | 9R plan loader. Module docstring: no network, credential, deployment, provider, or shadow run. |
| `newsroom/increment9/shadow_contracts.py` | 9A1 immutable scope/manifest. Docs: loading or validating performs no deployment, credential, network, provider, spend, campaign, or publication. `_NoEffect` keeps every authorisation flag false. |
| `newsroom/increment9/epoch.py`, `newsroom/authority/increment9_shadow_migrations.py` | 9B1 isolated SQLite Epoch. Effect-free except writes to an explicitly initialised shadow DB. Distinct application ID; rejected if it aliases production. Tests use `:memory:`. |
| `newsroom/increment9/controller.py` | 9B2 replay controller. Executes canonical fixture/replay records in an isolated journal. No credential, network, provider, publication, Evidence Intake, or production writer. Cannot start 9B3. |
| `newsroom/increment9/comparator.py` | 9C1 phase contracts only. No I/O; grants no credential, egress, spend, or publication. |
| `newsroom/increment9/review.py` | 9D1 pure construction/validation over sealed bytes. Does not invoke a reviewer or provider. |
| `newsroom/increment9/decision.py` | 9D2 sealed blocked decision. No file, network, reviewer, provider, or production I/O. |
| `newsroom/increment9/closeout.py` | 9G receipt validators. Preserves `BLOCKED_ACTIVE_COVERAGE`; cannot turn missing coverage into operational eligibility. |
| `newsroom/increment9/qualification.py`, `newsroom/increment9/fixtures/increment9q1_nonmutation/` | 9Q-1 CI fixture digests. Surfaces are literal files (`fixture:PUBLICATION`, and the other writer-route names). `assess()` fails if any writer probe succeeds. Does not mint a First I/O Gate Record. |
| `scripts/increment9q_nonmutation.py` | Assess-only CLI. No network. |
| `scripts/increment9_shadow_campaign.py` | 9B3 sealer. No network client, provider SDK, model/embedding adapter, publication path, or production writer. Emits `BLOCKED_BEFORE_FIRST_IO` when gates are missing. |
| `scripts/increment9_fault_campaign.py` | 9C2 sealer. Does not inject a fault or perform external I/O. When 9B3 blocked, all 26 phases are recorded as not run. |
| `scripts/increment9_shadow_decision.py` | Builds/verifies the sealed blocked 9D2 decision from already-retained bytes. |

`docs/traceability/increment-9g-final-closeout.md` records the closed-world result: empty Run/Attempt inventory, zero source/provider/model/embedding/reviewer usage, zero spend, zero public effects, zero production mutations. Closeout is evidence-process completion, not a technical PASS.

### Linux-runnable (not fixture-only)

| Path | What actually runs on Linux | Still not admitted |
|---|---|---|
| `newsroom/increment9/proving.py`, `scripts/increment9_proving_store.py` | 9P proving store. `assess`/`list` are local. `fetch` calls `default_fetch()` over allowlisted HTTPS (`urllib` plus TLS) and writes an isolated SQLite file. No publication, Graphiti, Neo4j, embeddings, OpenRouter, or Hermes. Tests inject a fake fetcher; the CLI `fetch` command is live. | Public OD-001 endpoints only. Discord and other hosts fail closed. Store path must not contain `news_pool.sqlite3` or `production`. |
| `newsroom/increment9/deployment.py` `materialise_isolated_deployment` / `teardown_isolated_deployment` / `verify_isolated_sqlite_backup_restore`, `scripts/increment9_shadow_deployment.py` | Local filesystem plus SQLite copy/backup. No provider or publication. | Needs an already-frozen schema-v32 snapshot. Does not start a campaign. |
| `probe_increment9_neo4j`, CLI `neo4j` | Bounded local Bolt/Neo4j probe. Credentials from `NEWSROOM_INCREMENT9_NEO4J_*`. `admit_readiness_egress()` allows only `bolt`/`neo4j` on `127.0.0.1`/`localhost`/`::1`. | Requires a running Neo4j `5.26.2` with database `increment9`. Secret values never enter receipts. This fork does not ship the GitHub Actions readiness workflow that previously supplied that service. |
| `probe_macm4_capacity`, CLI `capacity` | The function runs on Linux (`os.sysconf`, `platform.machine()`, optional `/usr/sbin/sysctl`). | `matches_od003` is true only for `arm64` plus `Mac16,10` plus `Apple M4` plus 10 cores plus 16 GiB. On this Linux computer it records `chip=UNAVAILABLE` and cannot qualify OD-003. |

`qualification.default_probe()` is the opposite of a live writer: it proves publication/Discord/Neo4j/SQLite writer routes fail closed (for example it asks `admit_readiness_egress("neo4j://production.example:7687")` and expects `DeploymentError`).

### Not a Linux first-boot runtime

These exist as bytes and tests, but they are not a host you can start here:

- Owner plan `OD-003` / `OD-007` / `OD-012` bind Hermes on macm4, macOS Keychain or equivalent, and `CAPACITY_MACM4` (`newsroom/increment9/shadow_plan_v1.json`).
- 9B3 first I/O remains blocked. Twenty First I/O Gates are still `MISSING` (`docs/adr/0008-increment-9q-qualification-evidence-before-gate-records.md`).
- Hermes Control Plane, Hermes Publication Admission, and Autonomy Envelope are accepted Increment 9 vocabulary in `CONTEXT.md`. Map #1 says this fork is a trusted-operator host and must not treat those as if they apply unless a later ticket decides they do.

## Foundation (`newsroom/authority/` and Increment 1C)

### Fixture-only

`README.md` and `docs/operations/increment-1c-integrated-foundation.md` state the accepted foundation:

- SQLite ledger, governed objects, Retrieval Context, and Candidate tables are authoritative.
- The Increment 1C proof is one synthetic, deterministic path through authenticated command authority, SQLite, governed-object hydration, an authority-selected Neo4j structural projection, and Candidate admission.
- It authorises no live source, Graphiti, model, embedding, publication, shadow, canary, production activation, spend, or public effect.

What that means in code:

- `newsroom/authority/` is a local SQLite event ledger plus command/object/policy modules. Tests such as `newsroom/tests/test_authority_a2b_sqlite.py` open `sqlite3.connect(":memory:")`.
- `newsroom/authority/auth.py` authenticates an in-process `AuthenticationProof`. That is a typed test/command token, not a cloud or Keychain secret.
- `newsroom/authority/increment9_shadow_migrations.py` installs a standalone shadow schema and refuses a production `user_version`.
- Most `newsroom/tests/test_integrated_c1_*.py` and `test_authority_*.py` files are this fixture path. `uv run --no-sync python -m pytest -q newsroom/tests` is the documented deterministic suite.

### Linux-runnable actual-service (optional)

`newsroom/tests/test_integrated_c1_neo4j_service.py` skips unless `NEWSROOM_NEO4J_SERVICE_REQUIRED=1`, then uses `Neo4jProjectorConfig.from_environment()`. `docs/operations/increment-1c-integrated-foundation.md` says the permanent gate used a pinned Neo4j Community image, runtime-generated masked credentials, loopback Bolt, and a dedicated `newsroom_projector` identity.

On this fork that CI workflow is not shipped (`d950336` removed `.github/workflows/`). The Python can still talk to a local Neo4j if an operator starts one. That is Linux-runnable service I/O, not fixture-only, and it is still not live source or publication.

`newsroom/authority/neo4j_admitted_graph_reader.py` and `neo4j_fulltext_reader.py` are the same class of code: they need an admitted local graph, not a public target.

## Runner (`newsroom/runner.py`)

`newsroom/runner.py` and `scripts/newsroom_runner.py` are the **legacy OpenClaw Discord runtime**, not Increment 9.

- README Key Areas: legacy story-writing and publishing runtime.
- Live imports: `GatewayClient`, `requests`, Brave Search, Gemini (`GEMINI_API_KEY`), image fetch, Discord publish.
- `scripts/newsroom_runner.py --dry-run` validates and renders prompts and does not post, spawn, or modify job files. The process still constructs `GatewayClient` from `load_gateway_config()` (default `http://127.0.0.1:3000` plus `OPENCLAW_GATEWAY_TOKEN`).
- Without `--dry-run` it can spawn workers, call Gemini, and publish to Discord.

Tests (`newsroom/tests/test_runner_hardening.py` and siblings) mock the gateway. Those tests are fixture-only. The module itself is a live publication path.

`AGENTS.md` documents the planner/runner cron architecture (Brave, GDELT, RSS, Gemini, Discord). That is the older product boundary. README: its existence does not make it the accepted authority architecture or a production-admitted path.

OpenClaw Gateway, Discord, and Gemini credentials are not present as first-boot contracts on this fork. Map #1 still lists whether OpenClaw or Discord ever appear on this fork as unspecified.

## Short list

### Fixture-only (no credential, network, provider, or publication)

- Increment 9R/9A1/9B1/9B2/9C1/9D1/9D2/9G/9Q-1 modules and their seal/assess CLIs
- Isolated Increment 9 shadow SQLite (`epoch.py`, `increment9_shadow_migrations.py`)
- `newsroom/authority/` SQLite ledger, objects, policy, and in-memory tests
- Increment 1C synthetic foundation proof (non-service tests)
- Runner unit tests with a dummy gateway

### Actually runnable on Linux

- 9P `scripts/increment9_proving_store.py fetch` — public HTTPS, isolated SQLite, no publication
- 9A2 materialise / backup-restore / teardown — local filesystem and SQLite
- 9A2 Neo4j probe — only with a local `neo4j:5.26.2` and env credentials
- Increment 1C / projection actual-service tests — only with `NEWSROOM_NEO4J_SERVICE_REQUIRED=1` and a local graph
- `scripts/newsroom_runner.py` — legacy live path; `--dry-run` avoids post/spawn but still wants a Gateway config

### Runs as Python, cannot qualify here

- `probe_macm4_capacity` / `CAPACITY_MACM4` / Hermes-on-macm4 plan bindings
- 9B3 campaign launch (blocked before first I/O)
- Any publication, Evidence Intake, canary, or production writer route

## Implications for first boot

A Linux first boot can run the deterministic pytest suite and the 9Q-1 / 9B3-blocked / 9P-assess CLIs without credentials. A live 9P fetch is the only Increment 9 network action that exists as code. Neo4j and the legacy runner are optional operator-supplied services, not a start path in this repository. They are not first-boot defaults.
