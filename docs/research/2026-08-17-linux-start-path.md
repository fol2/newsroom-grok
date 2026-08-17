# Linux start path in newsroom-grok today

Date: 2026-08-17  
Ticket: https://github.com/fol2/newsroom-grok/issues/7  
Sources: HEAD `main` (`d950336c6bd5e3b3dcf3125b10f3fdcaab6f2969`) via GitHub API only. No working-tree clone. No product-code edits.

## Question

What are the actual entry points, `uv` install steps, SQLite init, and start commands on Linux in `fol2/newsroom-grok` today? What fails if Keychain, launchd, and Hermes are absent?

## Method

Read the current tree and file contents through `gh api repos/fol2/newsroom-grok/...`. Primary files: `README.md`, `AGENTS.md`, `CONTRIBUTING.md`, `pyproject.toml`, `.env.example`, `.gitignore`, `scripts/`, `newsroom/`, plus `docs/adr/0007-hermes-autonomy-envelope-and-conditional-activation.md` and `docs/operations/increment-1c-integrated-foundation.md` for the Hermes / foundation boundary.

GitHub code search for `keychain`, `launchd`, `Hermes`, `linux`, and `darwin` returned zero indexed hits (and then rate-limited). The recursive tree listing also contains no `launchd` plist, no systemd unit, no Keychain helper, and no Hermes binary. The only Hermes document is ADR 0007.

## Two product boundaries

`README.md` states the repo holds two things that are not the same product:

1. **Accepted Increment 1 foundation** — authenticated command authority, SQLite ledger, governed-object hydration, authority-selected Neo4j projection, SQLite-authoritative Candidate admission. This is a foundation proof, not production admission. It authorises no live source access, Graphiti/model/embedding execution, publication, product shadow, canary, production activation, spending, or public effect.
2. **Legacy OpenClaw Discord runtime** — ingest → cluster → write stories → optional Discord publish through a separate OpenClaw Gateway. Its presence does not make it the accepted authority architecture.

There is no Linux-specific start document, no Grok Bot runtime, and no in-repo daemon that replaces Hermes.

## uv install on Linux

Prerequisites from `README.md`: Python 3.12+ and [uv](https://docs.astral.sh/uv/). OpenClaw Gateway is required only for the legacy Discord publishing path and is not included in this repository.

Documented install (`README.md`; clone URL there still points at `fol2/newsroom`, not `fol2/newsroom-grok`):

```bash
git clone https://github.com/fol2/newsroom.git
cd newsroom
uv sync --dev --locked
```

Optional charts extra: `uv sync --extra charts --locked`.

`CONTRIBUTING.md` uses the unlocked variant `uv sync --dev`.

Then copy env:

```bash
cp .env.example .env
```

`.env.example` is env-file only: `GEMINI_API_KEY`, `BRAVE_SEARCH_API_KEY` / `BRAVE_SEARCH_API_KEYS`, `OPENCLAW_GATEWAY_TOKEN`, plus optional `OPENCLAW_HOME`, `OPENCLAW_GATEWAY_HTTP_URL`, Gemini OAuth profile paths, and `NANO_BANANA_SCRIPT`. There is no Keychain, launchd, or Hermes variable.

Locked test suite (`README.md`):

```bash
uv lock --check
uv sync --dev --locked
uv run --no-sync python -m pytest -q newsroom/tests
```

`CONTRIBUTING.md`: `uv run python -m pytest newsroom/tests/ -v`.

`.gitignore` excludes `.venv/`, `.env`, `secrets/`, `data/`, `logs/`, `workspace/jobs/`, `*.sqlite3`, and `*.db`. A fresh Linux checkout therefore has no database, no secrets, and no `openclaw.json`.

## Actual entry points

There is no `__main__.py` package entry and no OS service. Runnable surfaces today:

### Console scripts (`pyproject.toml` `[project.scripts]`)

These wrap `scripts/*.py` `main(argv)` via `scripts/_cli.py`:

| Console script | Module |
|---|---|
| `news-pool-update` | `scripts.news_pool_update` |
| `gdelt-pool-update` | `scripts.gdelt_pool_update` |
| `rss-pool-update` | `scripts.rss_pool_update` |
| `news-pool-status` | `scripts.news_pool_status` |
| `newsroom-hourly-inputs` | `scripts.newsroom_hourly_inputs` |
| `newsroom-daily-inputs` | `scripts.newsroom_daily_inputs` |
| `newsroom-write-run-job` | `scripts.newsroom_write_run_job` |
| `newsroom-runner` | `scripts.newsroom_runner` |
| `newsroom-clustering-decisions` / `newsroom-decision-log-inspector` | `scripts.newsroom_clustering_decisions` |
| `build-clustering-eval-dataset` | `scripts.build_clustering_eval_dataset` |
| `replay-clustering-eval-dataset` | `scripts.replay_clustering_eval_dataset` |
| `eval-clustering-metrics` | `scripts.eval_clustering_metrics` |

### Direct script entry points (`scripts/`)

Same programs are also invoked as `uv run python scripts/<name>.py`. Additional scripts exist without console_script wrappers (`fix_pool_integrity.py`, increment 5/9/10 campaign scripts, `scripts/sdlc/*`). Those are evidence / qualification tools, not a Linux boot path.

Scripts set `OPENCLAW_HOME` to the **repository root** (`Path(__file__).resolve().parents[1]`) when unset. They do not detect Linux vs macOS.

### Library entry (accepted foundation)

`newsroom/authority/integrated_system.py` exports `open_candidate_admission_authority_system(...)`. The caller must supply a SQLite `path`, command/payload registries, authn/authz, read policies, and a `Neo4jProjectorConfig`. There is no CLI, no default database path, and no start command. Increment 1C (`docs/operations/increment-1c-integrated-foundation.md`) is an evidence boundary: no scheduler, no live sources, no publication.

### Scheduling entry (legacy only)

`newsroom/CRON.md` and `AGENTS.md`:

- OpenClaw cron runs the **planner** (LLM turn). That is the `openclaw cron add` CLI, not an in-repo binary.
- The **runner** is spawned on demand by `scripts/newsroom_write_run_job.py --launch-runner`, which `Popen`s `uv run python3 scripts/newsroom_runner.py --path <run_dir> --summary-only` (`start_new_session=True`).
- `CRON.md` also describes OS `cron` as an optional way to poll the runner every minute. No crontab, launchd plist, or systemd unit is checked in.

## SQLite init

Two SQLite worlds exist. Neither is Linux-specific.

### 1. Legacy news pool

`newsroom/news_pool_db.py` `NewsPoolDB.__init__(path)`:

- creates the parent directory
- `sqlite3.connect(..., timeout=30, isolation_level=None)`
- `PRAGMA foreign_keys=ON`, `journal_mode=WAL`, `synchronous=NORMAL`
- `_ensure_schema()`: `CREATE TABLE IF NOT EXISTS meta`, then create schema v5 tables or migrate through v6–v8 (`SCHEMA_VERSION = 8`)

Default path used by `scripts/news_pool_update.py`:

`<OPENCLAW_HOME>/data/newsroom/news_pool.sqlite3`

`scripts/newsroom_hourly_inputs.py` hardcodes the same path as `_DB_PATH` and has **no** `--db` flag. `README.md` documents `newsroom_hourly_inputs.py --db data/newsroom/news_pool.sandbox.sqlite3`; that flag is not implemented. Sandbox isolation therefore requires calling `news_pool_update.py --db ...` directly, or pointing the whole repo’s `data/newsroom/` tree at a disposable file.

Dump/restore (`README.md`):

```bash
uv run python scripts/news_pool_dump_jsonl.py \
  --db data/newsroom/news_pool.sqlite3 \
  --out-dir data/newsroom/db_dumps/<UTC-stamp>

uv run python scripts/news_pool_restore_jsonl.py \
  --dump-dir data/newsroom/db_dumps/<TIMESTAMP> \
  --db data/newsroom/news_pool.sandbox.sqlite3
```

### 2. Authority ledger (Increment 1)

`newsroom/authority/_event_store_base.py` `_EventStoreBase`:

- requires POSIX `fcntl` advisory locking (`AuthoritySchemaError` if `fcntl` is missing; Linux satisfies this)
- parent directory `0o700`, database file `0o600`, no group/other bits, owner must be the writer, no symlinks
- exclusive `.writer.lock` via `fcntl.flock`
- `sqlite3.connect` with `PRAGMA foreign_keys=ON`, `journal_mode=WAL` (fails closed if WAL unavailable), `synchronous=FULL`
- auto-applies pending migrations and validates fingerprint / `quick_check` / foreign keys

`open_candidate_admission_authority_system` opens that store at a **caller-supplied** path and also requires a live Neo4j adapter. There is no “init-db” script and no default Linux path.

## Start commands that exist today

### Accepted foundation

There is no start command. Qualification is tests / CI (`.github/workflows` Projection B2/B3/C1 Neo4j lane in the Increment 1C guide). On Linux without Neo4j and without the actual-service credentials that workflow generates, the integrated proof does not start.

### Legacy dry-run path documented in `README.md`

```bash
uv run python scripts/news_pool_update.py \
  --db data/newsroom/news_pool.sandbox.sqlite3

uv run python scripts/newsroom_hourly_inputs.py \
  --db data/newsroom/news_pool.sandbox.sqlite3   # flag does not exist; see above

uv run python scripts/newsroom_runner.py --dry-run
```

`newsroom/README.md` also documents:

```bash
uv run python3 scripts/newsroom_runner.py
uv run python3 scripts/newsroom_runner.py --dry-run
uv run python3 scripts/newsroom_runner.py --path <run_dir_or_story.json>
```

`CONTRIBUTING.md` claims most features work without API keys when using `--dry-run`. That is only partly true: `scripts/newsroom_runner.py` always calls `load_gateway_config(openclaw_home)` **before** constructing the runner, including in `--dry-run`.

`newsroom/gateway_client.py` `load_gateway_config`:

- reads `<OPENCLAW_HOME>/openclaw.json` (not in the repo tree)
- token from `OPENCLAW_GATEWAY_TOKEN` / `CLAWDBOT_GATEWAY_TOKEN` / `gateway.auth.token`
- URL from `OPENCLAW_GATEWAY_HTTP_URL` / `OPENCLAW_GATEWAY_URL`, else `http://127.0.0.1:<gateway.port>`

Missing `openclaw.json` or token raises `RuntimeError` / `FileNotFoundError` and the runner never starts.

### Legacy live path (needs extras that are not in this repo)

1. Populate pool: `uv run python scripts/news_pool_update.py` (Brave keys) and/or `rss_pool_update.py` / `gdelt_pool_update.py`.
2. Cluster + select: `uv run python scripts/newsroom_hourly_inputs.py` or `newsroom_daily_inputs.py` (Gemini via `newsroom/gemini_client.py`).
3. Write jobs: `uv run python scripts/newsroom_write_run_job.py --run-time-uk '...' --launch-runner`.
4. Runner talks to OpenClaw Gateway at loopback (default port from `openclaw.json`) to spawn workers and post to Discord.

Brave keys (`newsroom/brave_news.py` `load_brave_api_keys`): env `BRAVE_SEARCH_API_KEYS` / `BRAVE_SEARCH_API_KEY`, else `secrets/brave_search_api_keys.local.txt` / `secrets/brave_search_api_keys.txt`, else legacy single-key files. Missing keys raise `RuntimeError`.

Gemini (`newsroom/gemini_client.py`): OAuth profiles from `GEMINI_AUTH_PROFILES` or `~/.openclaw/agents/main/agent/auth-profiles.json`; fallback `GEMINI_API_KEY` / repo `.env`. The HTTP `User-Agent` is already `gemini-cli/0.1.0 linux/x64`. Tokens are JSON file / env, not Keychain.

## What fails if Keychain, launchd, and Hermes are absent

These three are **not runtime dependencies of this repository**. Their absence does not change the Python install or SQLite init. What fails is any expectation that macOS / `macm4` orchestration is present.

### Keychain

- No Keychain API, helper, or secret-broker module is in the tree.
- ADR 0007 says Hermes on `macm4` “uses a local credential broker backed by macOS Keychain or an equivalent local secret store.” That broker is not implemented here.
- This repo’s secrets are env vars, `.env`, `secrets/*.txt`, and `auth-profiles.json`.
- **Does not fail:** `uv sync`, pytest (no live keys), `NewsPoolDB` schema create, authority store open (given a path and POSIX `fcntl`), Gemini API-key mode, Brave/Gemini calls once env/files are present.
- **Does fail, independently of Keychain:** any path that needs secrets you did not put in env/files; runner start without `openclaw.json` + gateway token; Brave fetch without a key; Gemini clustering without API key or OAuth JSON.

### launchd

- No launchd job, plist, or `darwin` branch exists.
- Legacy scheduling is OpenClaw cron (external) plus on-demand `uv` subprocess, or optional OS cron described in `CRON.md` but not shipped.
- **Does not fail:** manual `uv run` of any script; pytest; SQLite init.
- **Does fail:** automatic hourly/daily planner and runner if you expected a Mac launchd agent or Hermes to start them. Linux has no substitute unit in this repo. `write_run_job --launch-runner` still works if `uv` is on `PATH`.

### Hermes

- Hermes is a planning/ops decision in ADR 0007, bound to host `macm4`. It is the local orchestration, admission, and delivery hub with Conditional Activation Authority for Increments 9 and 10.
- This repo does not contain a Hermes runtime, control plane, credential broker, or publication adapter.
- Increment 1C explicitly has no scheduler and no public effect.
- **Does not fail:** library import, locked pytest, legacy script execution, SQLite pool/ledger init.
- **Does fail:** autonomous Increment 9/10 delivery, publication admission, canary promotion, Keychain-backed secret injection, and any “start the newsroom” path that assumed Hermes was the process supervisor. Those capabilities are not startable from this tree on Linux or elsewhere.

## Linux-specific facts that are in the code

- Authority locking is POSIX `fcntl` — Linux is a supported runtime for the ledger; Windows would fail closed.
- Gemini client already advertises `linux/x64`.
- Scripts are POSIX (`#!/usr/bin/env python3`, `uv`, `which("uv")`).
- There is no Linux start path beyond “install uv, `uv sync --dev --locked`, run a script or pytest.”
- The accepted architecture still requires Neo4j for the integrated proof; that is a service dependency, not a Mac one.

## Bottom line

On Linux today, `fol2/newsroom-grok` starts as a **uv-managed Python library plus CLI scripts**. SQLite self-initialises on first `NewsPoolDB` or `_EventStoreBase` open. There is no Hermes, Keychain, or launchd integration to miss. What you cannot do without those (and without OpenClaw Gateway / Neo4j / API keys) is run the Mac-hosted autonomous newsroom or the legacy Discord publisher. What you can do is install, migrate a disposable pool DB, and run the locked test suite.
