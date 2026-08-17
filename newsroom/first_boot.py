"""Grok Bot first-boot bring-up: uv sync, host process, empty SQLite ledger."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

SHARED_HOME = Path("/home/box/newsroom")
_HOLD_WAIT_SECONDS = 60.0
_HOLD_POLL_SECONDS = 0.2


class FirstBootError(RuntimeError):
    """Bring-up or health assessment failed."""


def ledger_path(home: Path) -> Path:
    return Path(home) / "data" / "authority.sqlite3"


def venv_path(home: Path) -> Path:
    return Path(home) / ".venv"


def pid_path(home: Path) -> Path:
    return Path(home) / "logs" / "store.pid"


def resolve_project_root(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return Path(explicit).expanduser().resolve()
    for candidate in (Path(__file__).resolve().parents[1], Path.cwd()):
        if (candidate / "pyproject.toml").is_file() and (candidate / "uv.lock").is_file():
            return candidate
    raise FirstBootError("cannot find newsroom checkout (pyproject.toml + uv.lock)")


def process_is_up(home: Path) -> bool:
    pid = _read_pid(home)
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def assess_health(home: Path) -> dict[str, Any]:
    db = ledger_path(home)
    exists = db.is_file() and not db.is_symlink()
    up = process_is_up(home)
    empty = False
    signal_count: int | None = None
    event_count: int | None = None
    if exists:
        try:
            empty, signal_count, event_count = _read_ledger_emptiness(db)
        except sqlite3.Error:
            empty = False
    return {
        "ok": bool(up and exists and empty),
        "process_up": up,
        "ledger_exists": exists,
        "ledger_empty": empty,
        "ledger_path": str(db),
        "home": str(home),
        "pid": _read_pid(home) if up else None,
        "discovery_signal_count": signal_count,
        "ledger_event_count": event_count,
    }


def start(
    home: Path,
    *,
    project_root: Path | None = None,
    uv_bin: str = "uv",
) -> dict[str, Any]:
    root = resolve_project_root(project_root)
    _prepare_home(home)
    _sync_venv(home, root, uv_bin)
    if process_is_up(home) and assess_health(home)["ok"]:
        return _start_report(home, pid=_read_pid(home))
    if process_is_up(home):
        stop(home)
    _spawn_hold(home, root)
    report = _wait_healthy(home)
    if not report["ok"]:
        stop(home)
        raise FirstBootError(
            "first-boot health did not pass after bring-up: "
            + json.dumps(report, sort_keys=True)
        )
    return _start_report(home, pid=report.get("pid"))


def stop(home: Path) -> dict[str, Any]:
    pid = _read_pid(home)
    if pid is not None and process_is_up(home):
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and process_is_up(home):
            time.sleep(0.05)
        if process_is_up(home):
            os.kill(pid, signal.SIGKILL)
    _clear_pid(home)
    return {"ok": True, "home": str(home)}


def hold(home: Path) -> int:
    _prepare_home(home)
    stopped = threading.Event()

    def _request_stop(_signum: int, _frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    system = _open_host_store(ledger_path(home))
    try:
        _write_pid(home, os.getpid())
        while not stopped.is_set():
            stopped.wait(timeout=3600)
    finally:
        system.close()
        _clear_pid(home)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="First-boot bring-up and health for the trusted-operator host."
    )
    parser.add_argument("command", choices=("start", "health", "stop", "hold"))
    parser.add_argument("--home", default=str(SHARED_HOME))
    parser.add_argument("--project-root")
    args = parser.parse_args(argv)
    home = Path(args.home).expanduser().resolve()
    project_root = Path(args.project_root).expanduser() if args.project_root else None
    try:
        if args.command == "health":
            report = assess_health(home)
            _print_json(report)
            return 0 if report["ok"] else 1
        if args.command == "stop":
            _print_json(stop(home))
            return 0
        if args.command == "hold":
            return hold(home)
        _print_json(start(home, project_root=project_root))
        return 0
    except FirstBootError as exc:
        print(str(exc), file=sys.stderr)
        _print_json({"ok": False, "error": str(exc), "home": str(home)})
        return 1


def _start_report(home: Path, *, pid: int | None) -> dict[str, Any]:
    return {
        "ok": True,
        "uv_sync": True,
        "home": str(home),
        "venv": str(venv_path(home)),
        "ledger_path": str(ledger_path(home)),
        "pid": pid,
    }


def _prepare_home(home: Path) -> None:
    for relative in (".", "data", "logs"):
        path = home if relative == "." else home / relative
        path.mkdir(parents=True, exist_ok=True)
        os.chmod(path, 0o700)


def _sync_venv(home: Path, project_root: Path, uv_bin: str) -> None:
    env = os.environ.copy()
    env["UV_PROJECT_ENVIRONMENT"] = str(venv_path(home))
    completed = subprocess.run(
        [uv_bin, "sync", "--locked"],
        cwd=str(project_root),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "uv sync failed").strip()
        raise FirstBootError(f"uv sync into {venv_path(home)} failed: {detail}")


def _spawn_hold(home: Path, project_root: Path) -> None:
    python = venv_path(home) / "bin" / "python"
    if not python.is_file():
        raise FirstBootError(f"shared-home venv python missing: {python}")
    logs = home / "logs"
    with (logs / "store.out.log").open("a", encoding="utf-8") as out_f, (
        logs / "store.err.log"
    ).open("a", encoding="utf-8") as err_f:
        subprocess.Popen(
            [str(python), "-m", "newsroom.first_boot", "hold", "--home", str(home)],
            cwd=str(project_root),
            stdout=out_f,
            stderr=err_f,
            start_new_session=True,
        )


def _wait_healthy(home: Path) -> dict[str, Any]:
    deadline = time.monotonic() + _HOLD_WAIT_SECONDS
    report = assess_health(home)
    while time.monotonic() < deadline:
        report = assess_health(home)
        if report["ok"]:
            return report
        time.sleep(_HOLD_POLL_SECONDS)
    return report


def _read_pid(home: Path) -> int | None:
    path = pid_path(home)
    if not path.is_file() or path.is_symlink():
        return None
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except ValueError:
        return None


def _write_pid(home: Path, pid: int) -> None:
    path = pid_path(home)
    path.write_text(f"{pid}\n", encoding="utf-8")
    os.chmod(path, 0o600)


def _clear_pid(home: Path) -> None:
    path = pid_path(home)
    if path.is_file() and not path.is_symlink():
        path.unlink()


def _read_ledger_emptiness(path: Path) -> tuple[bool, int, int]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "discovery_signals" not in tables or "ledger_events" not in tables:
            return False, 0, 0
        signals = int(
            conn.execute("SELECT COUNT(*) FROM discovery_signals").fetchone()[0]
        )
        events = int(conn.execute("SELECT COUNT(*) FROM ledger_events").fetchone()[0])
        return signals == 0 and events == 0, signals, events
    finally:
        conn.close()


def _open_host_store(path: Path) -> Any:
    from newsroom.authority import (
        CommandDefinition,
        CommandRegistry,
        EventReadPolicy,
        MetadataClass,
        PayloadGoldenVector,
        PayloadMode,
        PayloadSchemaContract,
        PayloadSchemaRegistry,
        StaticAuthenticator,
        StaticAuthorizer,
        StaticPrincipal,
        TrustScope,
        open_authority_event_system,
    )

    def canonicalize_none(value: object) -> bytes:
        if value is None:
            return b""
        raise ValueError("host hold accepts no payload")

    contract = PayloadSchemaContract(
        schema_version="host_hold_v1",
        payload_mode=PayloadMode.NO_PAYLOAD,
        contract_version="host-hold-contract-v1",
        canonicalizer_implementation_version="host-hold-none-v1",
        canonicalizer=canonicalize_none,
        golden_vectors=(
            PayloadGoldenVector(
                name="empty",
                input_identity="none-v1",
                value=None,
                expected_bytes=b"",
            ),
        ),
    )
    definition = CommandDefinition(
        command_type="host.hold",
        definition_version="v1",
        aggregate_type="host.process",
        event_type="host.hold.recorded",
        event_schema_version=1,
        payload_mode=PayloadMode.NO_PAYLOAD,
        payload_schema_version=contract.schema_version,
        payload_schema_contract_version=contract.contract_version,
        payload_schema_contract_digest=contract.contract_digest,
        payload_canonicalizer_version=contract.canonicalizer_implementation_version,
        trust_scope=TrustScope.OBSERVED,
        security_scope="authority.host",
        retention_scope="authority.host",
        required_scope="authority.host.hold",
    )
    return open_authority_event_system(
        path=path,
        registry=CommandRegistry([definition]),
        payload_schemas=PayloadSchemaRegistry((contract,)),
        authenticator=StaticAuthenticator(
            credentials={
                "host-process": StaticPrincipal(
                    "host.newsroom",
                    assurance_class="HOST_PROCESS",
                )
            },
            authority_domain="newsroom.host",
        ),
        authorizer=StaticAuthorizer(
            policy_version="host-hold-v1",
            grants_by_principal={
                "host.newsroom": frozenset(
                    {"authority.host.hold", "authority.host.read"}
                )
            },
        ),
        event_read_policy=EventReadPolicy(
            policy_id="host-hold-read-v1",
            purpose="host.hold.audit",
            required_scope="authority.host.read",
            allowed_principal_ids=frozenset({"host.newsroom"}),
            allowed_security_scopes=frozenset({"authority.host"}),
            allowed_trust_scopes=frozenset({TrustScope.OBSERVED}),
            metadata_classes=frozenset(
                {
                    MetadataClass.ROUTING,
                    MetadataClass.PROVENANCE,
                    MetadataClass.RESULT,
                }
            ),
        ),
    )


def _print_json(value: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(value, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
