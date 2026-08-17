"""External envelope-grant seam: owner-signed ledger row, emergency stop holds."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import subprocess
import sys
import time
from pathlib import Path

from newsroom.first_boot import SHARED_HOME, ledger_path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "newsroom_first_boot.py"

CONTROLLER_ID = "grok_bot.agent_turn_controller"
OWNER_PRINCIPAL = "owner.newsroom"
EVENT_TYPE = "autonomy.envelope.granted"
ALLOWED_ACTS = {
    "gated_x_search",
    "official_source_definition_rss",
    "planning",
    "publication_decisions",
}
PAUSE_RESTORE_NAME = "restore.paused"


def _cli(*args: str, home: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    merged["PATH"] = str(home.parent / "bin") + os.pathsep + merged.get("PATH", "")
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args, "--home", str(home)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=merged,
        check=False,
    )


def _install_uv_stub(tmp_path: Path) -> None:
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    record = tmp_path / "uv-record"
    record.mkdir()
    stub = stub_dir / "uv"
    stub.write_text(
        "#!/bin/sh\n"
        "set -e\n"
        f'RECORD="{record}"\n'
        'printf "%s" "$UV_PROJECT_ENVIRONMENT" > "$RECORD/uv_project_environment"\n'
        'printf "%s" "$*" > "$RECORD/uv_args"\n'
        'mkdir -p "$UV_PROJECT_ENVIRONMENT/bin"\n'
        f'ln -sfn "{sys.executable}" "$UV_PROJECT_ENVIRONMENT/bin/python"\n'
        f'ln -sfn "{sys.executable}" "$UV_PROJECT_ENVIRONMENT/bin/python3"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)


def _stop(home: Path, *, pause_restore: bool = False) -> None:
    args = ("stop", "--pause-restore") if pause_restore else ("stop",)
    _cli(*args, home=home)


def _read_grants(db: Path) -> list[tuple[str, str, dict[str, object]]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            "SELECT e.event_type, e.principal_id, p.payload_bytes "
            "FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type=?",
            (EVENT_TYPE,),
        ).fetchall()
    grants: list[tuple[str, str, dict[str, object]]] = []
    for event_type, principal_id, payload_bytes in rows:
        payload = json.loads(bytes(payload_bytes))
        assert isinstance(payload, dict)
        grants.append((str(event_type), str(principal_id), payload))
    return grants


def test_shared_ledger_path_stays_on_the_box_home() -> None:
    assert SHARED_HOME == Path("/home/box/newsroom")
    assert ledger_path(SHARED_HOME) == Path("/home/box/newsroom/data/authority.sqlite3")


def test_envelope_grant_is_absent_after_first_boot_only(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        db = ledger_path(home)
        assert db.is_file()
        assert _read_grants(db) == []
        with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
            assert conn.execute("SELECT COUNT(*) FROM discovery_signals").fetchone()[0] == 0
            assert conn.execute("SELECT COUNT(*) FROM ledger_events").fetchone()[0] == 0
    finally:
        _stop(home)


def test_grant_envelope_records_owner_signed_controller_and_allowed_acts(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr

        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        report = json.loads(granted.stdout)
        assert report["ok"] is True
        assert report["ledger_path"] == str(ledger_path(home))

        grants = _read_grants(ledger_path(home))
        assert len(grants) == 1
        event_type, principal_id, payload = grants[0]
        assert event_type == EVENT_TYPE
        assert principal_id == OWNER_PRINCIPAL
        assert payload["controller"] == CONTROLLER_ID
        assert set(payload["allowed_acts"]) == ALLOWED_ACTS
        assert payload["auto_publish"] is False
        assert payload["hermes_control_plane"] is False
        assert payload["public_adapter"] is False
        assert payload["emergency_stop_retained"] is True
        assert payload["fail_closed_gates_retained"] is True

        with sqlite3.connect(f"file:{ledger_path(home)}?mode=ro", uri=True) as conn:
            assert conn.execute("SELECT COUNT(*) FROM discovery_signals").fetchone()[0] == 0
            assert conn.execute("SELECT COUNT(*) FROM ledger_events").fetchone()[0] == 1

        again = _cli("grant-envelope", home=home)
        assert again.returncode == 0, again.stderr
        assert len(_read_grants(ledger_path(home))) == 1
    finally:
        _stop(home)


def test_emergency_stop_still_holds_after_envelope_grant(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    pause = home / "logs" / PAUSE_RESTORE_NAME
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        assert len(_read_grants(ledger_path(home))) == 1

        stopped = _cli("stop", "--pause-restore", home=home)
        assert stopped.returncode == 0, stopped.stderr
        health = json.loads(_cli("health", home=home).stdout)
        assert health["process_up"] is False
        assert health["pid"] is None
        assert pause.is_file()

        time.sleep(1.2)
        still = json.loads(_cli("health", home=home).stdout)
        assert still["process_up"] is False
        assert still["pid"] is None
        assert pause.is_file()

        auto = _cli("start", home=home)
        assert auto.returncode != 0
        refused = json.loads(_cli("health", home=home).stdout)
        assert refused["process_up"] is False
        assert pause.is_file()
        assert len(_read_grants(ledger_path(home))) == 1
    finally:
        _stop(home, pause_restore=True)
