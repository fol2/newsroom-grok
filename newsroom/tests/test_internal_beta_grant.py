"""External internal-beta grant seam: owner-signed publish target, stop holds."""

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

OWNER_PRINCIPAL = "owner.newsroom"
EVENT_TYPE = "internal_beta.granted"
AUTO_PUBLISH_EVENT = "auto_publish.granted"
PAUSE_RESTORE_NAME = "restore.paused"
BUNDLE_DIGEST = (
    "sha256:c487ece7149f0fcf7afa6808f717f2896d5e89c932d7c08519883b7ae09b1b94"
)


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


def _read_events(db: Path, event_type: str) -> list[tuple[str, str, dict[str, object]]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            "SELECT e.event_type, e.principal_id, p.payload_bytes "
            "FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type=?",
            (event_type,),
        ).fetchall()
    events: list[tuple[str, str, dict[str, object]]] = []
    for row_type, principal_id, payload_bytes in rows:
        payload = json.loads(bytes(payload_bytes))
        assert isinstance(payload, dict)
        events.append((str(row_type), str(principal_id), payload))
    return events


def test_shared_ledger_path_stays_on_the_box_home() -> None:
    assert SHARED_HOME == Path("/home/box/newsroom")
    assert ledger_path(SHARED_HOME) == Path("/home/box/newsroom/data/authority.sqlite3")


def test_internal_beta_grant_is_absent_after_first_boot_only(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        db = ledger_path(home)
        assert db.is_file()
        assert _read_events(db, EVENT_TYPE) == []
    finally:
        _stop(home)


def test_grant_internal_beta_records_owner_signed_publish_target(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr

        granted = _cli("grant-internal-beta", home=home)
        assert granted.returncode == 0, granted.stderr
        report = json.loads(granted.stdout)
        assert report["ok"] is True
        assert report["ledger_path"] == str(ledger_path(home))
        assert report["target"] == "internal.beta.origin"
        assert report["operation"] == "publish"
        assert report["bundle_digest"] == BUNDLE_DIGEST
        assert report["auto_publish"] is False
        assert report["public_adapter"] is False
        assert report["discord"] is False
        assert report["x_as_publisher"] is False
        dumped = json.dumps(report).lower()
        assert "eugnel" not in dumped
        assert "newsroom-hub.vercel.app" not in dumped
        assert "x.com" not in dumped
        assert "discord.com" not in dumped

        grants = _read_events(ledger_path(home), EVENT_TYPE)
        assert len(grants) == 1
        event_type, principal_id, payload = grants[0]
        assert event_type == EVENT_TYPE
        assert principal_id == OWNER_PRINCIPAL
        assert principal_id != "grok_bot.agent_turn_controller"
        assert payload["target"] == "internal.beta.origin"
        assert payload["operation"] == "publish"
        assert payload["source_id"] == "HK-01"
        assert payload["bundle_digest"] == BUNDLE_DIGEST
        assert payload["controller_may_arm"] is False
        assert payload["auto_publish"] is False
        assert payload["public_adapter"] is False
        assert payload["discord"] is False
        assert payload["x_as_publisher"] is False
        assert payload["emergency_stop_retained"] is True
        assert payload["fail_closed_gates_retained"] is True
        assert payload["prohibited_effect_retained"] is True
        assert payload["hermes_publication_admission"] is False
        assert "host.authority.ledger" not in payload.values()

        assert _read_events(ledger_path(home), AUTO_PUBLISH_EVENT) == []

        again = _cli("grant-internal-beta", home=home)
        assert again.returncode == 0, again.stderr
        assert len(_read_events(ledger_path(home), EVENT_TYPE)) == 1
    finally:
        _stop(home)


def test_emergency_stop_still_holds_after_internal_beta_grant(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    pause = home / "logs" / PAUSE_RESTORE_NAME
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-internal-beta", home=home)
        assert granted.returncode == 0, granted.stderr
        assert len(_read_events(ledger_path(home), EVENT_TYPE)) == 1

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
        assert len(_read_events(ledger_path(home), EVENT_TYPE)) == 1
    finally:
        _stop(home, pause_restore=True)

def test_auto_010_grant_stays_unchanged_after_internal_beta_grant(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        armed = _cli("grant-auto-publish", home=home)
        assert armed.returncode == 0, armed.stderr
        before = _read_events(ledger_path(home), AUTO_PUBLISH_EVENT)
        assert len(before) == 1
        _event_type, principal_id, payload = before[0]
        assert _event_type == AUTO_PUBLISH_EVENT
        assert principal_id == OWNER_PRINCIPAL
        assert payload["semantic"] == "AUTO-010"
        assert payload["first_target"] == "host.authority.ledger"
        assert payload["operation"] == "record"

        granted = _cli("grant-internal-beta", home=home)
        assert granted.returncode == 0, granted.stderr
        after = _read_events(ledger_path(home), AUTO_PUBLISH_EVENT)
        assert after == before
        beta = _read_events(ledger_path(home), EVENT_TYPE)
        assert len(beta) == 1
        assert beta[0][2]["target"] == "internal.beta.origin"
        assert beta[0][2]["operation"] == "publish"
        assert beta[0][2]["bundle_digest"] == BUNDLE_DIGEST
    finally:
        _stop(home)


def test_grant_internal_beta_refuses_when_restore_paused(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    pause = home / "logs" / PAUSE_RESTORE_NAME
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        stopped = _cli("stop", "--pause-restore", home=home)
        assert stopped.returncode == 0, stopped.stderr
        assert pause.is_file()

        refused = _cli("grant-internal-beta", home=home)
        assert refused.returncode != 0
        report = json.loads(refused.stdout)
        assert report["ok"] is False
        assert "emergency stop" in report["error"].lower()
        assert _read_events(ledger_path(home), EVENT_TYPE) == []
        assert pause.is_file()
    finally:
        _stop(home, pause_restore=True)


def test_grant_internal_beta_does_not_write_public_surfaces(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-internal-beta", home=home)
        assert granted.returncode == 0, granted.stderr
        report = json.loads(granted.stdout)
        dumped = json.dumps(report).lower()
        assert "eugnel" not in dumped
        assert "discord.com" not in dumped
        assert "x.com" not in dumped
        assert "newsroom-hub.vercel.app" not in dumped
        assert report["auto_publish"] is False
        assert report["public_adapter"] is False
        assert report["discord"] is False
        assert report["x_as_publisher"] is False

        db = ledger_path(home)
        assert _read_events(db, "target.operation.dispatched") == []
        payload = _read_events(db, EVENT_TYPE)[0][2]
        assert payload["auto_publish"] is False
        assert payload["public_adapter"] is False
        assert payload["discord"] is False
        assert payload["x_as_publisher"] is False
        assert payload["target"] == "internal.beta.origin"
        assert payload["target"] != "host.authority.ledger"
        values = " ".join(str(value).lower() for value in payload.values())
        assert "eugnel" not in values
        assert "discord.com" not in values
        assert "x.com" not in values
        assert "vercel.app" not in values
    finally:
        _stop(home)
