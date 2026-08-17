"""External AUTO-010 commit seam: PD + TO on the host ledger, no public write."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

from newsroom.first_boot import SHARED_HOME, ledger_path
from newsroom.increment9.proving import ALLOWED_HOSTS, SOURCE_URLS, assert_allowed_url
from newsroom.increment9.qualification import GATE_ID as NONMUTATION_GATE
from newsroom.increment9.shadow_contracts import ProhibitedEffect, _NoEffect
from newsroom.increment10.requalification import EXPECTED_RESIDUAL_GATES


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "newsroom_first_boot.py"

CONTROLLER_ID = "grok_bot.agent_turn_controller"
HOST_PRINCIPAL = "host.newsroom"
DECISION_EVENT_TYPE = "publication.decision.authorised"
OPERATION_EVENT_TYPE = "target.operation.dispatched"
SIGNAL_EVENT_TYPE = "discovery.signal.admitted"
GRANT_EVENT_TYPE = "auto_publish.granted"
PAUSE_RESTORE_NAME = "restore.paused"
HOLD_NAME = "publication.hold"
LEDGER_TARGET = "host.authority.ledger"
HK01_URL = SOURCE_URLS["HK-01"]
RSS_BODY = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<rss version="2.0"><channel>'
    "<item><guid>hk-01-first-item</guid>"
    "<title>Official RSS item</title>"
    "<link>https://www.news.gov.hk/item-1</link>"
    "</item></channel></rss>\n"
)


def _cli(
    *args: str,
    home: Path,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
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


def _rss_env(tmp_path: Path) -> dict[str, str]:
    feed = tmp_path / "official-rss.xml"
    feed.write_text(RSS_BODY, encoding="utf-8")
    return {"NEWSROOM_INGEST_BODY_PATH": str(feed)}


def _bring_up_to_grant(home: Path, tmp_path: Path) -> None:
    start = _cli("start", home=home)
    assert start.returncode == 0, start.stderr
    granted = _cli("grant-envelope", home=home)
    assert granted.returncode == 0, granted.stderr
    ingested = _cli("ingest-signal", home=home, env=_rss_env(tmp_path))
    assert ingested.returncode == 0, ingested.stderr
    armed = _cli("grant-auto-publish", home=home)
    assert armed.returncode == 0, armed.stderr


def _read_events(
    db: Path, event_type: str
) -> list[tuple[str, str, dict[str, object]]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            "SELECT e.event_type, e.principal_id, p.payload_bytes "
            "FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type=? "
            "ORDER BY e.ledger_seq ASC",
            (event_type,),
        ).fetchall()
    events: list[tuple[str, str, dict[str, object]]] = []
    for event_type_value, principal_id, payload_bytes in rows:
        payload = json.loads(bytes(payload_bytes))
        assert isinstance(payload, dict)
        events.append((str(event_type_value), str(principal_id), payload))
    return events


def _count(db: Path, event_type: str) -> int:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM ledger_events WHERE event_type=?",
                (event_type,),
            ).fetchone()[0]
        )


def _write_hold(home: Path) -> Path:
    logs = home / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    path = logs / HOLD_NAME
    path.write_text("held\n", encoding="utf-8")
    return path


def test_shared_ledger_path_stays_on_the_box_home() -> None:
    assert SHARED_HOME == Path("/home/box/newsroom")
    assert ledger_path(SHARED_HOME) == Path("/home/box/newsroom/data/authority.sqlite3")


def test_fail_closed_gates_still_hold_on_this_seam() -> None:
    assert NONMUTATION_GATE == "PRODUCTION_NONMUTATION_BASELINE"
    assert "EGRESS_ALLOWLIST_ENFORCED" in EXPECTED_RESIDUAL_GATES
    rights = [gate for gate in EXPECTED_RESIDUAL_GATES if gate.startswith("RIGHTS_")]
    assert len(rights) == 10
    assert set(ProhibitedEffect) >= {
        ProhibitedEffect.PUBLICATION,
        ProhibitedEffect.DISCORD_OR_PUBLIC_DISPATCH,
        ProhibitedEffect.PRODUCTION_AUTHORITY_MUTATION,
    }
    assert _NoEffect.authorises_publication is False
    assert_allowed_url(HK01_URL)
    with pytest.raises(Exception, match="allowlist"):
        assert_allowed_url("https://discord.com/api")
    with pytest.raises(Exception, match="allowlist"):
        assert_allowed_url("https://x.com/i/api")
    assert "discord.com" not in ALLOWED_HOSTS


def test_auto_010_commit_is_absent_after_grant_auto_publish_only(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        _bring_up_to_grant(home, tmp_path)
        db = ledger_path(home)
        assert _read_events(db, GRANT_EVENT_TYPE)
        assert _read_events(db, SIGNAL_EVENT_TYPE)
        assert _read_events(db, DECISION_EVENT_TYPE) == []
        assert _read_events(db, OPERATION_EVENT_TYPE) == []
        assert _count(db, DECISION_EVENT_TYPE) == 0
        assert _count(db, OPERATION_EVENT_TYPE) == 0
    finally:
        _stop(home)


def test_auto_010_commit_records_publication_decision_and_target_operation(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        _bring_up_to_grant(home, tmp_path)

        committed = _cli("auto-010-commit", home=home)
        assert committed.returncode == 0, committed.stderr
        report = json.loads(committed.stdout)
        assert report["ok"] is True
        assert report["ledger_path"] == str(ledger_path(home))
        assert report["semantic"] == "AUTO-010"
        assert report["first_target"] == LEDGER_TARGET
        assert report["operation"] == "record"
        assert report["hold"] is False
        assert report["every_applicable_gate_passed"] is True
        assert report["auto_publish"] is False
        assert report["public_adapter"] is False
        assert report["discord"] is False
        assert "x.com" not in json.dumps(report)
        dumped = json.dumps(report).lower()
        assert "discord" not in dumped or report.get("discord") is False

        db = ledger_path(home)
        signals = _read_events(db, SIGNAL_EVENT_TYPE)
        assert len(signals) == 1
        decisions = _read_events(db, DECISION_EVENT_TYPE)
        assert len(decisions) == 1
        operations = _read_events(db, OPERATION_EVENT_TYPE)
        assert len(operations) == 1

        _event_type, principal_id, payload = decisions[0]
        assert _event_type == DECISION_EVENT_TYPE
        assert principal_id == CONTROLLER_ID
        assert principal_id != "owner.newsroom"
        assert "hermes" not in principal_id.lower()
        assert "public" not in principal_id.lower()
        assert payload["controller"] == CONTROLLER_ID
        assert payload["authorising"] is True
        assert payload["auto_publish"] is False
        assert payload["discord"] is False
        assert payload["public_adapter"] is False
        assert payload["hermes_publication_admission"] is False
        digest = payload["bundle_digest"]
        assert isinstance(digest, str)

        op_type, op_principal, op_payload = operations[0]
        assert op_type == OPERATION_EVENT_TYPE
        assert op_principal == HOST_PRINCIPAL
        assert op_payload["dispatcher"] == HOST_PRINCIPAL
        assert op_payload["bundle_digest"] == digest
        assert op_payload["target"] == LEDGER_TARGET
        assert op_payload["operation"] == "record"
        assert op_payload["auto_publish"] is False
        assert op_payload["discord"] is False
        assert op_payload["public_adapter"] is False

        assert _count(db, DECISION_EVENT_TYPE) == 1
        assert _count(db, OPERATION_EVENT_TYPE) == 1
        assert _count(db, GRANT_EVENT_TYPE) == 1
        assert _count(db, SIGNAL_EVENT_TYPE) == 1

        again = _cli("auto-010-commit", home=home)
        assert again.returncode == 0, again.stderr
        assert _count(db, DECISION_EVENT_TYPE) == 1
        assert _count(db, OPERATION_EVENT_TYPE) == 1
    finally:
        _stop(home)


def test_auto_010_commit_refuses_without_owner_signed_grant(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        ingested = _cli("ingest-signal", home=home, env=_rss_env(tmp_path))
        assert ingested.returncode == 0, ingested.stderr

        refused = _cli("auto-010-commit", home=home)
        assert refused.returncode != 0
        report = json.loads(refused.stdout)
        assert report["ok"] is False
        assert "grant" in report["error"].lower()

        db = ledger_path(home)
        assert _read_events(db, GRANT_EVENT_TYPE) == []
        assert _read_events(db, DECISION_EVENT_TYPE) == []
        assert _read_events(db, OPERATION_EVENT_TYPE) == []
    finally:
        _stop(home)


def test_hold_does_not_age_into_auto_publish(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        _bring_up_to_grant(home, tmp_path)
        hold = _write_hold(home)
        assert hold.is_file()

        first = _cli("auto-010-commit", home=home)
        assert first.returncode != 0
        report = json.loads(first.stdout)
        assert report["ok"] is False
        assert "hold" in report["error"].lower()
        assert "age" in report["error"].lower()

        db = ledger_path(home)
        assert _read_events(db, DECISION_EVENT_TYPE) == []
        assert _read_events(db, OPERATION_EVENT_TYPE) == []

        time.sleep(1.2)
        assert hold.is_file()
        aged = _cli("auto-010-commit", home=home)
        assert aged.returncode != 0
        aged_report = json.loads(aged.stdout)
        assert aged_report["ok"] is False
        assert "hold" in aged_report["error"].lower()
        assert _read_events(db, DECISION_EVENT_TYPE) == []
        assert _read_events(db, OPERATION_EVENT_TYPE) == []
        assert hold.is_file()
    finally:
        _stop(home)


def test_emergency_stop_still_holds_after_auto_010_commit(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    pause = home / "logs" / PAUSE_RESTORE_NAME
    _install_uv_stub(tmp_path)
    try:
        _bring_up_to_grant(home, tmp_path)
        committed = _cli("auto-010-commit", home=home)
        assert committed.returncode == 0, committed.stderr
        db = ledger_path(home)
        assert _count(db, DECISION_EVENT_TYPE) == 1
        assert _count(db, OPERATION_EVENT_TYPE) == 1

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
        assert _count(db, DECISION_EVENT_TYPE) == 1
        assert _count(db, OPERATION_EVENT_TYPE) == 1
    finally:
        _stop(home, pause_restore=True)
