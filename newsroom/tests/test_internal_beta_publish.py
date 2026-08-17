"""External internal-beta publish TO seam: host dispatch, grant required, stop vetoes."""

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

HOST_PRINCIPAL = "host.newsroom"
OWNER_PRINCIPAL = "owner.newsroom"
CONTROLLER_ID = "grok_bot.agent_turn_controller"
EVENT_TYPE = "target.operation.dispatched"
GRANT_EVENT_TYPE = "internal_beta.granted"
DECISION_EVENT_TYPE = "publication.decision.authorised"
SIGNAL_EVENT_TYPE = "discovery.signal.admitted"
AUTO_PUBLISH_EVENT = "auto_publish.granted"
PAUSE_RESTORE_NAME = "restore.paused"
TARGET = "internal.beta.origin"
OPERATION = "publish"
LEDGER_TARGET = "host.authority.ledger"
BUNDLE_DIGEST = (
    "sha256:c487ece7149f0fcf7afa6808f717f2896d5e89c932d7c08519883b7ae09b1b94"
)
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


def _bring_up_to_decision(home: Path, tmp_path: Path) -> None:
    start = _cli("start", home=home)
    assert start.returncode == 0, start.stderr
    granted = _cli("grant-envelope", home=home)
    assert granted.returncode == 0, granted.stderr
    ingested = _cli("ingest-signal", home=home, env=_rss_env(tmp_path))
    assert ingested.returncode == 0, ingested.stderr
    minted = _cli("mint-decision", home=home)
    assert minted.returncode == 0, minted.stderr


def _bring_up_to_grant(home: Path, tmp_path: Path) -> None:
    _bring_up_to_decision(home, tmp_path)
    granted = _cli("grant-internal-beta", home=home)
    assert granted.returncode == 0, granted.stderr


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


def _publish_operations(db: Path) -> list[tuple[str, str, dict[str, object]]]:
    return [
        row
        for row in _read_events(db, EVENT_TYPE)
        if row[2].get("target") == TARGET and row[2].get("operation") == OPERATION
    ]


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


def test_internal_beta_publish_is_absent_after_grant_and_decision_only(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        _bring_up_to_grant(home, tmp_path)
        db = ledger_path(home)
        assert _read_events(db, GRANT_EVENT_TYPE)
        assert _read_events(db, DECISION_EVENT_TYPE)
        assert _read_events(db, SIGNAL_EVENT_TYPE)
        assert _publish_operations(db) == []
        assert _read_events(db, EVENT_TYPE) == []
        assert _count(db, EVENT_TYPE) == 0
    finally:
        _stop(home)


def test_dispatch_internal_beta_records_exactly_one_publish_target_operation(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        _bring_up_to_grant(home, tmp_path)

        dispatched = _cli("dispatch-internal-beta", home=home)
        assert dispatched.returncode == 0, dispatched.stderr
        report = json.loads(dispatched.stdout)
        assert report["ok"] is True
        assert report["ledger_path"] == str(ledger_path(home))
        assert report["event_type"] == EVENT_TYPE
        assert report["dispatcher"] == HOST_PRINCIPAL
        assert report["target"] == TARGET
        assert report["target"] != LEDGER_TARGET
        assert report["operation"] == OPERATION
        assert report["operation"] != "record"
        assert report["bundle_digest"] == BUNDLE_DIGEST
        assert report["auto_publish"] is False
        assert report["public_adapter"] is False
        assert report["discord"] is False
        assert report["x_as_publisher"] is False
        dumped = json.dumps(report).lower()
        assert "eugnel" not in dumped
        assert "newsroom-hub.vercel.app" not in dumped
        assert "vercel.app" not in dumped
        assert "x.com" not in dumped
        assert "discord.com" not in dumped
        assert "auto-010" not in dumped
        assert "host.authority.ledger" not in dumped

        db = ledger_path(home)
        assert _count(db, GRANT_EVENT_TYPE) == 1
        assert _count(db, DECISION_EVENT_TYPE) == 1
        assert _count(db, SIGNAL_EVENT_TYPE) == 1
        assert _count(db, AUTO_PUBLISH_EVENT) == 0

        operations = _read_events(db, EVENT_TYPE)
        assert len(operations) == 1
        event_type, principal_id, payload = operations[0]
        assert event_type == EVENT_TYPE
        assert principal_id == HOST_PRINCIPAL
        assert principal_id != CONTROLLER_ID
        assert principal_id != OWNER_PRINCIPAL
        assert "hermes" not in principal_id.lower()
        assert "public" not in principal_id.lower()
        assert payload["dispatcher"] == HOST_PRINCIPAL
        assert payload["bundle_digest"] == BUNDLE_DIGEST
        assert payload["target"] == TARGET
        assert payload["target"] != LEDGER_TARGET
        assert payload["operation"] == OPERATION
        assert payload["operation"] != "record"
        assert payload["auto_publish"] is False
        assert payload["discord"] is False
        assert payload["public_adapter"] is False
        assert payload["x_as_publisher"] is False
        values = " ".join(str(value).lower() for value in payload.values())
        assert "eugnel" not in values
        assert "discord.com" not in values
        assert "x.com" not in values
        assert "vercel.app" not in values
        assert "host.authority.ledger" not in values
        assert "auto-010" not in values

        again = _cli("dispatch-internal-beta", home=home)
        assert again.returncode == 0, again.stderr
        assert len(_read_events(db, EVENT_TYPE)) == 1
        assert len(_publish_operations(db)) == 1
        assert _count(db, GRANT_EVENT_TYPE) == 1
        assert _count(db, DECISION_EVENT_TYPE) == 1
    finally:
        _stop(home)


def test_dispatch_internal_beta_refuses_without_owner_signed_grant(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        _bring_up_to_decision(home, tmp_path)
        db = ledger_path(home)
        assert _read_events(db, GRANT_EVENT_TYPE) == []
        assert _read_events(db, DECISION_EVENT_TYPE)

        refused = _cli("dispatch-internal-beta", home=home)
        assert refused.returncode != 0
        report = json.loads(refused.stdout)
        assert report["ok"] is False
        assert "grant" in report["error"].lower()
        assert _publish_operations(db) == []
        assert _read_events(db, EVENT_TYPE) == []
        assert _read_events(db, GRANT_EVENT_TYPE) == []
    finally:
        _stop(home)


def test_dispatch_internal_beta_refuses_while_emergency_stop_is_paused(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    pause = home / "logs" / PAUSE_RESTORE_NAME
    _install_uv_stub(tmp_path)
    try:
        _bring_up_to_grant(home, tmp_path)
        db = ledger_path(home)
        assert _publish_operations(db) == []
        assert _read_events(db, EVENT_TYPE) == []

        stopped = _cli("stop", "--pause-restore", home=home)
        assert stopped.returncode == 0, stopped.stderr
        assert pause.is_file()

        refused = _cli("dispatch-internal-beta", home=home)
        assert refused.returncode != 0
        report = json.loads(refused.stdout)
        assert report["ok"] is False
        error = report["error"].lower()
        assert "pause" in error or "emergency stop" in error
        assert "resume" in error
        assert _publish_operations(db) == []
        assert _read_events(db, EVENT_TYPE) == []
        assert _count(db, EVENT_TYPE) == 0
        assert pause.is_file()

        auto = _cli("start", home=home)
        assert auto.returncode != 0
        health = json.loads(_cli("health", home=home).stdout)
        assert health["process_up"] is False
        assert health["pid"] is None
        assert pause.is_file()

        still = _cli("dispatch-internal-beta", home=home)
        assert still.returncode != 0
        still_report = json.loads(still.stdout)
        assert still_report["ok"] is False
        assert _publish_operations(db) == []
        assert _read_events(db, EVENT_TYPE) == []

        resumed = _cli("start", "--resume", home=home)
        assert resumed.returncode == 0, resumed.stderr
        assert not pause.is_file()

        dispatched = _cli("dispatch-internal-beta", home=home)
        assert dispatched.returncode == 0, dispatched.stderr
        assert len(_publish_operations(db)) == 1
        assert _count(db, EVENT_TYPE) == 1
        event_type, principal_id, payload = _publish_operations(db)[0]
        assert event_type == EVENT_TYPE
        assert principal_id == HOST_PRINCIPAL
        assert payload["target"] == TARGET
        assert payload["operation"] == OPERATION
        assert payload["bundle_digest"] == BUNDLE_DIGEST
    finally:
        _stop(home, pause_restore=True)


def test_emergency_stop_still_holds_after_dispatch_internal_beta(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    pause = home / "logs" / PAUSE_RESTORE_NAME
    _install_uv_stub(tmp_path)
    try:
        _bring_up_to_grant(home, tmp_path)
        dispatched = _cli("dispatch-internal-beta", home=home)
        assert dispatched.returncode == 0, dispatched.stderr
        db = ledger_path(home)
        assert len(_publish_operations(db)) == 1

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
        assert len(_publish_operations(db)) == 1
        assert _count(db, DECISION_EVENT_TYPE) == 1
        assert _count(db, GRANT_EVENT_TYPE) == 1
    finally:
        _stop(home, pause_restore=True)


def test_dispatch_internal_beta_does_not_write_public_surfaces(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        _bring_up_to_grant(home, tmp_path)
        dispatched = _cli("dispatch-internal-beta", home=home)
        assert dispatched.returncode == 0, dispatched.stderr
        report = json.loads(dispatched.stdout)
        dumped = json.dumps(report).lower()
        assert "eugnel" not in dumped
        assert "discord.com" not in dumped
        assert "x.com" not in dumped
        assert "newsroom-hub.vercel.app" not in dumped
        assert "vercel.app" not in dumped
        assert report["auto_publish"] is False
        assert report["public_adapter"] is False
        assert report["discord"] is False
        assert report["x_as_publisher"] is False
        assert report["target"] == TARGET
        assert report["operation"] == OPERATION

        db = ledger_path(home)
        payload = _publish_operations(db)[0][2]
        assert payload["auto_publish"] is False
        assert payload["public_adapter"] is False
        assert payload["discord"] is False
        assert payload["x_as_publisher"] is False
        assert payload["target"] == TARGET
        assert payload["target"] != LEDGER_TARGET
        values = " ".join(str(value).lower() for value in payload.values())
        assert "eugnel" not in values
        assert "discord.com" not in values
        assert "x.com" not in values
        assert "vercel.app" not in values
    finally:
        _stop(home)
