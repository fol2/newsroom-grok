"""External first-ingest seam: one Discovery Signal from X or official RSS."""

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

OWNER_PRINCIPAL = "owner.newsroom"
EVENT_TYPE = "discovery.signal.admitted"
ALLOWED_ADAPTERS = {
    "gated_x_search",
    "official_source_definition_rss",
}
RETIRED_MARKERS = ("brave", "gdelt", "api.search.brave.com", "gdeltproject.org")
PAUSE_RESTORE_NAME = "restore.paused"
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


def _read_signals(db: Path) -> list[tuple[str, str, dict[str, object]]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            "SELECT e.event_type, e.principal_id, p.payload_bytes "
            "FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type=?",
            (EVENT_TYPE,),
        ).fetchall()
    signals: list[tuple[str, str, dict[str, object]]] = []
    for event_type, principal_id, payload_bytes in rows:
        payload = json.loads(bytes(payload_bytes))
        assert isinstance(payload, dict)
        signals.append((str(event_type), str(principal_id), payload))
    return signals


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
        assert_allowed_url("https://api.search.brave.com/res/v1/news/search")
    with pytest.raises(Exception, match="allowlist"):
        assert_allowed_url("https://api.gdeltproject.org/api/v2/doc/doc")
    assert "discord.com" not in ALLOWED_HOSTS


def test_discovery_signal_is_absent_after_envelope_grant_only(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        db = ledger_path(home)
        assert _read_signals(db) == []
        with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
            assert conn.execute("SELECT COUNT(*) FROM discovery_signals").fetchone()[0] == 0
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM ledger_events WHERE event_type=?",
                    (EVENT_TYPE,),
                ).fetchone()[0]
                == 0
            )
    finally:
        _stop(home)


def test_ingest_signal_records_exactly_one_authorised_discovery_signal(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr

        ingested = _cli("ingest-signal", home=home, env=_rss_env(tmp_path))
        assert ingested.returncode == 0, ingested.stderr
        report = json.loads(ingested.stdout)
        assert report["ok"] is True
        assert report["ledger_path"] == str(ledger_path(home))

        signals = _read_signals(ledger_path(home))
        assert len(signals) == 1
        event_type, principal_id, payload = signals[0]
        assert event_type == EVENT_TYPE
        assert principal_id == OWNER_PRINCIPAL
        assert payload["adapter"] in ALLOWED_ADAPTERS
        assert payload["auto_publish"] is False
        assert payload["discord"] is False
        assert payload["public_adapter"] is False
        raw = json.dumps(payload).lower()
        assert all(marker not in raw for marker in RETIRED_MARKERS)
        assert_allowed_url(str(payload["url"]))
        assert str(payload["url"]).startswith("https://")

        with sqlite3.connect(f"file:{ledger_path(home)}?mode=ro", uri=True) as conn:
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM ledger_events WHERE event_type=?",
                    (EVENT_TYPE,),
                ).fetchone()[0]
                == 1
            )
            news_leads = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='news_leads'"
            ).fetchone()
            if news_leads is not None:
                assert conn.execute("SELECT COUNT(*) FROM news_leads").fetchone()[0] == 0

        again = _cli("ingest-signal", home=home, env=_rss_env(tmp_path))
        assert again.returncode == 0, again.stderr
        assert len(_read_signals(ledger_path(home))) == 1
    finally:
        _stop(home)


def test_emergency_stop_still_holds_after_ingest(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    pause = home / "logs" / PAUSE_RESTORE_NAME
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        ingested = _cli("ingest-signal", home=home, env=_rss_env(tmp_path))
        assert ingested.returncode == 0, ingested.stderr
        assert len(_read_signals(ledger_path(home))) == 1

        stopped = _cli("stop", "--pause-restore", home=home)
        assert stopped.returncode == 0, stopped.stderr
        health = json.loads(_cli("health", home=home).stdout)
        assert health["process_up"] is False
        assert health["pid"] is None
        assert pause.is_file()

        time.sleep(1.2)
        still = json.loads(_cli("health", home=home).stdout)
        assert still["process_up"] is False
        assert pause.is_file()

        auto = _cli("start", home=home)
        assert auto.returncode != 0
        refused = json.loads(_cli("health", home=home).stdout)
        assert refused["process_up"] is False
        assert pause.is_file()
        assert len(_read_signals(ledger_path(home))) == 1
    finally:
        _stop(home, pause_restore=True)


REMAINING_RSS_SOURCE_IDS = ("HK-04", "RAD-01", "RAD-02", "UK-01", "UK-05", "UK-10")
SKIP_EVENT_TYPE = "discovery.signal.skipped"
PARKED_JSON_SOURCE_IDS = ("HK-02", "UK-02", "UK-03")


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


def test_ingest_signal_accepts_source_id_and_records_that_source(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr

        ingested = _cli(
            "ingest-signal",
            "--source-id",
            "HK-04",
            home=home,
            env=_rss_env(tmp_path),
        )
        assert ingested.returncode == 0, ingested.stderr
        report = json.loads(ingested.stdout)
        assert report["ok"] is True
        assert report.get("skipped") is not True
        assert report["source_id"] == "HK-04"
        assert report["url"] == SOURCE_URLS["HK-04"]
        assert report["event_type"] == EVENT_TYPE
        assert_allowed_url(str(report["url"]))

        signals = _read_signals(ledger_path(home))
        assert len(signals) == 1
        _event_type, principal_id, payload = signals[0]
        assert _event_type == EVENT_TYPE
        assert principal_id == OWNER_PRINCIPAL
        assert payload["source_id"] == "HK-04"
        assert payload["url"] == SOURCE_URLS["HK-04"]
        assert payload["adapter"] == "official_source_definition_rss"
        assert payload["auto_publish"] is False
        assert payload["discord"] is False
        assert payload["public_adapter"] is False
        assert payload["item_id"]
        raw = json.dumps(payload).lower()
        assert all(marker not in raw for marker in RETIRED_MARKERS)

        again = _cli(
            "ingest-signal",
            "--source-id",
            "HK-04",
            home=home,
            env=_rss_env(tmp_path),
        )
        assert again.returncode == 0, again.stderr
        assert len(_read_signals(ledger_path(home))) == 1
    finally:
        _stop(home)


def test_ingest_signal_records_one_admitted_row_per_remaining_rss_source(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr

        first = _cli("ingest-signal", home=home, env=_rss_env(tmp_path))
        assert first.returncode == 0, first.stderr
        assert json.loads(first.stdout)["source_id"] == "HK-01"

        for source_id in REMAINING_RSS_SOURCE_IDS:
            ingested = _cli(
                "ingest-signal",
                "--source-id",
                source_id,
                home=home,
                env=_rss_env(tmp_path),
            )
            assert ingested.returncode == 0, ingested.stderr
            report = json.loads(ingested.stdout)
            assert report["ok"] is True
            assert report.get("skipped") is not True
            assert report["source_id"] == source_id
            assert report["url"] == SOURCE_URLS[source_id]
            assert report["event_type"] == EVENT_TYPE

        signals = _read_signals(ledger_path(home))
        source_ids = {payload["source_id"] for _event, _principal, payload in signals}
        assert source_ids == {"HK-01", *REMAINING_RSS_SOURCE_IDS}
        assert len(signals) == 7
        for _event, principal_id, payload in signals:
            assert principal_id == OWNER_PRINCIPAL
            assert payload["adapter"] == "official_source_definition_rss"
            assert payload["auto_publish"] is False
            assert payload["discord"] is False
            assert payload["public_adapter"] is False
            assert payload["url"] == SOURCE_URLS[str(payload["source_id"])]
            assert_allowed_url(str(payload["url"]))
    finally:
        _stop(home)


def test_ingest_signal_records_skip_when_feed_fetch_fails(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr

        skipped = _cli(
            "ingest-signal",
            "--source-id",
            "HK-04",
            home=home,
            env={"NEWSROOM_INGEST_HTTP_STATUS": "503"},
        )
        assert skipped.returncode == 0, skipped.stderr
        report = json.loads(skipped.stdout)
        assert report["ok"] is True
        assert report["skipped"] is True
        assert report["source_id"] == "HK-04"
        assert report["url"] == SOURCE_URLS["HK-04"]
        assert report["event_type"] == SKIP_EVENT_TYPE
        assert "503" in str(report["reason"])
        assert report["auto_publish"] is False
        assert report["discord"] is False
        assert report["public_adapter"] is False

        assert _read_signals(ledger_path(home)) == []
        skips = _read_events(ledger_path(home), SKIP_EVENT_TYPE)
        assert len(skips) == 1
        _event_type, principal_id, payload = skips[0]
        assert _event_type == SKIP_EVENT_TYPE
        assert principal_id == OWNER_PRINCIPAL
        assert payload["source_id"] == "HK-04"
        assert payload["url"] == SOURCE_URLS["HK-04"]
        assert payload["adapter"] == "official_source_definition_rss"
        assert payload["auto_publish"] is False
        assert payload["discord"] is False
        assert payload["public_adapter"] is False
        assert "503" in str(payload["reason"])
        assert "item_id" not in payload
        raw = json.dumps(payload).lower()
        assert all(marker not in raw for marker in RETIRED_MARKERS)

        again = _cli(
            "ingest-signal",
            "--source-id",
            "HK-04",
            home=home,
            env={"NEWSROOM_INGEST_HTTP_STATUS": "503"},
        )
        assert again.returncode == 0, again.stderr
        assert len(_read_events(ledger_path(home), SKIP_EVENT_TYPE)) == 1
        assert _read_signals(ledger_path(home)) == []
    finally:
        _stop(home)


def test_ingest_signal_refuses_parked_json_and_unknown_source(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr

        for source_id in (*PARKED_JSON_SOURCE_IDS, "EXAMPLE-01"):
            refused = _cli(
                "ingest-signal",
                "--source-id",
                source_id,
                home=home,
                env=_rss_env(tmp_path),
            )
            assert refused.returncode != 0
            report = json.loads(refused.stdout)
            assert report["ok"] is False
            assert "invent" not in json.dumps(report).lower()

        assert _read_signals(ledger_path(home)) == []
        assert _read_events(ledger_path(home), SKIP_EVENT_TYPE) == []
    finally:
        _stop(home)
