"""External X-search ingest seam: one Discovery Signal from gated X search."""

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

from newsroom.discovery_ingest import RSS_SOURCE_IDS
from newsroom.first_boot import SHARED_HOME, ledger_path
from newsroom.increment9.proving import ALLOWED_HOSTS, assert_allowed_url
from newsroom.increment9.qualification import GATE_ID as NONMUTATION_GATE
from newsroom.increment9.shadow_contracts import ProhibitedEffect, _NoEffect
from newsroom.increment10.requalification import EXPECTED_RESIDUAL_GATES
from newsroom.x_search_ingest import (
    GATED_X_SEARCH_ADAPTER,
    PAYLOAD_KEYS,
    SAMPLE_PAYLOAD,
    X_SEARCH_SOURCE_IDS,
    canonicalize_signal_payload,
    load_x_search_hit,
)


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "newsroom_first_boot.py"

OWNER_PRINCIPAL = "owner.newsroom"
EVENT_TYPE = "discovery.signal.admitted"
ADAPTER = GATED_X_SEARCH_ADAPTER
RETIRED_MARKERS = ("brave", "gdelt", "api.search.brave.com", "gdeltproject.org")
PAUSE_RESTORE_NAME = "restore.paused"
SAMPLE_URL = SAMPLE_PAYLOAD["url"]
SAMPLE_ITEM = SAMPLE_PAYLOAD["item_id"]
SAMPLE_SOURCE = SAMPLE_PAYLOAD["source_id"]


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


def _x_search_env(tmp_path: Path, *, payload: dict[str, object] | None = None) -> dict[str, str]:
    hit = tmp_path / "x-search-hit.json"
    body = payload or {
        "source_id": SAMPLE_SOURCE,
        "item_id": SAMPLE_ITEM,
        "url": SAMPLE_URL,
    }
    hit.write_text(json.dumps(body), encoding="utf-8")
    return {"NEWSROOM_X_SEARCH_BODY_PATH": str(hit)}


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


def test_payload_keys_match_rss_discovery_contract() -> None:
    from newsroom.discovery_ingest import PAYLOAD_KEYS as RSS_KEYS

    assert PAYLOAD_KEYS == RSS_KEYS
    assert set(SAMPLE_PAYLOAD) == set(PAYLOAD_KEYS)
    assert SAMPLE_PAYLOAD["adapter"] == ADAPTER
    assert X_SEARCH_SOURCE_IDS.isdisjoint(RSS_SOURCE_IDS)
    canonicalize_signal_payload(SAMPLE_PAYLOAD)


def test_contract_rejects_rss_ids_outlets_and_write_flags() -> None:
    with pytest.raises(ValueError, match="X search posts or news"):
        canonicalize_signal_payload({**SAMPLE_PAYLOAD, "source_id": "HK-01"})
    with pytest.raises(ValueError, match="X search posts or news"):
        canonicalize_signal_payload({**SAMPLE_PAYLOAD, "source_id": "BBC"})
    with pytest.raises(ValueError, match="gated X search"):
        canonicalize_signal_payload(
            {**SAMPLE_PAYLOAD, "adapter": "official_source_definition_rss"}
        )
    with pytest.raises(ValueError, match="AUTO_PUBLISH"):
        canonicalize_signal_payload({**SAMPLE_PAYLOAD, "auto_publish": True})
    with pytest.raises(ValueError, match="Discord"):
        canonicalize_signal_payload({**SAMPLE_PAYLOAD, "discord": True})
    with pytest.raises(ValueError, match="public adapters"):
        canonicalize_signal_payload({**SAMPLE_PAYLOAD, "public_adapter": True})
    with pytest.raises(ValueError, match="x.com"):
        canonicalize_signal_payload(
            {**SAMPLE_PAYLOAD, "url": "https://www.news.gov.hk/tc/common/html/topstories.rss.xml"}
        )
    with pytest.raises(ValueError, match="Brave"):
        canonicalize_signal_payload(
            {**SAMPLE_PAYLOAD, "url": "https://x.com/i/news/brave-search"}
        )


def test_load_requires_supplied_hit_and_does_not_invent_a_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("NEWSROOM_X_SEARCH_BODY_PATH", raising=False)
    with pytest.raises(ValueError, match="NEWSROOM_X_SEARCH_BODY_PATH"):
        load_x_search_hit()
    env = _x_search_env(tmp_path)
    monkeypatch.setenv("NEWSROOM_X_SEARCH_BODY_PATH", env["NEWSROOM_X_SEARCH_BODY_PATH"])
    source_id, url, item_id = load_x_search_hit()
    assert source_id == SAMPLE_SOURCE
    assert url == SAMPLE_URL
    assert item_id == SAMPLE_ITEM


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
    with pytest.raises(Exception, match="allowlist"):
        assert_allowed_url("https://discord.com/api")
    with pytest.raises(Exception, match="allowlist"):
        assert_allowed_url("https://api.search.brave.com/res/v1/news/search")
    with pytest.raises(Exception, match="allowlist"):
        assert_allowed_url("https://x.com/i/news/x-search-news-first-item")
    assert "discord.com" not in ALLOWED_HOSTS
    assert "x.com" not in ALLOWED_HOSTS


def test_x_search_signal_is_absent_after_envelope_grant_only(tmp_path: Path) -> None:
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


def test_ingest_x_search_records_exactly_one_authorised_discovery_signal(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr

        ingested = _cli("ingest-x-search", home=home, env=_x_search_env(tmp_path))
        assert ingested.returncode == 0, ingested.stderr
        report = json.loads(ingested.stdout)
        assert report["ok"] is True
        assert report["adapter"] == ADAPTER
        assert report["ledger_path"] == str(ledger_path(home))
        assert report["source_id"] == SAMPLE_SOURCE
        assert report["url"] == SAMPLE_URL

        signals = _read_signals(ledger_path(home))
        assert len(signals) == 1
        event_type, principal_id, payload = signals[0]
        assert event_type == EVENT_TYPE
        assert principal_id == OWNER_PRINCIPAL
        assert payload["adapter"] == ADAPTER
        assert payload["source_id"] in X_SEARCH_SOURCE_IDS
        assert payload["source_id"] not in RSS_SOURCE_IDS
        assert payload["auto_publish"] is False
        assert payload["discord"] is False
        assert payload["public_adapter"] is False
        assert payload["url"] == SAMPLE_URL
        raw = json.dumps(payload).lower()
        assert all(marker not in raw for marker in RETIRED_MARKERS)
        assert str(payload["url"]).startswith("https://x.com/")

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

        again = _cli("ingest-x-search", home=home, env=_x_search_env(tmp_path))
        assert again.returncode == 0, again.stderr
        assert len(_read_signals(ledger_path(home))) == 1
    finally:
        _stop(home)


def test_emergency_stop_vetoes_x_search_ingest(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    pause = home / "logs" / PAUSE_RESTORE_NAME
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        stopped = _cli("stop", "--pause-restore", home=home)
        assert stopped.returncode == 0, stopped.stderr
        assert pause.is_file()

        refused = _cli("ingest-x-search", home=home, env=_x_search_env(tmp_path))
        assert refused.returncode != 0
        assert _read_signals(ledger_path(home)) == []
        health = json.loads(_cli("health", home=home).stdout)
        assert health["process_up"] is False
        assert pause.is_file()
    finally:
        _stop(home, pause_restore=True)


def test_emergency_stop_still_holds_after_x_search_ingest(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    pause = home / "logs" / PAUSE_RESTORE_NAME
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        ingested = _cli("ingest-x-search", home=home, env=_x_search_env(tmp_path))
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
