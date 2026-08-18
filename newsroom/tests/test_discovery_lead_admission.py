"""Live-shaped first-boot Signal → Gate → News Lead admission."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from newsroom.authority.types import UtcTimestamp
from newsroom.discovery_ingest import SOURCE_URLS, record_discovery_signal, record_discovery_skip
from newsroom.discovery_lead_admission import (
    item_identity_url,
    record_first_boot_leads,
    resolve_atom_tag,
)
from newsroom.first_boot import SHARED_HOME, ledger_path
from newsroom.graphiti_adapter.models import REAL_GRAPHITI_RUNTIME_ENABLED
from newsroom.host_store import open_host_discovery_system, open_host_store
from newsroom.publication_bundle import (
    EVENT_TYPE as BUNDLE_EVENT,
    ITEM_URL as HK01_ITEM,
    SAMPLE_PAYLOAD as BUNDLE_STORY,
    record_publication_bundle,
)
from newsroom.publication_decision import (
    load_first_discovery_signal,
    record_publication_decision,
)
from .discovery_3d_helpers import SIGNAL_ID as FIXTURE_3D_SIGNAL
from newsroom.x_search_ingest import record_discovery_signal as record_x_search_signal


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "newsroom_first_boot.py"
NOW = UtcTimestamp.parse("2026-08-18T01:00:00.000000Z")
HK04_ITEM = (
    "https://www.edb.gov.hk/tc/student-parents/parents-related/"
    "ebulletin-for-parents/2025-2026/20260814.html"
)
RAD01_ITEM = "https://news.rthk.hk/rthk/ch/component/k2/1866503-20260817.htm"
RAD02_ITEM = "https://www.bbc.co.uk/news/articles/c4g3re5ew8do#0"
UK01_ITEM = "tag:www.gov.uk,2005:/government/publications/form-bota-guidance"
UK05_ITEM = "tag:www.gov.uk,2005:/guidance/special-educational-needs-survey"
X_ITEM = "https://x.com/newsgovhk/status/2089334709910491610"
ADMITTED = (
    ("HK-01", HK01_ITEM, SOURCE_URLS["HK-01"]),
    ("HK-04", HK04_ITEM, SOURCE_URLS["HK-04"]),
    ("RAD-01", RAD01_ITEM, SOURCE_URLS["RAD-01"]),
    ("RAD-02", RAD02_ITEM, SOURCE_URLS["RAD-02"]),
    ("UK-01", UK01_ITEM, SOURCE_URLS["UK-01"]),
    ("UK-05", UK05_ITEM, SOURCE_URLS["UK-05"]),
)
PROMOTED_SOURCE_IDS = tuple(item[0] for item in ADMITTED) + ("X-SEARCH-POSTS",)


def _rss(item_id: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<rss version=\"2.0\"><channel>"
        f"<item><guid>{item_id}</guid><link>{item_id}</link>"
        "<title>Recorded first-boot item</title></item>"
        "</channel></rss>"
    ).encode("utf-8")


def _atom(item_id: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        f"<entry><id>{item_id}</id><title>Recorded first-boot item</title></entry>"
        "</feed>"
    ).encode("utf-8")


FEEDS = {
    "HK-01": _rss(HK01_ITEM),
    "HK-04": _rss(HK04_ITEM),
    "RAD-01": _rss(RAD01_ITEM),
    "RAD-02": _rss(RAD02_ITEM),
    "UK-01": _atom(UK01_ITEM),
    "UK-05": _atom(UK05_ITEM),
}


def _feed_loader(source_id: str) -> tuple[str, str, bytes]:
    return source_id, SOURCE_URLS[source_id], FEEDS[source_id]


def _empty_feed_loader(source_id: str) -> tuple[str, str, bytes]:
    empty = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b"<rss version=\"2.0\"><channel></channel></rss>"
    )
    return source_id, SOURCE_URLS[source_id], empty


def _db(tmp_path: Path) -> Path:
    tmp_path.chmod(0o700)
    db = tmp_path / "authority.sqlite3"
    open_host_store(db).close()
    return db


def _seed_live_admissions(db: Path) -> None:
    for source_id, item_id, url in ADMITTED:
        record_discovery_signal(db, source_id=source_id, url=url, item_id=item_id)
    record_x_search_signal(
        db,
        source_id="X-SEARCH-POSTS",
        url=X_ITEM,
        item_id=X_ITEM,
    )
    record_discovery_skip(
        db,
        source_id="UK-10",
        url=SOURCE_URLS["UK-10"],
        reason="empty-feed",
    )


def _admit(db: Path, *, feed_loader=_feed_loader) -> dict[str, object]:
    return record_first_boot_leads(db, feed_loader=feed_loader, clock=lambda: NOW)


def _table_count(db: Path, name: str) -> int:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()
        if present is None:
            return 0
        return int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])


def _native_ids(db: Path) -> set[str]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            "SELECT source_native_id FROM source_items WHERE source_native_id IS NOT NULL"
        ).fetchall()
    return {str(row[0]) for row in rows}


def _signal_ids(db: Path) -> set[str]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        rows = conn.execute("SELECT signal_id FROM discovery_signals").fetchall()
    return {str(row[0]) for row in rows}


def _phases(db: Path) -> list[tuple[str, int, str]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        return [
            (str(outcome), int(ordinal), str(disp))
            for outcome, ordinal, disp in conn.execute(
                "SELECT g.outcome, d.decision_ordinal, d.outcome "
                "FROM news_leads l "
                "JOIN discovery_gate_decisions g "
                "ON g.decision_id=l.promoting_gate_decision_id "
                "JOIN lead_disposition_decisions d ON d.lead_id=l.lead_id "
                "ORDER BY l.lead_id, d.decision_ordinal"
            )
        ]


def _bundle_rows(db: Path) -> list[tuple[int, str, bytes]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        return [
            (int(seq), str(kind), bytes(payload))
            for seq, kind, payload in conn.execute(
                "SELECT e.ledger_seq, e.event_type, p.payload_bytes "
                "FROM ledger_events e "
                "JOIN authority_payloads p ON p.payload_id=e.payload_id "
                "WHERE e.event_type=? ORDER BY e.ledger_seq",
                (BUNDLE_EVENT,),
            )
        ]


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


def test_shared_ledger_path_stays_on_the_box_home() -> None:
    assert SHARED_HOME == Path("/home/box/newsroom")
    assert ledger_path(SHARED_HOME) == Path("/home/box/newsroom/data/authority.sqlite3")


def test_atom_tag_resolves_gov_uk_and_fails_closed() -> None:
    assert resolve_atom_tag(UK01_ITEM) == (
        "https://www.gov.uk/government/publications/form-bota-guidance"
    )
    assert item_identity_url(UK05_ITEM) == (
        "https://www.gov.uk/guidance/special-educational-needs-survey"
    )
    assert item_identity_url(HK01_ITEM) == HK01_ITEM
    with pytest.raises(Exception, match="Atom tag cannot be resolved"):
        resolve_atom_tag("tag:broken")


def test_seven_live_admissions_promote_to_queued_leads(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed_live_admissions(db)
    result = _admit(db)
    assert REAL_GRAPHITI_RUNTIME_ENABLED is False
    assert result["ok"] is True
    assert result["graphiti_runtime_enabled"] is False
    assert result["news_leads"] == 7
    assert result["extraction_runs"] == 0
    assert result["editorial_relation_decisions"] == 0
    promoted = {item["source_id"]: item for item in result["promoted"]}
    assert tuple(promoted) == PROMOTED_SOURCE_IDS
    for item in result["promoted"]:
        assert item["phase"] == "LEAD_QUEUED"
        assert item["gate_outcome"] == "SIGNAL_PROMOTED_TO_LEAD"
        assert item["disposition"] == "LEAD_QUEUED_FOR_TRIAGE"
        assert item["replayed"] is False
        assert item["signal_id"] != str(FIXTURE_3D_SIGNAL)
    assert {item["source_id"] for item in result["skipped"]} == {"UK-10"}
    assert result["held"] == []
    assert _native_ids(db) == {
        HK01_ITEM,
        HK04_ITEM,
        RAD01_ITEM,
        RAD02_ITEM,
        UK01_ITEM,
        UK05_ITEM,
        X_ITEM,
    }
    assert str(FIXTURE_3D_SIGNAL) not in _signal_ids(db)
    assert _phases(db) == [
        ("SIGNAL_PROMOTED_TO_LEAD", 1, "LEAD_QUEUED_FOR_TRIAGE")
    ] * 7
    assert _table_count(db, "extraction_runs") == 0
    assert _table_count(db, "extraction_proposals") == 0
    assert _table_count(db, "editorial_relation_decisions") == 0
    assert _table_count(db, "editorial_relation_proposals") == 0


def test_uk10_skip_does_not_create_a_lead(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed_live_admissions(db)
    result = _admit(db)
    native = _native_ids(db)
    assert "UK-10" not in native
    assert all(item["source_id"] != "UK-10" for item in result["promoted"])
    assert any(item["source_id"] == "UK-10" for item in result["skipped"])


def test_second_run_is_idempotent_and_does_not_add_leads(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed_live_admissions(db)
    first = _admit(db)
    second = _admit(db)
    assert first["news_leads"] == second["news_leads"] == 7
    assert {item["lead_id"] for item in first["promoted"]} == {
        item["lead_id"] for item in second["promoted"]
    }
    assert all(item["replayed"] is True for item in second["promoted"])
    assert all(item["phase"] == "LEAD_QUEUED" for item in second["promoted"])


def test_gone_item_holds_and_does_not_create_a_lead(tmp_path: Path) -> None:
    db = _db(tmp_path)
    record_discovery_signal(
        db,
        source_id="HK-01",
        url=SOURCE_URLS["HK-01"],
        item_id=HK01_ITEM,
    )
    result = _admit(db, feed_loader=_empty_feed_loader)
    assert result["news_leads"] == 0
    assert result["ok"] is False
    assert result["promoted"] == []
    assert result["held"] == [
        {"source_id": "HK-01", "item_id": HK01_ITEM, "reason": "item-gone"}
    ]


def test_unresolved_atom_tag_holds(tmp_path: Path) -> None:
    db = _db(tmp_path)
    record_discovery_signal(
        db,
        source_id="UK-01",
        url=SOURCE_URLS["UK-01"],
        item_id="tag:broken",
    )
    result = _admit(db)
    assert result["news_leads"] == 0
    assert result["held"] == [
        {
            "source_id": "UK-01",
            "item_id": "tag:broken",
            "reason": "atom-tag-unresolved",
        }
    ]


def test_disallowed_resolved_host_holds(tmp_path: Path) -> None:
    db = _db(tmp_path)
    record_discovery_signal(
        db,
        source_id="UK-05",
        url=SOURCE_URLS["UK-05"],
        item_id="tag:example.com,2005:/not-allowed",
    )
    result = _admit(db)
    assert result["news_leads"] == 0
    assert result["held"] == [
        {
            "source_id": "UK-05",
            "item_id": "tag:example.com,2005:/not-allowed",
            "reason": "host-not-allowed",
        }
    ]


def test_host_store_reopens_after_leads_and_seq15_stays_put(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed_live_admissions(db)
    signal = load_first_discovery_signal(db)
    record_publication_decision(db, signal_payload=signal)
    recorded = record_publication_bundle(
        db,
        story={"title": BUNDLE_STORY["story_title"], "body": BUNDLE_STORY["story_body"]},
    )
    before = _bundle_rows(db)
    assert len(before) == 1
    result = _admit(db)
    after = _bundle_rows(db)
    assert result["news_leads"] == 7
    assert after == before
    assert json.loads(after[0][2])["bundle_digest"] == recorded["bundle_digest"]
    open_host_store(db).close()
    with open_host_discovery_system(db) as system:
        assert system is not None
    assert _table_count(db, "extraction_runs") == 0
    assert _table_count(db, "editorial_relation_decisions") == 0


def test_cli_admit_leads_promotes_recorded_hk01(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    feeds = tmp_path / "feeds"
    feeds.mkdir()
    (feeds / "HK-01.xml").write_bytes(FEEDS["HK-01"])
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        ingested = _cli(
            "ingest-signal",
            home=home,
            env={"NEWSROOM_INGEST_BODY_PATH": str(feeds / "HK-01.xml")},
        )
        assert ingested.returncode == 0, ingested.stderr
        admitted = _cli(
            "admit-leads",
            home=home,
            env={"NEWSROOM_LEAD_FEED_DIR": str(feeds)},
        )
        assert admitted.returncode == 0, admitted.stderr + admitted.stdout
        payload = json.loads(admitted.stdout)
        assert payload["ok"] is True
        assert payload["news_leads"] == 1
        assert payload["promoted"][0]["source_id"] == "HK-01"
        assert payload["promoted"][0]["phase"] == "LEAD_QUEUED"
        assert payload["extraction_runs"] == 0
        assert payload["graphiti_runtime_enabled"] is False
        db = ledger_path(home)
        assert _table_count(db, "news_leads") == 1
        assert _native_ids(db) == {HK01_ITEM}
    finally:
        _cli("stop", home=home)


def test_cli_admit_leads_fails_closed_when_paused(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        stopped = _cli("stop", "--pause-restore", home=home)
        assert stopped.returncode == 0, stopped.stderr
        admitted = _cli("admit-leads", home=home)
        assert admitted.returncode != 0
        assert "paused" in (admitted.stderr + admitted.stdout).lower()
    finally:
        _cli("stop", home=home)
