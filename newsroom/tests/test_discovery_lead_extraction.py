from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from newsroom.discovery_lead_extraction import (
    EXTRACT_SOURCE_IDS,
    record_first_boot_extraction,
)
from newsroom.first_boot import ledger_path
from newsroom.graphiti_adapter.models import REAL_GRAPHITI_RUNTIME_ENABLED
from newsroom.tests.test_discovery_lead_admission import (
    FEEDS,
    NOW,
    _admit,
    _cli,
    _db,
    _install_uv_stub,
    _seed_live_admissions,
    _table_count,
)


def test_extract_binds_revision_and_representation_not_item_id(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed_live_admissions(db)
    _admit(db)
    result = record_first_boot_extraction(db, clock=lambda: NOW)
    assert result["ok"] is True
    assert result["extraction_runs"] == 6
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT er.lead_bind_revision, er.representation_id, nl.revision_id,
                   nl.representation_id, nl.item_id, er.item_id
            FROM (
                SELECT revision_id AS lead_bind_revision, representation_id,
                       item_id, run_id
                FROM extraction_runs
            ) AS er
            JOIN news_leads AS nl
              ON nl.revision_id = er.lead_bind_revision
             AND nl.representation_id = er.representation_id
            """
        ).fetchall()
        assert len(rows) == 6
        for revision_id, representation_id, lead_rev, lead_rep, lead_item, run_item in rows:
            assert revision_id == lead_rev
            assert representation_id == lead_rep
            assert run_item == lead_item
            assert revision_id != lead_item
            assert representation_id != lead_item
        passages = conn.execute("SELECT COUNT(*) FROM extraction_run_passages").fetchone()
        assert passages is not None
        assert int(passages[0]) == 6
    finally:
        conn.close()


def test_extract_six_allowlisted_leads_skips_rad02(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed_live_admissions(db)
    _admit(db)
    result = record_first_boot_extraction(db, clock=lambda: NOW)
    assert REAL_GRAPHITI_RUNTIME_ENABLED is False
    assert result["ok"] is True
    assert result["graphiti"] is False
    assert result["graphiti_runtime_enabled"] is False
    assert result["auto_publish"] is False
    assert result["discord"] is False
    assert result["public_adapter"] is False
    assert result["x_as_publisher"] is False
    assert result["extraction_runs"] == 6
    assert {item["source_id"] for item in result["extracted"]} == set(EXTRACT_SOURCE_IDS)
    assert {item["source_id"] for item in result["skipped"]} == {"RAD-02"}
    assert all(item["reason"] == "source-not-in-4a-allowlist" for item in result["skipped"])
    assert all(item["outcome"] == "SUCCESS" for item in result["extracted"])
    assert all(item["replayed"] is False for item in result["extracted"])
    assert _table_count(db, "extraction_runs") == 6
    assert _table_count(db, "extraction_proposals") == 18
    assert _table_count(db, "canonical_entities") == 0
    assert _table_count(db, "editorial_relation_decisions") == 0
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        graphiti_rows = conn.execute(
            "SELECT COUNT(*) FROM graphiti_adapter_configurations"
        ).fetchone()
        assert graphiti_rows is not None
        assert int(graphiti_rows[0]) == 0
        for item in result["extracted"]:
            row = conn.execute(
                """
                SELECT er.revision_id, er.representation_id, nl.revision_id,
                       nl.representation_id, nl.item_id
                FROM extraction_runs AS er
                JOIN news_leads AS nl
                  ON nl.revision_id = er.revision_id
                 AND nl.representation_id = er.representation_id
                WHERE er.run_id = ?
                """,
                (item["run_id"],),
            ).fetchone()
            assert row is not None
            assert row[0] == row[2] == item["revision_id"]
            assert row[1] == row[3] == item["representation_id"]
            assert row[4] == item["item_id"]
            assert item["revision_id"] != item["item_id"]
            assert item["representation_id"] != item["item_id"]
    finally:
        conn.close()


def test_extract_is_idempotent(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed_live_admissions(db)
    _admit(db)
    first = record_first_boot_extraction(db, clock=lambda: NOW)
    second = record_first_boot_extraction(db, clock=lambda: NOW)
    assert first["ok"] is True
    assert second["ok"] is True
    assert len(first["extracted"]) == 6
    assert second["extracted"] == []
    assert {item["reason"] for item in second["skipped"]} == {
        "already-extracted",
        "source-not-in-4a-allowlist",
    }
    assert second["extraction_runs"] == 6
    assert _table_count(db, "extraction_runs") == 6


def test_cli_extract_leads_after_hk01_admit(tmp_path: Path) -> None:
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
        extracted = _cli("extract-leads", home=home)
        assert extracted.returncode == 0, extracted.stderr + extracted.stdout
        payload = json.loads(extracted.stdout)
        assert payload["ok"] is True
        assert payload["extracted"][0]["source_id"] == "HK-01"
        assert payload["extraction_runs"] == 1
        assert payload["graphiti"] is False
        assert payload["auto_publish"] is False
        assert payload["discord"] is False
        assert payload["public_adapter"] is False
        db = ledger_path(home)
        assert _table_count(db, "news_leads") == 1
        assert _table_count(db, "extraction_runs") == 1
        assert _table_count(db, "extraction_run_passages") == 1
    finally:
        _cli("stop", home=home)
