from __future__ import annotations

import ast
import json
import sqlite3
from pathlib import Path

from newsroom.discovery_lead_editorial_relations import (
    record_first_boot_editorial_relations,
)
from newsroom.discovery_lead_entity_resolution import (
    record_first_boot_entity_resolution,
)
from newsroom.discovery_lead_extraction import record_first_boot_extraction
from newsroom.discovery_lead_story_candidates import (
    TRIAGE_SOURCE_IDS,
    record_first_boot_story_candidates,
)
from newsroom.first_boot import ledger_path
from newsroom.graphiti_adapter.models import REAL_GRAPHITI_RUNTIME_ENABLED
from newsroom.host_store import open_host_store
from newsroom.publication_bundle import (
    EVENT_TYPE as BUNDLE_EVENT,
    SAMPLE_PAYLOAD as BUNDLE_STORY,
    record_publication_bundle,
)
from newsroom.publication_decision import (
    load_first_discovery_signal,
    record_publication_decision,
)
from newsroom.tests.test_discovery_lead_admission import (
    FEEDS,
    NOW,
    _admit,
    _bundle_rows,
    _cli,
    _db,
    _install_uv_stub,
    _seed_live_admissions,
    _table_count,
)


def _relate(db: Path) -> dict[str, object]:
    _seed_live_admissions(db)
    _admit(db)
    record_first_boot_extraction(db, clock=lambda: NOW)
    record_first_boot_entity_resolution(db, clock=lambda: NOW)
    return record_first_boot_editorial_relations(db, clock=lambda: NOW)


def test_candidates_bind_revision_representation_and_admitted_passages(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    _relate(db)
    result = record_first_boot_story_candidates(db, clock=lambda: NOW)
    assert result["ok"] is True
    assert result["story_candidates"] == 6
    assert result["story_candidate_heads"] == 6
    assert result["story_candidate_admission_receipts_v2"] == 6
    assert result["story_candidate_versions"] == 0
    assert result["triage_work_items"] == 6
    assert result["triage_work_item_versions"] == 6
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT json_extract(CAST(h.candidate_bytes AS TEXT), '$.revision_id'),
                   json_extract(CAST(h.candidate_bytes AS TEXT), '$.representation_id'),
                   json_extract(CAST(h.candidate_bytes AS TEXT), '$.item_id'),
                   json_extract(CAST(h.candidate_bytes AS TEXT), '$.passage_id'),
                   json_extract(CAST(h.candidate_bytes AS TEXT), '$.lead_id'),
                   json_extract(CAST(h.candidate_bytes AS TEXT), '$.run_id'),
                   nl.revision_id, nl.representation_id, nl.item_id, nl.lead_id,
                   rp.passage_id, er.run_id
            FROM story_candidate_heads AS h
            JOIN news_leads AS nl
              ON nl.lead_id = json_extract(CAST(h.candidate_bytes AS TEXT), '$.lead_id')
            JOIN extraction_runs AS er
              ON er.revision_id = nl.revision_id
             AND er.representation_id = nl.representation_id
            JOIN extraction_run_passages AS rp ON rp.run_id = er.run_id
            """
        ).fetchall()
        assert len(rows) == 6
        for (
            cand_rev,
            cand_rep,
            cand_item,
            cand_passage,
            cand_lead,
            cand_run,
            lead_rev,
            lead_rep,
            lead_item,
            lead_id,
            run_passage,
            run_id,
        ) in rows:
            assert cand_rev == lead_rev
            assert cand_rep == lead_rep
            assert cand_item == lead_item
            assert cand_lead == lead_id
            assert cand_passage == run_passage
            assert cand_run == run_id
            assert cand_rev != lead_item
            assert cand_rep != lead_item
            assert cand_passage != lead_item
        json_as_passage = conn.execute(
            """
            SELECT COUNT(*)
            FROM story_candidate_heads h
            JOIN entity_mentions m
              ON m.passage_id = json_extract(
                   CAST(h.candidate_bytes AS TEXT), '$.passage_id'
                 )
            WHERE json_extract(CAST(h.candidate_bytes AS TEXT), '$.passage_id')
                  = m.mention_text
               OR json_extract(CAST(h.candidate_bytes AS TEXT), '$.passage_id')
                  = m.normalized_text
            """
        ).fetchone()
        assert json_as_passage is not None
        assert int(json_as_passage[0]) == 0
    finally:
        conn.close()


def test_candidates_bind_4b_entities_and_4c_accept_relations(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _relate(db)
    result = record_first_boot_story_candidates(db, clock=lambda: NOW)
    assert result["canonical_entities"] == 12
    assert result["editorial_relation_assertions"] == 6
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT json_extract(CAST(h.candidate_bytes AS TEXT), '$.lead_id'),
                   json_extract(CAST(h.candidate_bytes AS TEXT), '$.relation_assertion_id'),
                   json_extract(CAST(h.candidate_bytes AS TEXT), '$.canonical_entity_ids'),
                   json_extract(CAST(h.candidate_bytes AS TEXT), '$.authorises_publication'),
                   json_extract(CAST(h.candidate_bytes AS TEXT), '$.authorises_evidence')
            FROM story_candidate_heads AS h
            """
        ).fetchall()
        assert len(rows) == 6
        for _lead_id, assertion_id, entity_ids, publication, evidence in rows:
            assert publication in {0, False}
            assert evidence in {0, False}
            entities = json.loads(entity_ids)
            assert len(entities) == 2
            accepted = conn.execute(
                """
                SELECT COUNT(*) FROM editorial_relation_assertions
                WHERE assertion_id=?
                """,
                (assertion_id,),
            ).fetchone()
            assert accepted is not None
            assert int(accepted[0]) == 1
            current = conn.execute(
                """
                SELECT COUNT(*)
                FROM json_each(?) AS e
                JOIN canonical_entity_heads AS h ON h.entity_id = e.value
                WHERE h.lifecycle = 'ACTIVE'
                """,
                (entity_ids,),
            ).fetchone()
            assert current is not None
            assert int(current[0]) == 2
    finally:
        conn.close()


def test_triage_six_allowlisted_leads_skips_rad02_and_excludes_neo4j(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    _relate(db)
    result = record_first_boot_story_candidates(db, clock=lambda: NOW)
    assert REAL_GRAPHITI_RUNTIME_ENABLED is False
    assert result["ok"] is True
    assert result["schema_version"] == 34
    assert result["graphiti"] is False
    assert result["neo4j"] is False
    assert result["graphiti_runtime_enabled"] is False
    assert result["auto_publish"] is False
    assert result["discord"] is False
    assert result["public_adapter"] is False
    assert result["x_as_publisher"] is False
    assert result["event_hypotheses_v2"] == 0
    assert result["evaluation_handoffs"] == 0
    assert result["story_candidates"] == 6
    assert result["story_candidate_heads"] == 6
    assert result["story_candidate_admission_receipts_v2"] == 6
    assert result["story_candidate_versions"] == 0
    assert result["triage_work_items"] == 6
    assert result["triage_work_item_versions"] == 6
    assert {item["source_id"] for item in result["admitted"]} == set(TRIAGE_SOURCE_IDS)
    assert all(item["authorises_publication"] is False for item in result["admitted"])
    assert all(item["authorises_evidence"] is False for item in result["admitted"])
    assert all(item["revision_id"] != item["item_id"] for item in result["admitted"])
    assert all(item["passage_id"] != item["item_id"] for item in result["admitted"])
    assert {item["source_id"] for item in result["skipped"]} == set()
    assert _table_count(db, "news_leads") == 7
    assert _table_count(db, "extraction_runs") == 6
    assert _table_count(db, "canonical_entities") == 12
    assert _table_count(db, "editorial_relation_assertions") == 6
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        graphiti_rows = conn.execute(
            "SELECT COUNT(*) FROM graphiti_adapter_configurations"
        ).fetchone()
        assert graphiti_rows is not None
        assert int(graphiti_rows[0]) == 0
        neo4j_tables = conn.execute(
            """
            SELECT COUNT(*) FROM sqlite_master
            WHERE type='table' AND name LIKE '%neo4j%'
            """
        ).fetchone()
        assert neo4j_tables is not None
        assert int(neo4j_tables[0]) == 0
        projector = Path(__file__).resolve().parents[1] / "neo4j_host_projection.py"
        before = projector.read_bytes()
        record_first_boot_story_candidates(db, clock=lambda: NOW)
        assert projector.read_bytes() == before
    finally:
        conn.close()


def test_triage_module_does_not_import_neo4j_graphiti_or_publishers() -> None:
    source = Path(__file__).resolve().parents[1] / "discovery_lead_story_candidates.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".", 1)[0])
            imported.add(node.module)
    assert "neo4j" not in imported
    assert "newsroom.neo4j_host_projection" not in imported
    assert "newsroom.graphiti_adapter.adapter" not in imported
    assert "newsroom.auto_publish_grant" not in imported
    assert "newsroom.internal_beta_publish" not in imported


def test_triage_is_idempotent_and_does_not_rerun_prior_commands(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    related = _relate(db)
    first = record_first_boot_story_candidates(db, clock=lambda: NOW)
    second = record_first_boot_story_candidates(db, clock=lambda: NOW)
    assert first["ok"] is True
    assert second["ok"] is True
    assert len(first["admitted"]) == 6
    assert second["admitted"] == []
    assert {item["reason"] for item in second["skipped"]} == {"already-triaged"}
    assert first["extraction_runs"] == related["extraction_runs"] == 6
    assert first["canonical_entities"] == 12
    assert first["editorial_relation_assertions"] == 6
    assert second["story_candidates"] == 6
    assert second["story_candidate_versions"] == 0
    assert second["story_candidate_admission_receipts_v2"] == 6
    assert second["triage_work_items"] == 6
    assert _table_count(db, "extraction_runs") == 6
    assert _table_count(db, "canonical_entities") == 12
    assert _table_count(db, "editorial_relation_assertions") == 6
    assert _table_count(db, "story_candidate_versions") == 0
    assert _table_count(db, "event_hypotheses_v2") == 0
    assert _table_count(db, "evaluation_handoffs") == 0


def test_triage_does_not_remint_publication_bundle(tmp_path: Path) -> None:
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
    _admit(db)
    record_first_boot_extraction(db, clock=lambda: NOW)
    record_first_boot_entity_resolution(db, clock=lambda: NOW)
    record_first_boot_editorial_relations(db, clock=lambda: NOW)
    record_first_boot_story_candidates(db, clock=lambda: NOW)
    after = _bundle_rows(db)
    assert after == before
    assert json.loads(after[0][2])["bundle_digest"] == recorded["bundle_digest"]
    assert after[0][1] == BUNDLE_EVENT
    open_host_store(db).close()
    assert _table_count(db, "story_candidates") == 6
    assert _table_count(db, "story_candidate_versions") == 0
    assert _table_count(db, "triage_work_items") == 6
    assert _table_count(db, "extraction_runs") == 6


def test_cli_triage_leads_after_relate(tmp_path: Path) -> None:
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
        resolved = _cli("resolve-leads", home=home)
        assert resolved.returncode == 0, resolved.stderr + resolved.stdout
        related = _cli("relate-leads", home=home)
        assert related.returncode == 0, related.stderr + related.stdout
        triaged = _cli("triage-leads", home=home)
        assert triaged.returncode == 0, triaged.stderr + triaged.stdout
        payload = json.loads(triaged.stdout)
        assert payload["ok"] is True
        assert payload["schema_version"] == 34
        assert payload["admitted"][0]["source_id"] == "HK-01"
        assert payload["admitted"][0]["authorises_publication"] is False
        assert payload["admitted"][0]["authorises_evidence"] is False
        assert payload["story_candidates"] == 1
        assert payload["story_candidate_heads"] == 1
        assert payload["story_candidate_admission_receipts_v2"] == 1
        assert payload["story_candidate_versions"] == 0
        assert payload["triage_work_items"] == 1
        assert payload["graphiti"] is False
        assert payload["neo4j"] is False
        assert payload["auto_publish"] is False
        db = ledger_path(home)
        assert _table_count(db, "news_leads") == 1
        assert _table_count(db, "story_candidates") == 1
        assert _table_count(db, "story_candidate_versions") == 0
        assert _table_count(db, "triage_work_items") == 1
    finally:
        _cli("stop", home=home)
