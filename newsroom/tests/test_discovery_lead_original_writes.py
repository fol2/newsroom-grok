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
from newsroom.discovery_lead_evidence_packages import (
    record_first_boot_evidence_packages,
)
from newsroom.discovery_lead_extraction import record_first_boot_extraction
from newsroom.discovery_lead_original_writes import (
    WRITE_SOURCE_IDS,
    record_first_boot_original_writes,
)
from newsroom.discovery_lead_story_candidates import (
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


def _package(db: Path) -> dict[str, object]:
    _seed_live_admissions(db)
    _admit(db)
    record_first_boot_extraction(db, clock=lambda: NOW)
    record_first_boot_entity_resolution(db, clock=lambda: NOW)
    record_first_boot_editorial_relations(db, clock=lambda: NOW)
    record_first_boot_story_candidates(db, clock=lambda: NOW)
    return record_first_boot_evidence_packages(db, clock=lambda: NOW)


def test_writes_bind_package_revision_representation_and_passage(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    _package(db)
    result = record_first_boot_original_writes(db, clock=lambda: NOW)
    assert result["ok"] is True
    assert result["original_writes"] == 6
    assert result["original_write_heads"] == 6
    assert result["original_write_receipts"] == 6
    assert result["evidence_packages"] == 6
    assert result["story_candidates"] == 6
    assert result["story_candidate_versions"] == 0
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT json_extract(CAST(w.write_bytes AS TEXT), '$.revision_id'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.representation_id'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.item_id'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.passage_id'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.lead_id'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.candidate_id'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.package_id'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.run_id'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.language'),
                   nl.revision_id, nl.representation_id, nl.item_id, nl.lead_id,
                   rp.passage_id, er.run_id, h.candidate_id, p.package_id
            FROM original_write_heads AS w
            JOIN news_leads AS nl
              ON nl.lead_id = json_extract(CAST(w.write_bytes AS TEXT), '$.lead_id')
            JOIN evidence_package_heads AS p
              ON p.package_id = json_extract(
                   CAST(w.write_bytes AS TEXT), '$.package_id'
                 )
            JOIN story_candidate_heads AS h
              ON h.candidate_id = json_extract(
                   CAST(w.write_bytes AS TEXT), '$.candidate_id'
                 )
            JOIN extraction_runs AS er
              ON er.revision_id = nl.revision_id
             AND er.representation_id = nl.representation_id
            JOIN extraction_run_passages AS rp ON rp.run_id = er.run_id
            """
        ).fetchall()
        assert len(rows) == 6
        for (
            write_rev,
            write_rep,
            write_item,
            write_passage,
            write_lead,
            write_candidate,
            write_package,
            write_run,
            language,
            lead_rev,
            lead_rep,
            lead_item,
            lead_id,
            run_passage,
            run_id,
            candidate_id,
            package_id,
        ) in rows:
            assert write_rev == lead_rev
            assert write_rep == lead_rep
            assert write_item == lead_item
            assert write_lead == lead_id
            assert write_passage == run_passage
            assert write_run == run_id
            assert write_candidate == candidate_id
            assert write_package == package_id
            assert language == "yue"
            assert write_rev != lead_item
            assert write_rep != lead_item
            assert write_passage != lead_item
    finally:
        conn.close()


def test_writes_are_original_yue_bound_to_4b_4c_and_package(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    _package(db)
    result = record_first_boot_original_writes(db, clock=lambda: NOW)
    assert result["canonical_entities"] == 12
    assert result["editorial_relation_assertions"] == 6
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT json_extract(CAST(w.write_bytes AS TEXT), '$.story_title'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.story_body'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.mention_texts'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.relation_assertion_id'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.canonical_entity_ids'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.authorises_publication'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.invented_evidence'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.publication_decision'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.remint'),
                   json_extract(CAST(w.write_bytes AS TEXT), '$.auto_publish')
            FROM original_write_heads AS w
            """
        ).fetchall()
        assert len(rows) == 6
        for (
            title,
            body,
            mention_texts,
            assertion_id,
            entity_ids,
            publication,
            invented,
            decision,
            remint,
            auto_publish,
        ) in rows:
            assert publication in {0, False}
            assert invented in {0, False}
            assert decision in {0, False}
            assert remint in {0, False}
            assert auto_publish in {0, False}
            assert title != BUNDLE_STORY["story_title"]
            assert body != BUNDLE_STORY["story_body"]
            assert "呢篇" in body and "嘅" in body and "唔係" in body
            mentions = json.loads(mention_texts)
            assert len(mentions) == 2
            for mention in mentions:
                assert mention in body
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


def test_write_six_allowlisted_packages_skips_rad02_and_excludes_neo4j(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    _package(db)
    result = record_first_boot_original_writes(db, clock=lambda: NOW)
    assert REAL_GRAPHITI_RUNTIME_ENABLED is False
    assert result["ok"] is True
    assert result["schema_version"] == 36
    assert result["graphiti"] is False
    assert result["neo4j"] is False
    assert result["graphiti_runtime_enabled"] is False
    assert result["auto_publish"] is False
    assert result["discord"] is False
    assert result["public_adapter"] is False
    assert result["x_as_publisher"] is False
    assert result["event_hypotheses_v2"] == 0
    assert result["evaluation_handoffs"] == 0
    assert result["original_writes"] == 6
    assert result["original_write_heads"] == 6
    assert result["original_write_receipts"] == 6
    assert result["evidence_packages"] == 6
    assert result["story_candidate_versions"] == 0
    assert {item["source_id"] for item in result["admitted"]} == set(WRITE_SOURCE_IDS)
    assert all(item["authorises_publication"] is False for item in result["admitted"])
    assert all(item["invented_evidence"] is False for item in result["admitted"])
    assert all(item["publication_decision"] is False for item in result["admitted"])
    assert all(item["remint"] is False for item in result["admitted"])
    assert all(item["language"] == "yue" for item in result["admitted"])
    assert all(item["revision_id"] != item["item_id"] for item in result["admitted"])
    assert all(item["passage_id"] != item["item_id"] for item in result["admitted"])
    assert {item["source_id"] for item in result["skipped"]} == set()
    assert _table_count(db, "news_leads") == 7
    assert _table_count(db, "extraction_runs") == 6
    assert _table_count(db, "canonical_entities") == 12
    assert _table_count(db, "editorial_relation_assertions") == 6
    assert _table_count(db, "story_candidates") == 6
    assert _table_count(db, "evidence_packages") == 6
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
        record_first_boot_original_writes(db, clock=lambda: NOW)
        assert projector.read_bytes() == before
    finally:
        conn.close()


def test_write_module_does_not_import_neo4j_graphiti_or_publishers() -> None:
    source = Path(__file__).resolve().parents[1] / "discovery_lead_original_writes.py"
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
    assert "newsroom.publication_decision" not in imported
    assert "newsroom.publication_bundle" not in imported


def test_write_is_idempotent_and_does_not_rerun_prior_commands(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    packaged = _package(db)
    first = record_first_boot_original_writes(db, clock=lambda: NOW)
    second = record_first_boot_original_writes(db, clock=lambda: NOW)
    assert first["ok"] is True
    assert second["ok"] is True
    assert len(first["admitted"]) == 6
    assert second["admitted"] == []
    assert {item["reason"] for item in second["skipped"]} == {"already-written"}
    assert first["extraction_runs"] == packaged["extraction_runs"] == 6
    assert first["canonical_entities"] == 12
    assert first["editorial_relation_assertions"] == 6
    assert first["story_candidates"] == 6
    assert first["evidence_packages"] == 6
    assert second["original_writes"] == 6
    assert second["story_candidate_versions"] == 0
    assert second["story_candidate_admission_receipts_v2"] == 6
    assert second["triage_work_items"] == 6
    assert _table_count(db, "extraction_runs") == 6
    assert _table_count(db, "canonical_entities") == 12
    assert _table_count(db, "editorial_relation_assertions") == 6
    assert _table_count(db, "story_candidates") == 6
    assert _table_count(db, "evidence_packages") == 6
    assert _table_count(db, "story_candidate_versions") == 0
    assert _table_count(db, "event_hypotheses_v2") == 0
    assert _table_count(db, "evaluation_handoffs") == 0


def test_write_does_not_remint_publication_bundle(tmp_path: Path) -> None:
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
    decisions_before = _table_count(db, "ledger_events")
    _admit(db)
    record_first_boot_extraction(db, clock=lambda: NOW)
    record_first_boot_entity_resolution(db, clock=lambda: NOW)
    record_first_boot_editorial_relations(db, clock=lambda: NOW)
    record_first_boot_story_candidates(db, clock=lambda: NOW)
    record_first_boot_evidence_packages(db, clock=lambda: NOW)
    record_first_boot_original_writes(db, clock=lambda: NOW)
    after = _bundle_rows(db)
    assert after == before
    assert json.loads(after[0][2])["bundle_digest"] == recorded["bundle_digest"]
    assert after[0][1] == BUNDLE_EVENT
    open_host_store(db).close()
    assert _table_count(db, "original_writes") == 6
    assert _table_count(db, "evidence_packages") == 6
    assert _table_count(db, "story_candidates") == 6
    assert _table_count(db, "story_candidate_versions") == 0
    assert _table_count(db, "triage_work_items") == 6
    assert _table_count(db, "extraction_runs") == 6
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        seq15 = conn.execute(
            """
            SELECT e.ledger_seq, e.event_type
            FROM ledger_events e
            WHERE e.event_type=?
            ORDER BY e.ledger_seq
            """,
            (BUNDLE_EVENT,),
        ).fetchall()
        assert seq15
        decisions = conn.execute(
            """
            SELECT COUNT(*) FROM ledger_events
            WHERE event_type LIKE 'publication.decision%'
            """
        ).fetchone()
        assert decisions is not None
        assert int(decisions[0]) == 1
        titles = conn.execute(
            """
            SELECT json_extract(CAST(write_bytes AS TEXT), '$.story_title'),
                   json_extract(CAST(write_bytes AS TEXT), '$.story_body')
            FROM original_write_heads
            """
        ).fetchall()
        assert titles
        for title, body in titles:
            assert title != BUNDLE_STORY["story_title"]
            assert body != BUNDLE_STORY["story_body"]
    finally:
        conn.close()
    assert _table_count(db, "ledger_events") >= decisions_before


def test_cli_write_leads_after_evidence(tmp_path: Path) -> None:
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
        packaged = _cli("evidence-leads", home=home)
        assert packaged.returncode == 0, packaged.stderr + packaged.stdout
        written = _cli("write-leads", home=home)
        assert written.returncode == 0, written.stderr + written.stdout
        payload = json.loads(written.stdout)
        assert payload["ok"] is True
        assert payload["schema_version"] == 36
        assert payload["admitted"][0]["source_id"] == "HK-01"
        assert payload["admitted"][0]["language"] == "yue"
        assert payload["admitted"][0]["authorises_publication"] is False
        assert payload["admitted"][0]["invented_evidence"] is False
        assert payload["admitted"][0]["publication_decision"] is False
        assert payload["admitted"][0]["remint"] is False
        assert "呢篇" in payload["admitted"][0]["story_body"]
        assert payload["original_writes"] == 1
        assert payload["original_write_heads"] == 1
        assert payload["original_write_receipts"] == 1
        assert payload["evidence_packages"] == 1
        assert payload["story_candidates"] == 1
        assert payload["story_candidate_versions"] == 0
        assert payload["graphiti"] is False
        assert payload["neo4j"] is False
        assert payload["auto_publish"] is False
        db = ledger_path(home)
        assert _table_count(db, "news_leads") == 1
        assert _table_count(db, "story_candidates") == 1
        assert _table_count(db, "evidence_packages") == 1
        assert _table_count(db, "original_writes") == 1
        assert _table_count(db, "story_candidate_versions") == 0
        assert _table_count(db, "triage_work_items") == 1
    finally:
        _cli("stop", home=home)
