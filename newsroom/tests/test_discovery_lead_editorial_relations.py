from __future__ import annotations

import ast
import json
import sqlite3
from pathlib import Path

from newsroom.discovery_lead_editorial_relations import (
    RELATE_SOURCE_IDS,
    record_first_boot_editorial_relations,
)
from newsroom.discovery_lead_entity_resolution import (
    record_first_boot_entity_resolution,
)
from newsroom.discovery_lead_extraction import record_first_boot_extraction
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


def _resolve(db: Path) -> dict[str, object]:
    _seed_live_admissions(db)
    _admit(db)
    record_first_boot_extraction(db, clock=lambda: NOW)
    return record_first_boot_entity_resolution(db, clock=lambda: NOW)


def test_relations_bind_current_4b_canonical_entity_versions_not_extractor_names(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    resolved = _resolve(db)
    assert resolved["canonical_entities"] == 12
    result = record_first_boot_editorial_relations(db, clock=lambda: NOW)
    assert result["ok"] is True
    assert result["schema_version"] == 34
    assert result["editorial_relation_decisions"] == 6
    assert result["editorial_relation_assertions"] == 6
    assert result["canonical_entities"] == 12
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT p.predicate, s.entity_id, s.entity_version_id,
                   o.entity_id, o.entity_version_id,
                   hs.current_entity_version_id, ho.current_entity_version_id,
                   ms.mention_text, mo.mention_text, ms.normalized_text,
                   mo.normalized_text, xs.local_id, xo.local_id
            FROM editorial_relation_proposals AS p
            JOIN editorial_relation_endpoints AS s
              ON s.endpoint_digest = p.subject_endpoint_digest
            JOIN editorial_relation_endpoints AS o
              ON o.endpoint_digest = p.object_endpoint_digest
            JOIN canonical_entity_heads AS hs ON hs.entity_id = s.entity_id
            JOIN canonical_entity_heads AS ho ON ho.entity_id = o.entity_id
            JOIN entity_aliases AS asrc ON asrc.entity_id = s.entity_id
            JOIN entity_mentions AS ms ON ms.mention_id = asrc.provenance_mention_id
            JOIN extraction_proposals AS xs ON xs.proposal_id = ms.source_proposal_id
            JOIN entity_aliases AS aobj ON aobj.entity_id = o.entity_id
            JOIN entity_mentions AS mo ON mo.mention_id = aobj.provenance_mention_id
            JOIN extraction_proposals AS xo ON xo.proposal_id = mo.source_proposal_id
            """
        ).fetchall()
        assert len(rows) == 6
        extractor_names: set[str] = set()
        for (
            predicate,
            subject_id,
            subject_version,
            object_id,
            object_version,
            subject_current,
            object_current,
            subject_text,
            object_text,
            subject_normalized,
            object_normalized,
            subject_local,
            object_local,
        ) in rows:
            assert predicate == "SAME_PROCESS_AS"
            assert subject_version == subject_current
            assert object_version == object_current
            assert subject_id != object_id
            assert {subject_local, object_local} == {"entity.source", "entity.item"}
            for name in (
                subject_id,
                object_id,
                subject_version,
                object_version,
            ):
                assert name != subject_text
                assert name != object_text
                assert name != subject_text.strip('"')
                assert name != object_text.strip('"')
                assert name != subject_normalized
                assert name != object_normalized
                assert name not in {"entity.source", "entity.item", "relation.source-about-item"}
            extractor_names.update(
                {
                    subject_text,
                    object_text,
                    subject_text.strip('"'),
                    object_text.strip('"'),
                    subject_normalized,
                    object_normalized,
                }
            )
        assert all(row[1] not in extractor_names and row[3] not in extractor_names for row in rows)
        hints = conn.execute(
            """
            SELECT COUNT(*) FROM extraction_proposals
            WHERE proposal_kind='RELATION' AND predicate_hint='ABOUT_EVENT'
            """
        ).fetchone()
        assert hints is not None
        assert int(hints[0]) == 6
        admitted_about = conn.execute(
            """
            SELECT COUNT(*) FROM editorial_relation_proposals
            WHERE predicate='ABOUT_EVENT'
            """
        ).fetchone()
        assert admitted_about is not None
        assert int(admitted_about[0]) == 0
        stale = conn.execute(
            """
            SELECT COUNT(*)
            FROM editorial_relation_endpoints e
            JOIN canonical_entity_heads h ON h.entity_id = e.entity_id
            WHERE e.kind='CANONICAL_ENTITY_VERSION'
              AND e.entity_version_id != h.current_entity_version_id
            """
        ).fetchone()
        assert stale is not None
        assert int(stale[0]) == 0
    finally:
        conn.close()


def test_relations_bind_revision_representation_and_admitted_passages(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    _resolve(db)
    result = record_first_boot_editorial_relations(db, clock=lambda: NOW)
    assert result["editorial_relation_assertions"] == 6
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT ev.run_id, ev.passage_id, ev.start_byte, ev.end_byte,
                   er.revision_id, er.representation_id, er.item_id,
                   rp.passage_id, nl.revision_id, nl.representation_id, nl.item_id,
                   p.proposal_kind, p.local_id
            FROM editorial_relation_extraction_evidence AS ev
            JOIN extraction_runs AS er ON er.run_id = ev.run_id
            JOIN extraction_run_passages AS rp
              ON rp.run_id = ev.run_id AND rp.passage_id = ev.passage_id
            JOIN news_leads AS nl
              ON nl.revision_id = er.revision_id
             AND nl.representation_id = er.representation_id
            JOIN extraction_proposals AS p
              ON p.proposal_id = ev.source_proposal_id
            """
        ).fetchall()
        assert len(rows) == 12
        for (
            _run_id,
            evidence_passage,
            start_byte,
            end_byte,
            run_rev,
            run_rep,
            run_item,
            run_passage,
            lead_rev,
            lead_rep,
            lead_item,
            kind,
            local_id,
        ) in rows:
            assert kind == "RELATION"
            assert local_id == "relation.source-about-item"
            assert evidence_passage == run_passage
            assert run_rev == lead_rev
            assert run_rep == lead_rep
            assert run_item == lead_item
            assert run_rev != lead_item
            assert run_rep != lead_item
            assert evidence_passage != lead_item
            assert end_byte > start_byte
        json_as_passage = conn.execute(
            """
            SELECT COUNT(*)
            FROM editorial_relation_extraction_evidence ev
            JOIN entity_mentions m ON m.passage_id = ev.passage_id
            WHERE ev.passage_id = m.mention_text
               OR ev.passage_id = m.normalized_text
            """
        ).fetchone()
        assert json_as_passage is not None
        assert int(json_as_passage[0]) == 0
    finally:
        conn.close()


def test_relate_six_allowlisted_extracts_skips_rad02_and_excludes_neo4j(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    _resolve(db)
    result = record_first_boot_editorial_relations(db, clock=lambda: NOW)
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
    assert result["extraction_runs"] == 6
    assert result["extraction_outputs"] == 6
    assert result["entity_mentions"] == 12
    assert result["canonical_entities"] == 12
    assert result["entity_resolution_decisions"] == 12
    assert result["editorial_relation_proposals"] == 6
    assert result["editorial_relation_decisions"] == 6
    assert result["editorial_relation_assertions"] == 6
    assert {item["source_id"] for item in result["related"]} == set(RELATE_SOURCE_IDS)
    assert all(item["action"] == "ACCEPT" for item in result["related"])
    assert all(item["predicate"] == "SAME_PROCESS_AS" for item in result["related"])
    assert all(item["replayed"] is False for item in result["related"])
    assert all(item["revision_id"] != item["item_id"] for item in result["related"])
    assert {item["source_id"] for item in result["skipped"]} == set()
    assert _table_count(db, "extraction_runs") == 6
    assert _table_count(db, "canonical_entities") == 12
    assert _table_count(db, "editorial_relation_decisions") == 6
    assert _table_count(db, "editorial_relation_assertions") == 6
    assert _table_count(db, "entity_resolution_dependencies") == 12
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
    finally:
        conn.close()


def test_relate_module_does_not_import_neo4j_graphiti_or_publishers() -> None:
    source = Path(__file__).resolve().parents[1] / "discovery_lead_editorial_relations.py"
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


def test_relate_is_idempotent_and_does_not_rerun_extract_or_resolve(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    _resolve(db)
    first = record_first_boot_editorial_relations(db, clock=lambda: NOW)
    second = record_first_boot_editorial_relations(db, clock=lambda: NOW)
    assert first["ok"] is True
    assert second["ok"] is True
    assert len(first["related"]) == 6
    assert second["related"] == []
    assert {item["reason"] for item in second["skipped"]} == {"already-related"}
    assert second["extraction_runs"] == first["extraction_runs"] == 6
    assert second["canonical_entities"] == first["canonical_entities"] == 12
    assert second["editorial_relation_decisions"] == 6
    assert _table_count(db, "extraction_runs") == 6
    assert _table_count(db, "canonical_entities") == 12
    assert _table_count(db, "editorial_relation_assertions") == 6


def test_relate_does_not_remint_publication_bundle(tmp_path: Path) -> None:
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
    after = _bundle_rows(db)
    assert after == before
    assert json.loads(after[0][2])["bundle_digest"] == recorded["bundle_digest"]
    assert after[0][1] == BUNDLE_EVENT
    open_host_store(db).close()
    assert _table_count(db, "extraction_runs") == 6
    assert _table_count(db, "canonical_entities") == 12
    assert _table_count(db, "editorial_relation_decisions") == 6
    assert _table_count(db, "editorial_relation_assertions") == 6


def test_cli_relate_leads_after_resolve(tmp_path: Path) -> None:
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
        payload = json.loads(related.stdout)
        assert payload["ok"] is True
        assert payload["schema_version"] == 34
        assert payload["related"][0]["source_id"] == "HK-01"
        assert payload["related"][0]["action"] == "ACCEPT"
        assert payload["related"][0]["predicate"] == "SAME_PROCESS_AS"
        assert payload["extraction_runs"] == 1
        assert payload["canonical_entities"] == 2
        assert payload["editorial_relation_decisions"] == 1
        assert payload["editorial_relation_assertions"] == 1
        assert payload["graphiti"] is False
        assert payload["neo4j"] is False
        assert payload["auto_publish"] is False
        db = ledger_path(home)
        assert _table_count(db, "news_leads") == 1
        assert _table_count(db, "extraction_runs") == 1
        assert _table_count(db, "canonical_entities") == 2
        assert _table_count(db, "editorial_relation_decisions") == 1
        assert _table_count(db, "editorial_relation_assertions") == 1
    finally:
        _cli("stop", home=home)
