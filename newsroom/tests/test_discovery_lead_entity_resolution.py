from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from newsroom.authority.canonical import digest_bytes
from newsroom.discovery_lead_entity_resolution import (
    RESOLVE_SOURCE_IDS,
    record_first_boot_entity_resolution,
)
from newsroom.discovery_lead_extraction import record_first_boot_extraction
from newsroom.entities import (
    EntityCreationDecisionKind,
    EntityContractError,
    resolve_mention_text,
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


def _extract(db: Path) -> dict[str, object]:
    _seed_live_admissions(db)
    _admit(db)
    return record_first_boot_extraction(db, clock=lambda: NOW)


def test_resolve_mention_text_keeps_fixture_bytes_and_json_value_spans() -> None:
    fixture = "Hong Kong Transport Department"
    digest = digest_bytes(fixture.encode("utf-8"))
    assert (
        resolve_mention_text(
            fixture,
            start_byte=0,
            end_byte=len(fixture.encode("utf-8")),
            evidence_text_digest=digest,
        )
        == fixture
    )
    encoded = json.dumps("HK-01", ensure_ascii=False)
    json_digest = digest_bytes(encoded.encode("utf-8"))
    assert (
        resolve_mention_text(
            "HK-01",
            start_byte=12,
            end_byte=12 + len(encoded.encode("utf-8")),
            evidence_text_digest=json_digest,
        )
        == encoded
    )
    with pytest.raises(EntityContractError, match="exact extraction evidence"):
        resolve_mention_text(
            "HK-01",
            start_byte=0,
            end_byte=5,
            evidence_text_digest="sha256:" + "0" * 64,
        )


def test_canonical_entities_come_from_accept_not_extractor_names(tmp_path: Path) -> None:
    db = _db(tmp_path)
    extracted = _extract(db)
    assert extracted["extraction_runs"] == 6
    result = record_first_boot_entity_resolution(db, clock=lambda: NOW)
    assert result["ok"] is True
    assert result["schema_version"] == 34
    assert result["canonical_entities"] == 12
    assert result["entity_mentions"] == 12
    assert result["entity_resolution_decisions"] == 12
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT c.entity_id, c.created_by_kind, c.created_by_decision_id,
                   d.action, m.mention_text, m.normalized_text
            FROM canonical_entities AS c
            JOIN entity_resolution_decisions AS d
              ON d.decision_id = c.created_by_decision_id
            JOIN entity_aliases AS a ON a.entity_id = c.entity_id
            JOIN entity_mentions AS m ON m.mention_id = a.provenance_mention_id
            """
        ).fetchall()
        assert len(rows) == 12
        extractor_names = set()
        for (
            entity_id,
            created_by_kind,
            _decision_id,
            action,
            mention_text,
            normalized_text,
        ) in rows:
            assert created_by_kind == EntityCreationDecisionKind.RESOLUTION.value
            assert action == "ACCEPT"
            assert entity_id != mention_text
            assert entity_id != mention_text.strip('"')
            assert entity_id != normalized_text
            extractor_names.add(mention_text)
            extractor_names.add(mention_text.strip('"'))
        assert all(row[0] not in extractor_names for row in rows)
        assert conn.execute(
            "SELECT COUNT(*) FROM canonical_entities c LEFT JOIN "
            "entity_resolution_decisions d ON d.decision_id=c.created_by_decision_id "
            "WHERE d.decision_id IS NULL OR d.action != 'ACCEPT'"
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_resolve_binds_revision_representation_and_admitted_passages(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    _extract(db)
    result = record_first_boot_entity_resolution(db, clock=lambda: NOW)
    assert result["entity_mentions"] == 12
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT m.revision_id, m.representation_id, m.passage_id, m.item_id,
                   er.revision_id, er.representation_id, rp.passage_id,
                   nl.revision_id, nl.representation_id, nl.item_id
            FROM entity_mentions AS m
            JOIN extraction_runs AS er ON er.run_id = m.run_id
            JOIN extraction_run_passages AS rp
              ON rp.run_id = m.run_id AND rp.passage_id = m.passage_id
            JOIN news_leads AS nl
              ON nl.revision_id = m.revision_id
             AND nl.representation_id = m.representation_id
            """
        ).fetchall()
        assert len(rows) == 12
        for (
            mention_rev,
            mention_rep,
            mention_passage,
            mention_item,
            run_rev,
            run_rep,
            run_passage,
            lead_rev,
            lead_rep,
            lead_item,
        ) in rows:
            assert mention_rev == run_rev == lead_rev
            assert mention_rep == run_rep == lead_rep
            assert mention_passage == run_passage
            assert mention_item == lead_item
            assert mention_rev != mention_item
            assert mention_rep != mention_item
        evidence = conn.execute(
            """
            SELECT m.mention_text, p.subject_placeholder, m.start_byte, m.end_byte,
                   m.evidence_text_digest, ev.evidence_text_digest
            FROM entity_mentions AS m
            JOIN extraction_proposals AS p ON p.proposal_id = m.source_proposal_id
            JOIN extraction_proposal_evidence AS ev
              ON ev.proposal_id = m.source_proposal_id
             AND ev.passage_id = m.passage_id
             AND ev.start_byte = m.start_byte
             AND ev.end_byte = m.end_byte
            """
        ).fetchall()
        assert len(evidence) == 12
        for (
            mention_text,
            placeholder,
            start_byte,
            end_byte,
            mention_digest,
            evidence_digest,
        ) in evidence:
            encoded = mention_text.encode("utf-8")
            assert len(encoded) == end_byte - start_byte
            assert mention_digest == evidence_digest
            assert mention_text == json.dumps(placeholder, ensure_ascii=False)
            assert mention_text != placeholder
    finally:
        conn.close()


def test_resolve_six_allowlisted_extracts_skips_rad02_and_excludes_4c_neo4j(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    _extract(db)
    result = record_first_boot_entity_resolution(db, clock=lambda: NOW)
    assert REAL_GRAPHITI_RUNTIME_ENABLED is False
    assert result["ok"] is True
    assert result["schema_version"] == 34
    assert result["graphiti"] is False
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
    assert result["editorial_relation_decisions"] == 0
    assert {item["source_id"] for item in result["resolved"]} == set(RESOLVE_SOURCE_IDS)
    assert all(item["action"] == "ACCEPT" for item in result["resolved"])
    assert all(item["replayed"] is False for item in result["resolved"])
    assert all(item["revision_id"] != item["item_id"] for item in result["resolved"])
    assert {item["source_id"] for item in result["skipped"]} == set()
    assert _table_count(db, "extraction_runs") == 6
    assert _table_count(db, "extraction_outputs") == 6
    assert _table_count(db, "canonical_entities") == 12
    assert _table_count(db, "editorial_relation_decisions") == 0
    assert _table_count(db, "editorial_relation_proposals") == 0
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        graphiti_rows = conn.execute(
            "SELECT COUNT(*) FROM graphiti_adapter_configurations"
        ).fetchone()
        assert graphiti_rows is not None
        assert int(graphiti_rows[0]) == 0
        relation_kinds = conn.execute(
            """
            SELECT COUNT(*) FROM extraction_proposals
            WHERE proposal_kind = 'RELATION'
            """
        ).fetchone()
        assert relation_kinds is not None
        assert int(relation_kinds[0]) == 6
        bound_relations = conn.execute(
            "SELECT COUNT(*) FROM entity_resolution_dependencies"
        ).fetchone()
        assert bound_relations is not None
        assert int(bound_relations[0]) == 0
    finally:
        conn.close()


def test_resolve_is_idempotent_and_does_not_rerun_extract(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _extract(db)
    first = record_first_boot_entity_resolution(db, clock=lambda: NOW)
    second = record_first_boot_entity_resolution(db, clock=lambda: NOW)
    assert first["ok"] is True
    assert second["ok"] is True
    assert len(first["resolved"]) == 12
    assert second["resolved"] == []
    assert {item["reason"] for item in second["skipped"]} == {"already-resolved"}
    assert second["extraction_runs"] == first["extraction_runs"] == 6
    assert second["canonical_entities"] == first["canonical_entities"] == 12
    assert _table_count(db, "extraction_runs") == 6


def test_resolve_does_not_remint_publication_bundle(tmp_path: Path) -> None:
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
    after = _bundle_rows(db)
    assert after == before
    assert json.loads(after[0][2])["bundle_digest"] == recorded["bundle_digest"]
    assert after[0][1] == BUNDLE_EVENT
    open_host_store(db).close()
    assert _table_count(db, "extraction_runs") == 6
    assert _table_count(db, "canonical_entities") == 12
    assert _table_count(db, "editorial_relation_decisions") == 0


def test_cli_resolve_leads_after_extract(tmp_path: Path) -> None:
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
        payload = json.loads(resolved.stdout)
        assert payload["ok"] is True
        assert payload["schema_version"] == 34
        assert payload["resolved"][0]["source_id"] == "HK-01"
        assert payload["extraction_runs"] == 1
        assert payload["entity_mentions"] == 2
        assert payload["canonical_entities"] == 2
        assert payload["editorial_relation_decisions"] == 0
        assert payload["graphiti"] is False
        assert payload["auto_publish"] is False
        db = ledger_path(home)
        assert _table_count(db, "news_leads") == 1
        assert _table_count(db, "extraction_runs") == 1
        assert _table_count(db, "canonical_entities") == 2
        assert _table_count(db, "editorial_relation_decisions") == 0
    finally:
        _cli("stop", home=home)
