"""Governed live-official Increment 7 Evidence Package.

Reads the six existing Story Candidates and records Evidence Packages bound
to Source Revision, Discovery Representation, admitted passages, 4B ACCEPT
Canonical Entities, the 4C ACCEPT Relation Assertion, and the Story
Candidate. Packages already-admitted material only. No invented evidence.
A package grants no publication authority and no new evidence-acquisition
authority. No publication.decision this pass.

Does not remint, extract, resolve, relate, triage, write Neo4j, extend the
projector, invent RAD-02 / UK-10, write 粵語, or run a model.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

from newsroom.authority import UtcTimestamp, canonical_json_bytes, digest_bytes
from newsroom.authority.types import UUIDv4Id
from newsroom.checks import deterministic_uuid4
from newsroom.discovery_lead_story_candidates import TRIAGE_SOURCE_IDS

EVIDENCE_ID_NAMESPACE = "newsroom.first-boot.evidence-v1"
EVIDENCE_SOURCE_IDS = TRIAGE_SOURCE_IDS
EVIDENCE_SOURCE_ID_SET = frozenset(EVIDENCE_SOURCE_IDS)
PACKAGE_SCHEMA = "newsroom.first-boot.evidence-package.v1"


class LeadEvidencePackageError(ValueError):
    """First-boot live-official Evidence Package failed closed."""


def _evidence_id(*, kind: str, semantic: object) -> str:
    return str(
        deterministic_uuid4(
            UUIDv4Id,
            namespace=EVIDENCE_ID_NAMESPACE,
            semantic_value={"kind": kind, "value": semantic},
        )
    )


def _count_table(path: Path, name: str) -> int:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()
        if present is None:
            return 0
        return int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
    finally:
        conn.close()


def _schema_version(path: Path) -> int:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        return int(conn.execute("PRAGMA user_version").fetchone()[0])
    finally:
        conn.close()


def _require_tables(conn: sqlite3.Connection, names: tuple[str, ...], *, missing: str) -> None:
    for table in names:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if present is None:
            raise LeadEvidencePackageError(f"{table} is absent; {missing}")


def _apply_checked_schema(path: Path) -> None:
    from newsroom.host_store import open_host_store

    open_host_store(path).close()


def _existing_package_candidates(path: Path) -> set[str]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='evidence_package_heads'"
        ).fetchone()
        if present is None:
            return set()
        rows = conn.execute("SELECT candidate_id FROM evidence_package_heads").fetchall()
    finally:
        conn.close()
    return {str(row[0]) for row in rows if row[0]}


def _load_candidate_rows(path: Path) -> tuple[dict[str, Any], ...]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        _require_tables(
            conn,
            (
                "news_leads",
                "story_candidates",
                "story_candidate_heads",
                "extraction_runs",
                "extraction_run_passages",
                "canonical_entities",
                "editorial_relation_assertions",
            ),
            missing="triage-leads first",
        )
        rows = conn.execute(
            """
            SELECT h.candidate_id, h.candidate_bytes,
                   nl.lead_id, nl.signal_id, nl.item_id, nl.revision_id,
                   nl.representation_id, nl.authority_event_id
            FROM story_candidate_heads AS h
            JOIN news_leads AS nl
              ON nl.lead_id = json_extract(CAST(h.candidate_bytes AS TEXT), '$.lead_id')
            ORDER BY h.candidate_id
            """
        ).fetchall()
    finally:
        conn.close()
    return tuple(dict(row) for row in rows)


def _load_accepted_entities(path: Path, run_id: str) -> tuple[dict[str, Any], ...]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT m.mention_id, m.mention_text, m.normalized_text, m.passage_id,
                   m.revision_id, m.representation_id, m.item_id,
                   d.accepted_entity_id, d.accepted_entity_version_id, d.action,
                   h.current_entity_version_id, h.lifecycle
            FROM entity_mentions AS m
            JOIN entity_resolution_proposals AS rp
              ON rp.subject_mention_id = m.mention_id
            JOIN entity_resolution_decision_heads AS dh
              ON dh.resolution_proposal_id = rp.resolution_proposal_id
            JOIN entity_resolution_decisions AS d
              ON d.decision_id = dh.current_decision_id
            JOIN canonical_entity_heads AS h ON h.entity_id = d.accepted_entity_id
            JOIN extraction_proposals AS p ON p.proposal_id = m.source_proposal_id
            WHERE p.run_id = ?
            ORDER BY m.mention_id
            """,
            (run_id,),
        ).fetchall()
    finally:
        conn.close()
    items = tuple(dict(row) for row in rows)
    if len(items) != 2:
        raise LeadEvidencePackageError(
            "Evidence Package requires the two 4B ACCEPT Canonical Entities"
        )
    for item in items:
        if str(item["action"]) != "ACCEPT" or str(item["lifecycle"]) != "ACTIVE":
            raise LeadEvidencePackageError(
                "Evidence Package requires current 4B ACCEPT Canonical Entities"
            )
        if str(item["accepted_entity_version_id"]) != str(item["current_entity_version_id"]):
            raise LeadEvidencePackageError(
                "Evidence Package requires the current Entity Version"
            )
        if str(item["accepted_entity_id"]) in {
            str(item["mention_text"]),
            str(item["normalized_text"]),
            str(item["mention_text"]).strip('"'),
        }:
            raise LeadEvidencePackageError(
                "Canonical Entity identity cannot be an extractor name"
            )
    return items


def _load_accepted_assertion(path: Path, run_id: str) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT a.assertion_id, p.predicate, e.passage_id
            FROM editorial_relation_assertions AS a
            JOIN editorial_relation_decisions AS d ON d.assertion_id = a.assertion_id
            JOIN editorial_relation_proposals AS p ON p.proposal_id = d.proposal_id
            JOIN editorial_relation_extraction_evidence AS e
              ON e.proposal_version_id = d.proposal_version_id
            JOIN extraction_runs AS er ON er.run_id = e.run_id
            WHERE er.run_id = ? AND d.action = 'ACCEPT'
            ORDER BY e.evidence_ordinal
            LIMIT 1
            """,
            (run_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise LeadEvidencePackageError(
            "Evidence Package requires a 4C ACCEPT Relation Assertion"
        )
    return dict(row)


def _candidate_payload(row: dict[str, Any]) -> dict[str, Any]:
    raw = row["candidate_bytes"]
    if isinstance(raw, memoryview):
        raw = raw.tobytes()
    payload = json.loads(bytes(raw).decode("utf-8"))
    if not isinstance(payload, dict):
        raise LeadEvidencePackageError("Story Candidate bytes must be an object")
    return payload


def _eligible_candidates(
    path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    existing = _existing_package_candidates(path)
    selected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in _load_candidate_rows(path):
        payload = _candidate_payload(row)
        source_id = str(payload.get("source_id") or "")
        candidate_id = str(row["candidate_id"])
        lead_id = str(row["lead_id"])
        revision_id = str(row["revision_id"])
        representation_id = str(row["representation_id"])
        item_id = str(row["item_id"])
        passage_id = str(payload.get("passage_id") or "")
        run_id = str(payload.get("run_id") or "")
        binding = {
            "source_id": source_id,
            "lead_id": lead_id,
            "candidate_id": candidate_id,
            "item_id": item_id,
            "revision_id": revision_id,
            "representation_id": representation_id,
            "passage_id": passage_id,
            "run_id": run_id,
        }
        if source_id not in EVIDENCE_SOURCE_ID_SET:
            skipped.append({**binding, "reason": "source-not-in-increment-7-allowlist"})
            continue
        if candidate_id in existing:
            skipped.append({**binding, "reason": "already-packaged"})
            continue
        if str(payload.get("lead_id")) != lead_id:
            raise LeadEvidencePackageError("Evidence Package must bind the Story Candidate lead")
        if str(payload.get("revision_id")) != revision_id:
            raise LeadEvidencePackageError("Story Candidate must bind the Source Revision")
        if str(payload.get("representation_id")) != representation_id:
            raise LeadEvidencePackageError(
                "Story Candidate must bind the Discovery Representation"
            )
        if str(payload.get("item_id")) != item_id:
            raise LeadEvidencePackageError("Story Candidate item_id must match the lead")
        if revision_id == item_id or representation_id == item_id or passage_id == item_id:
            raise LeadEvidencePackageError("JSON identity is not a passage")
        if payload.get("authorises_publication") not in {False, 0}:
            raise LeadEvidencePackageError("Story Candidate must not authorise publication")
        entities = _load_accepted_entities(path, run_id)
        assertion = _load_accepted_assertion(path, run_id)
        entity_ids = [str(entity["accepted_entity_id"]) for entity in entities]
        if list(payload.get("canonical_entity_ids") or []) != entity_ids:
            raise LeadEvidencePackageError(
                "Evidence Package must bind the Story Candidate 4B ACCEPT entities"
            )
        if str(payload.get("relation_assertion_id")) != str(assertion["assertion_id"]):
            raise LeadEvidencePackageError(
                "Evidence Package must bind the Story Candidate 4C ACCEPT assertion"
            )
        for entity in entities:
            if str(entity["revision_id"]) != revision_id:
                raise LeadEvidencePackageError("4B mention must bind the Source Revision")
            if str(entity["representation_id"]) != representation_id:
                raise LeadEvidencePackageError(
                    "4B mention must bind the Discovery Representation"
                )
            if str(entity["passage_id"]) != passage_id:
                raise LeadEvidencePackageError(
                    "4B mention must bind the admitted representation passage"
                )
            if str(entity["item_id"]) in {passage_id, revision_id, representation_id}:
                raise LeadEvidencePackageError("JSON identity is not a passage")
        if str(assertion["passage_id"]) != passage_id:
            raise LeadEvidencePackageError(
                "4C assertion must bind the admitted representation passage"
            )
        selected.append(
            {
                **binding,
                "signal_id": str(row["signal_id"]),
                "work_item_id": str(payload.get("work_item_id") or ""),
                "admission_id": str(payload.get("admission_id") or ""),
                "canonical_entity_ids": entity_ids,
                "canonical_entity_version_ids": [
                    str(entity["accepted_entity_version_id"]) for entity in entities
                ],
                "relation_assertion_id": str(assertion["assertion_id"]),
                "relation_predicate": str(assertion["predicate"]),
                "lead_event_id": str(row["authority_event_id"]),
            }
        )
    selected.sort(
        key=lambda item: (
            EVIDENCE_SOURCE_IDS.index(item["source_id"]),
            item["candidate_id"],
        )
    )
    return selected, skipped


def _package_value(item: dict[str, Any], *, package_id: str) -> dict[str, object]:
    return {
        "schema": PACKAGE_SCHEMA,
        "package_id": package_id,
        "source_id": item["source_id"],
        "lead_id": item["lead_id"],
        "candidate_id": item["candidate_id"],
        "signal_id": item["signal_id"],
        "work_item_id": item["work_item_id"],
        "revision_id": item["revision_id"],
        "representation_id": item["representation_id"],
        "item_id": item["item_id"],
        "passage_id": item["passage_id"],
        "run_id": item["run_id"],
        "admission_id": item["admission_id"],
        "canonical_entity_ids": item["canonical_entity_ids"],
        "canonical_entity_version_ids": item["canonical_entity_version_ids"],
        "relation_assertion_id": item["relation_assertion_id"],
        "relation_predicate": item["relation_predicate"],
        "authorises_publication": False,
        "authorises_evidence": False,
        "authorises_egress": False,
        "auto_publish": False,
        "invented_evidence": False,
        "publication_decision": False,
    }


def _admit_package(
    conn: sqlite3.Connection,
    *,
    item: dict[str, Any],
    recorded_at: str,
) -> dict[str, Any]:
    package_id = _evidence_id(kind="package", semantic=item["candidate_id"])
    package_value = _package_value(item, package_id=package_id)
    package_bytes = canonical_json_bytes(package_value)
    semantic_digest = digest_bytes(
        canonical_json_bytes(
            {
                "candidate_id": item["candidate_id"],
                "revision_id": item["revision_id"],
                "representation_id": item["representation_id"],
                "passage_id": item["passage_id"],
                "lead_id": item["lead_id"],
            }
        )
    )
    receipt_digest = digest_bytes(package_bytes)
    conn.execute(
        "INSERT INTO evidence_packages(package_id,candidate_id,semantic_digest,created_at) "
        "VALUES(?,?,?,?)",
        (package_id, item["candidate_id"], semantic_digest, recorded_at),
    )
    conn.execute(
        "INSERT INTO evidence_package_receipts("
        "receipt_digest,package_id,candidate_id,package_bytes,recorded_at) "
        "VALUES(?,?,?,?,?)",
        (receipt_digest, package_id, item["candidate_id"], package_bytes, recorded_at),
    )
    conn.execute(
        "INSERT INTO evidence_package_heads("
        "package_id,candidate_id,package_bytes,semantic_digest,updated_at) "
        "VALUES(?,?,?,?,?)",
        (package_id, item["candidate_id"], package_bytes, semantic_digest, recorded_at),
    )
    return {
        "source_id": item["source_id"],
        "lead_id": item["lead_id"],
        "candidate_id": item["candidate_id"],
        "package_id": package_id,
        "revision_id": item["revision_id"],
        "representation_id": item["representation_id"],
        "item_id": item["item_id"],
        "passage_id": item["passage_id"],
        "run_id": item["run_id"],
        "canonical_entity_ids": item["canonical_entity_ids"],
        "relation_assertion_id": item["relation_assertion_id"],
        "authorises_publication": False,
        "authorises_evidence": False,
        "invented_evidence": False,
        "publication_decision": False,
        "replayed": False,
    }


def record_first_boot_evidence_packages(
    path: Path,
    *,
    clock: Callable[[], UtcTimestamp] | None = None,
) -> dict[str, Any]:
    from newsroom.graphiti_adapter.models import REAL_GRAPHITI_RUNTIME_ENABLED

    if REAL_GRAPHITI_RUNTIME_ENABLED:
        raise LeadEvidencePackageError("REAL_GRAPHITI_RUNTIME_ENABLED must stay False")
    _apply_checked_schema(path)
    selected, skipped = _eligible_candidates(path)
    if not selected and not skipped:
        raise LeadEvidencePackageError(
            "no retained live-official Story Candidates in the Increment 7 allowlist"
        )
    admitted: list[dict[str, Any]] = []
    if selected:
        now = clock or UtcTimestamp.now
        selected_clock = now if callable(now) else (lambda: now)
        recorded_at = selected_clock().to_text()
        connection = sqlite3.connect(str(path), isolation_level=None, timeout=5)
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            _require_tables(
                connection,
                (
                    "evidence_packages",
                    "evidence_package_receipts",
                    "evidence_package_heads",
                ),
                missing="checked v35 Evidence Package migration first",
            )
            for item in selected:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    admitted.append(
                        _admit_package(connection, item=item, recorded_at=recorded_at)
                    )
                    connection.execute("COMMIT")
                except sqlite3.Error as exc:
                    if connection.in_transaction:
                        connection.execute("ROLLBACK")
                    raise LeadEvidencePackageError(str(exc)) from exc
                except Exception:
                    if connection.in_transaction:
                        connection.execute("ROLLBACK")
                    raise
        finally:
            connection.close()
    return {
        "ok": True,
        "schema_version": _schema_version(path),
        "auto_publish": False,
        "discord": False,
        "public_adapter": False,
        "x_as_publisher": False,
        "graphiti": False,
        "neo4j": False,
        "news_leads": _count_table(path, "news_leads"),
        "extraction_runs": _count_table(path, "extraction_runs"),
        "extraction_outputs": _count_table(path, "extraction_outputs"),
        "entity_mentions": _count_table(path, "entity_mentions"),
        "canonical_entities": _count_table(path, "canonical_entities"),
        "entity_resolution_decisions": _count_table(
            path, "entity_resolution_decisions"
        ),
        "editorial_relation_assertions": _count_table(
            path, "editorial_relation_assertions"
        ),
        "triage_work_items": _count_table(path, "triage_work_items"),
        "story_candidates": _count_table(path, "story_candidates"),
        "story_candidate_heads": _count_table(path, "story_candidate_heads"),
        "story_candidate_admission_receipts_v2": _count_table(
            path, "story_candidate_admission_receipts_v2"
        ),
        "story_candidate_versions": _count_table(path, "story_candidate_versions"),
        "evidence_packages": _count_table(path, "evidence_packages"),
        "evidence_package_heads": _count_table(path, "evidence_package_heads"),
        "evidence_package_receipts": _count_table(path, "evidence_package_receipts"),
        "event_hypotheses_v2": _count_table(path, "event_hypotheses_v2"),
        "evaluation_handoffs": _count_table(path, "evaluation_handoffs"),
        "admitted": admitted,
        "skipped": skipped,
        "graphiti_runtime_enabled": REAL_GRAPHITI_RUNTIME_ENABLED,
    }


__all__ = [
    "EVIDENCE_SOURCE_IDS",
    "LeadEvidencePackageError",
    "record_first_boot_evidence_packages",
]
