"""Governed live-official Increment 6 Story Candidate triage.

Reads the six retained LEAD_QUEUED News Leads, records bounded Triage Work
Items, and admits Story Candidates bound to Source Revision, Discovery
Representation, admitted passages, 4B Canonical Entities and 4C ACCEPT
relations. A Story Candidate grants no publication or evidence-acquisition
authority.

Does not remint, extract, resolve, relate, write Neo4j, extend the projector,
invent RAD-02 / UK-10, or run a model.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from newsroom.authority import UtcTimestamp, canonical_json_bytes, digest_bytes
from newsroom.authority.types import UUIDv4Id
from newsroom.checks import deterministic_uuid4
from newsroom.discovery_lead_admission import ID_NAMESPACE as LEAD_ID_NAMESPACE
from newsroom.discovery_lead_extraction import (
    EXTRACT_SOURCE_ID_SET,
    EXTRACT_SOURCE_IDS,
)
from newsroom.increment6.outcomes import PriorityLane, PrioritySelection, ReasonReference
from newsroom.increment6.work_items import (
    DecisionLeadBinding,
    RetrievalBindingState,
    RetrievalInputBinding,
    TriageWorkItem,
    TriageWorkItemStore,
    TriageWorkItemVersion,
    WorkItemContractError,
)
from newsroom.sources import SourceDefinitionId

TRIAGE_ID_NAMESPACE = "newsroom.first-boot.triage-v1"
TRIAGE_SOURCE_IDS = EXTRACT_SOURCE_IDS
TRIAGE_SOURCE_ID_SET = EXTRACT_SOURCE_ID_SET
COLLISION_NAMESPACE = "newsroom.first-boot.live-official-triage"
CANDIDATE_SCHEMA = "newsroom.first-boot.story-candidate.v1"
CANDIDATE_VERSION_SCHEMA = "newsroom.first-boot.story-candidate-version.v1"


class LeadStoryCandidateError(ValueError):
    """First-boot live-official Story Candidate triage failed closed."""


def _triage_id(*, kind: str, semantic: object) -> str:
    return str(
        deterministic_uuid4(
            UUIDv4Id,
            namespace=TRIAGE_ID_NAMESPACE,
            semantic_value={"kind": kind, "value": semantic},
        )
    )


def _lead_definition_id(source_id: str) -> SourceDefinitionId:
    return deterministic_uuid4(
        SourceDefinitionId,
        namespace=LEAD_ID_NAMESPACE,
        semantic_value={"kind": "definition", "value": source_id},
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


def _definition_map() -> dict[str, str]:
    return {
        str(_lead_definition_id(source_id)): source_id for source_id in EXTRACT_SOURCE_IDS
    }


def _require_tables(conn: sqlite3.Connection, names: tuple[str, ...], *, missing: str) -> None:
    for table in names:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if present is None:
            raise LeadStoryCandidateError(f"{table} is absent; {missing}")


def _existing_candidate_leads(path: Path) -> set[str]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='story_candidates'"
        ).fetchone()
        if present is None:
            return set()
        rows = conn.execute(
            """
            SELECT json_extract(CAST(h.candidate_bytes AS TEXT), '$.lead_id')
            FROM story_candidate_heads AS h
            """
        ).fetchall()
    finally:
        conn.close()
    return {str(row[0]) for row in rows if row[0]}


def _load_lead_rows(path: Path) -> tuple[dict[str, Any], ...]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        _require_tables(
            conn,
            (
                "news_leads",
                "lead_disposition_decisions",
                "lead_disposition_heads",
                "extraction_runs",
                "extraction_run_passages",
            ),
            missing="extract-leads first",
        )
        _require_tables(
            conn,
            (
                "canonical_entities",
                "entity_resolution_decisions",
                "editorial_relation_assertions",
            ),
            missing="relate-leads first",
        )
        rows = conn.execute(
            """
            SELECT nl.lead_id, nl.signal_id, nl.promoting_gate_decision_id,
                   nl.definition_id, nl.definition_version_id, nl.item_id,
                   nl.revision_id, nl.representation_id, nl.canonical_digest,
                   nl.canonical_bytes, nl.authority_event_id,
                   nl.authority_aggregate_version,
                   d.decision_id, d.canonical_digest AS disposition_digest,
                   d.canonical_bytes AS disposition_bytes,
                   d.authority_event_id AS disposition_event_id,
                   d.authority_aggregate_version AS disposition_aggregate_version,
                   d.decision_ordinal, d.previous_decision_id, d.outcome,
                   er.run_id, rp.passage_id, rp.admission_id, rp.access_decision_id,
                   rp.byte_offset, rp.byte_length
            FROM news_leads AS nl
            JOIN lead_disposition_heads AS dh ON dh.lead_id = nl.lead_id
            JOIN lead_disposition_decisions AS d
              ON d.decision_id = dh.current_decision_id
            JOIN extraction_runs AS er
              ON er.revision_id = nl.revision_id
             AND er.representation_id = nl.representation_id
            JOIN extraction_run_passages AS rp ON rp.run_id = er.run_id
            WHERE d.outcome = 'LEAD_QUEUED_FOR_TRIAGE'
            ORDER BY nl.lead_id
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
        raise LeadStoryCandidateError(
            "Story Candidate requires the two 4B ACCEPT Canonical Entities"
        )
    for item in items:
        if str(item["action"]) != "ACCEPT" or str(item["lifecycle"]) != "ACTIVE":
            raise LeadStoryCandidateError(
                "Story Candidate requires current 4B ACCEPT Canonical Entities"
            )
        if str(item["accepted_entity_version_id"]) != str(item["current_entity_version_id"]):
            raise LeadStoryCandidateError(
                "Story Candidate requires the current Entity Version"
            )
        if str(item["accepted_entity_id"]) in {
            str(item["mention_text"]),
            str(item["normalized_text"]),
            str(item["mention_text"]).strip('"'),
        }:
            raise LeadStoryCandidateError(
                "Canonical Entity identity cannot be an extractor name"
            )
    return items


def _load_accepted_assertion(path: Path, run_id: str) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT a.assertion_id, p.predicate, e.passage_id, e.start_byte, e.end_byte,
                   e.source_proposal_id
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
        raise LeadStoryCandidateError(
            "Story Candidate requires a 4C ACCEPT Relation Assertion"
        )
    return dict(row)


def _eligible_leads(
    path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    definitions = _definition_map()
    existing = _existing_candidate_leads(path)
    selected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in _load_lead_rows(path):
        source_id = definitions.get(str(row["definition_id"]))
        lead_id = str(row["lead_id"])
        revision_id = str(row["revision_id"])
        representation_id = str(row["representation_id"])
        item_id = str(row["item_id"])
        passage_id = str(row["passage_id"])
        binding = {
            "source_id": source_id,
            "lead_id": lead_id,
            "item_id": item_id,
            "revision_id": revision_id,
            "representation_id": representation_id,
            "passage_id": passage_id,
            "run_id": str(row["run_id"]),
        }
        if source_id not in TRIAGE_SOURCE_ID_SET:
            skipped.append({**binding, "reason": "source-not-in-increment-6-allowlist"})
            continue
        if lead_id in existing:
            skipped.append({**binding, "reason": "already-triaged"})
            continue
        if revision_id == item_id or representation_id == item_id or passage_id == item_id:
            raise LeadStoryCandidateError("JSON identity is not a passage")
        entities = _load_accepted_entities(path, str(row["run_id"]))
        assertion = _load_accepted_assertion(path, str(row["run_id"]))
        for entity in entities:
            if str(entity["revision_id"]) != revision_id:
                raise LeadStoryCandidateError(
                    "4B mention must bind the Source Revision"
                )
            if str(entity["representation_id"]) != representation_id:
                raise LeadStoryCandidateError(
                    "4B mention must bind the Discovery Representation"
                )
            if str(entity["passage_id"]) != passage_id:
                raise LeadStoryCandidateError(
                    "4B mention must bind the admitted representation passage"
                )
            if str(entity["item_id"]) in {passage_id, revision_id, representation_id}:
                raise LeadStoryCandidateError("JSON identity is not a passage")
        if str(assertion["passage_id"]) != passage_id:
            raise LeadStoryCandidateError(
                "4C assertion must bind the admitted representation passage"
            )
        selected.append(
            {
                **binding,
                "signal_id": str(row["signal_id"]),
                "lead_digest": str(row["canonical_digest"]),
                "lead_event_id": str(row["authority_event_id"]),
                "lead_aggregate_version": int(row["authority_aggregate_version"]),
                "gate_decision_id": str(row["promoting_gate_decision_id"]),
                "definition_id": str(row["definition_id"]),
                "definition_version_id": str(row["definition_version_id"]),
                "disposition_id": str(row["decision_id"]),
                "disposition_digest": str(row["disposition_digest"]),
                "disposition_event_id": str(row["disposition_event_id"]),
                "disposition_aggregate_version": int(
                    row["disposition_aggregate_version"]
                ),
                "disposition_ordinal": int(row["decision_ordinal"]),
                "previous_disposition_id": row["previous_decision_id"],
                "disposition_outcome": str(row["outcome"]),
                "admission_id": str(row["admission_id"]),
                "access_decision_id": str(row["access_decision_id"]),
                "byte_offset": int(row["byte_offset"]),
                "byte_length": int(row["byte_length"]),
                "entities": entities,
                "assertion": assertion,
            }
        )
    selected.sort(
        key=lambda item: (
            TRIAGE_SOURCE_IDS.index(item["source_id"]),
            item["lead_id"],
        )
    )
    return selected, skipped


def _decision_binding(item: dict[str, Any]) -> DecisionLeadBinding:
    previous = item["previous_disposition_id"]
    return DecisionLeadBinding(
        item["lead_id"],
        item["lead_digest"],
        item["lead_event_id"],
        item["lead_aggregate_version"],
        item["gate_decision_id"],
        item["definition_id"],
        item["definition_version_id"],
        item["disposition_id"],
        item["disposition_digest"],
        item["disposition_event_id"],
        item["disposition_aggregate_version"],
        item["disposition_ordinal"],
        None if previous is None else str(previous),
        item["disposition_outcome"],
    )


def _pending_retrieval(lead_id: str) -> RetrievalInputBinding:
    request_id = _triage_id(kind="retrieval-request", semantic=lead_id)
    key = f"first-boot-triage-retrieval-{lead_id}"
    request = canonical_json_bytes({"idempotency_key": key, "request_id": request_id})
    return RetrievalInputBinding(
        RetrievalBindingState.REQUEST_PENDING,
        request_id,
        key,
        digest_bytes(request),
        request,
    )


def _candidate_value(item: dict[str, Any], *, work_item_id: str) -> dict[str, object]:
    entities = item["entities"]
    assertion = item["assertion"]
    return {
        "schema": CANDIDATE_SCHEMA,
        "source_id": item["source_id"],
        "lead_id": item["lead_id"],
        "signal_id": item["signal_id"],
        "work_item_id": work_item_id,
        "revision_id": item["revision_id"],
        "representation_id": item["representation_id"],
        "item_id": item["item_id"],
        "passage_id": item["passage_id"],
        "run_id": item["run_id"],
        "admission_id": item["admission_id"],
        "canonical_entity_ids": [
            str(entity["accepted_entity_id"]) for entity in entities
        ],
        "canonical_entity_version_ids": [
            str(entity["accepted_entity_version_id"]) for entity in entities
        ],
        "relation_assertion_id": str(assertion["assertion_id"]),
        "relation_predicate": str(assertion["predicate"]),
        "authorises_publication": False,
        "authorises_evidence": False,
        "authorises_egress": False,
        "auto_publish": False,
    }


def _open_store(path: Path) -> tuple[sqlite3.Connection, TriageWorkItemStore]:
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=5)
    connection.execute("PRAGMA foreign_keys=ON")
    return connection, TriageWorkItemStore(connection)


def _admit_candidate(
    conn: sqlite3.Connection,
    *,
    item: dict[str, Any],
    work_item_id: str,
    recorded_at: str,
) -> dict[str, Any]:
    candidate_id = _triage_id(kind="candidate", semantic=item["lead_id"])
    version_id = _triage_id(kind="candidate-version", semantic=item["lead_id"])
    decision_id = _triage_id(kind="candidate-decision", semantic=item["lead_id"])
    request_id = _triage_id(kind="candidate-request", semantic=item["lead_id"])
    hypothesis_version_id = _triage_id(
        kind="unverified-hypothesis-identity", semantic=item["lead_id"]
    )
    candidate_value = _candidate_value(item, work_item_id=work_item_id)
    candidate_bytes = canonical_json_bytes(candidate_value)
    semantic_scope = digest_bytes(
        canonical_json_bytes(
            {
                "revision_id": item["revision_id"],
                "representation_id": item["representation_id"],
                "lead_id": item["lead_id"],
                "passage_id": item["passage_id"],
            }
        )
    )
    version_value = {
        "schema": CANDIDATE_VERSION_SCHEMA,
        "candidate_id": candidate_id,
        "version_id": version_id,
        "ordinal": 1,
        "work_item_id": work_item_id,
        "route": "NEW_EVENT",
        "candidate": candidate_value,
        "authorises_publication": False,
        "authorises_evidence": False,
    }
    version_bytes = canonical_json_bytes(version_value)
    version_digest = digest_bytes(version_bytes)
    collision_request = canonical_json_bytes(
        {
            "namespace": COLLISION_NAMESPACE,
            "lead_id": item["lead_id"],
            "revision_id": item["revision_id"],
            "representation_id": item["representation_id"],
        }
    )
    collision_decision = canonical_json_bytes(
        {
            "outcome": "ADMISSIBLE",
            "reason": "LIVE_OFFICIAL_NEW_EVENT",
            "lead_id": item["lead_id"],
        }
    )
    admission_bytes = canonical_json_bytes(
        {
            "schema": CANDIDATE_SCHEMA,
            "candidate_id": candidate_id,
            "version_id": version_id,
            "work_item_id": work_item_id,
            "outcome": "ADMISSIBLE",
            "authorises_publication": False,
            "authorises_evidence": False,
        }
    )
    admission_digest = digest_bytes(admission_bytes)
    request_digest = digest_bytes(
        canonical_json_bytes({"request_id": request_id, "lead_id": item["lead_id"]})
    )
    actor_digest = digest_bytes(
        canonical_json_bytes({"producer": "first-boot.live-official-triage"})
    )
    collision_request_digest = digest_bytes(collision_request)
    collision_decision_digest = digest_bytes(collision_decision)
    conn.execute(
        "INSERT INTO story_candidates(candidate_id,semantic_collision_digest,created_at) "
        "VALUES(?,?,?)",
        (candidate_id, semantic_scope, recorded_at),
    )
    conn.execute(
        "INSERT INTO story_candidate_admission_receipts_v2 VALUES("
        "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            admission_digest,
            request_id,
            request_digest,
            actor_digest,
            f"first-boot-triage-{item['lead_id']}",
            candidate_id,
            item["lead_event_id"],
            decision_id,
            admission_bytes,
            candidate_id,
            candidate_bytes,
            version_id,
            1,
            version_bytes,
            version_digest,
            None,
            None,
            digest_bytes(candidate_bytes),
            semantic_scope,
            hypothesis_version_id,
            collision_decision_digest,
            canonical_json_bytes([item["disposition_id"]]),
            collision_request,
            collision_request_digest,
            collision_decision,
            collision_decision_digest,
            None,
            None,
            None,
            None,
            recorded_at,
        ),
    )
    conn.execute(
        "INSERT INTO story_candidate_collision_bindings VALUES(?,?,?,?,?,?,?,?)",
        (
            COLLISION_NAMESPACE,
            semantic_scope,
            candidate_id,
            semantic_scope,
            admission_digest,
            request_digest,
            collision_decision_digest,
            collision_decision,
        ),
    )
    conn.execute(
        "INSERT INTO story_candidate_heads VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            candidate_id,
            candidate_bytes,
            semantic_scope,
            version_id,
            1,
            version_digest,
            admission_digest,
            COLLISION_NAMESPACE,
            semantic_scope,
            recorded_at,
        ),
    )
    return {
        "source_id": item["source_id"],
        "lead_id": item["lead_id"],
        "work_item_id": work_item_id,
        "candidate_id": candidate_id,
        "version_id": version_id,
        "revision_id": item["revision_id"],
        "representation_id": item["representation_id"],
        "item_id": item["item_id"],
        "passage_id": item["passage_id"],
        "run_id": item["run_id"],
        "canonical_entity_ids": [
            str(entity["accepted_entity_id"]) for entity in item["entities"]
        ],
        "relation_assertion_id": str(item["assertion"]["assertion_id"]),
        "authorises_publication": False,
        "authorises_evidence": False,
        "replayed": False,
    }


def record_first_boot_story_candidates(
    path: Path,
    *,
    clock: Callable[[], UtcTimestamp] | None = None,
) -> dict[str, Any]:
    from newsroom.graphiti_adapter.models import REAL_GRAPHITI_RUNTIME_ENABLED

    if REAL_GRAPHITI_RUNTIME_ENABLED:
        raise LeadStoryCandidateError("REAL_GRAPHITI_RUNTIME_ENABLED must stay False")
    selected, skipped = _eligible_leads(path)
    if not selected and not skipped:
        raise LeadStoryCandidateError(
            "no retained live-official leads in the Increment 6 allowlist"
        )
    admitted: list[dict[str, Any]] = []
    if selected:
        now = clock or UtcTimestamp.now
        selected_clock = now if callable(now) else (lambda: now)
        recorded_at = selected_clock().to_text()
        connection, store = _open_store(path)
        try:
            for item in selected:
                try:
                    decision = _decision_binding(item)
                    work_item = TriageWorkItem.create((decision,))
                    version_id = str(
                        uuid.uuid5(uuid.NAMESPACE_URL, f"{work_item.work_item_id}|1")
                    )
                    version = TriageWorkItemVersion.create(
                        work_item_id=work_item.work_item_id,
                        ordinal=1,
                        previous_version_id=None,
                        decision_leads=work_item.decision_leads,
                        context_leads=(),
                        retrieval=_pending_retrieval(item["lead_id"]),
                        priority=PrioritySelection(
                            work_item.work_item_id,
                            version_id,
                            PriorityLane.ROUTINE,
                            (ReasonReference("news_lead", item["lead_id"]),),
                        ),
                    )
                    stored = store.create_or_replay(work_item, version)
                except WorkItemContractError as exc:
                    raise LeadStoryCandidateError(str(exc)) from exc
                connection.execute("BEGIN IMMEDIATE")
                try:
                    admitted.append(
                        _admit_candidate(
                            connection,
                            item=item,
                            work_item_id=stored.work_item_id,
                            recorded_at=recorded_at,
                        )
                    )
                    connection.execute("COMMIT")
                except sqlite3.Error as exc:
                    if connection.in_transaction:
                        connection.execute("ROLLBACK")
                    raise LeadStoryCandidateError(str(exc)) from exc
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
        "triage_work_item_versions": _count_table(path, "triage_work_item_versions"),
        "story_candidates": _count_table(path, "story_candidates"),
        "story_candidate_heads": _count_table(path, "story_candidate_heads"),
        "story_candidate_admission_receipts_v2": _count_table(
            path, "story_candidate_admission_receipts_v2"
        ),
        "story_candidate_versions": _count_table(path, "story_candidate_versions"),
        "event_hypotheses_v2": _count_table(path, "event_hypotheses_v2"),
        "evaluation_handoffs": _count_table(path, "evaluation_handoffs"),
        "admitted": admitted,
        "skipped": skipped,
        "graphiti_runtime_enabled": REAL_GRAPHITI_RUNTIME_ENABLED,
    }


__all__ = [
    "TRIAGE_SOURCE_IDS",
    "LeadStoryCandidateError",
    "record_first_boot_story_candidates",
]
