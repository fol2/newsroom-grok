"""Governed live-official original 粵語 write from Evidence Packages.

Reads the six existing Evidence Packages and records original Cantonese
writes bound to Source Revision, Discovery Representation, admitted
passages, 4B ACCEPT Canonical Entities, the 4C ACCEPT Relation
Assertion, the Story Candidate, and the Evidence Package. Writes already-
admitted material only. No invented evidence. A write grants no
publication authority.

Does not remint seq 15 / #71, extract, resolve, relate, triage, package,
write Neo4j, extend the projector, invent RAD-02 / UK-10, AUTO_PUBLISH,
or run a model.
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
from newsroom.discovery_lead_evidence_packages import EVIDENCE_SOURCE_IDS

WRITE_ID_NAMESPACE = "newsroom.first-boot.original-write-v1"
WRITE_SOURCE_IDS = EVIDENCE_SOURCE_IDS
WRITE_SOURCE_ID_SET = frozenset(WRITE_SOURCE_IDS)
WRITE_SCHEMA = "newsroom.first-boot.original-write.v1"
WRITE_LANGUAGE = "yue"
MIN_READABLE_BODY_CHARS = 40
CANTONESE_MARKERS = ("呢篇", "嘅", "唔係")


class LeadOriginalWriteError(ValueError):
    """First-boot live-official original 粵語 write failed closed."""


def _write_id(*, kind: str, semantic: object) -> str:
    return str(
        deterministic_uuid4(
            UUIDv4Id,
            namespace=WRITE_ID_NAMESPACE,
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
            raise LeadOriginalWriteError(f"{table} is absent; {missing}")


def _apply_checked_schema(path: Path) -> None:
    from newsroom.host_store import open_host_store

    open_host_store(path).close()


def _existing_write_packages(path: Path) -> set[str]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='original_write_heads'"
        ).fetchone()
        if present is None:
            return set()
        rows = conn.execute("SELECT package_id FROM original_write_heads").fetchall()
    finally:
        conn.close()
    return {str(row[0]) for row in rows if row[0]}


def _package_payload(raw: object) -> dict[str, Any]:
    if isinstance(raw, memoryview):
        raw = raw.tobytes()
    payload = json.loads(bytes(raw).decode("utf-8"))
    if not isinstance(payload, dict):
        raise LeadOriginalWriteError("Evidence Package bytes must be an object")
    return payload


def _load_package_rows(path: Path) -> tuple[dict[str, Any], ...]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        _require_tables(
            conn,
            (
                "news_leads",
                "story_candidates",
                "story_candidate_heads",
                "evidence_packages",
                "evidence_package_heads",
                "extraction_runs",
                "extraction_run_passages",
                "canonical_entities",
                "editorial_relation_assertions",
            ),
            missing="evidence-leads first",
        )
        rows = conn.execute(
            """
            SELECT h.package_id, h.package_bytes,
                   nl.lead_id, nl.signal_id, nl.item_id, nl.revision_id,
                   nl.representation_id, nl.authority_event_id
            FROM evidence_package_heads AS h
            JOIN news_leads AS nl
              ON nl.lead_id = json_extract(CAST(h.package_bytes AS TEXT), '$.lead_id')
            ORDER BY h.package_id
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
        raise LeadOriginalWriteError(
            "original write requires the two 4B ACCEPT Canonical Entities"
        )
    for item in items:
        if str(item["action"]) != "ACCEPT" or str(item["lifecycle"]) != "ACTIVE":
            raise LeadOriginalWriteError(
                "original write requires current 4B ACCEPT Canonical Entities"
            )
        if str(item["accepted_entity_version_id"]) != str(item["current_entity_version_id"]):
            raise LeadOriginalWriteError(
                "original write requires the current Entity Version"
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
        raise LeadOriginalWriteError(
            "original write requires a 4C ACCEPT Relation Assertion"
        )
    return dict(row)


def _mention_surface(text: object) -> str:
    return str(text or "").strip().strip('"').strip()


def _compose_original_yue(
    *,
    source_id: str,
    package_id: str,
    mention_texts: tuple[str, str],
    predicate: str,
) -> tuple[str, str]:
    source_mention, item_mention = mention_texts
    if source_mention != source_id:
        source_mention, item_mention = item_mention, source_mention
    if source_mention != source_id:
        source_mention = source_id
        item_mention = mention_texts[0] if mention_texts[0] != source_id else mention_texts[1]
    title = f"{source_id} 原創粵語稿"
    body = (
        f"呢篇係根據已接納證據包寫嘅原創粵語稿，唔係官方原文再鑄，"
        f"亦唔係第15號帳本事件再鑄。"
        f"來源 {source_mention} 同已接納稿件 {item_mention} 屬同一工序（{predicate}）。"
        f"證據包 {package_id} 只授權呢次原稿寫作，唔授權公開發表，亦冇發明證據。"
        f"自動公開同公開適配器一律關閉。"
    )
    if len(body) < MIN_READABLE_BODY_CHARS:
        raise LeadOriginalWriteError("original 粵語 body is shorter than the readable floor")
    if any(marker not in body for marker in CANTONESE_MARKERS):
        raise LeadOriginalWriteError("original write must be 粵語")
    if source_mention not in body or item_mention not in body:
        raise LeadOriginalWriteError("original write must bind admitted mention text")
    return title, body


def _eligible_packages(
    path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    existing = _existing_write_packages(path)
    selected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in _load_package_rows(path):
        payload = _package_payload(row["package_bytes"])
        source_id = str(payload.get("source_id") or "")
        package_id = str(row["package_id"])
        candidate_id = str(payload.get("candidate_id") or "")
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
            "package_id": package_id,
            "item_id": item_id,
            "revision_id": revision_id,
            "representation_id": representation_id,
            "passage_id": passage_id,
            "run_id": run_id,
        }
        if source_id not in WRITE_SOURCE_ID_SET:
            skipped.append({**binding, "reason": "source-not-in-original-write-allowlist"})
            continue
        if package_id in existing:
            skipped.append({**binding, "reason": "already-written"})
            continue
        if str(payload.get("package_id")) != package_id:
            raise LeadOriginalWriteError("original write must bind the Evidence Package head")
        if str(payload.get("lead_id")) != lead_id:
            raise LeadOriginalWriteError("original write must bind the Evidence Package lead")
        if str(payload.get("revision_id")) != revision_id:
            raise LeadOriginalWriteError("Evidence Package must bind the Source Revision")
        if str(payload.get("representation_id")) != representation_id:
            raise LeadOriginalWriteError(
                "Evidence Package must bind the Discovery Representation"
            )
        if str(payload.get("item_id")) != item_id:
            raise LeadOriginalWriteError("Evidence Package item_id must match the lead")
        if revision_id == item_id or representation_id == item_id or passage_id == item_id:
            raise LeadOriginalWriteError("JSON identity is not a passage")
        if payload.get("authorises_publication") not in {False, 0}:
            raise LeadOriginalWriteError("Evidence Package must not authorise publication")
        if payload.get("invented_evidence") not in {False, 0}:
            raise LeadOriginalWriteError("Evidence Package must not invent evidence")
        entities = _load_accepted_entities(path, run_id)
        assertion = _load_accepted_assertion(path, run_id)
        entity_ids = [str(entity["accepted_entity_id"]) for entity in entities]
        if list(payload.get("canonical_entity_ids") or []) != entity_ids:
            raise LeadOriginalWriteError(
                "original write must bind the Evidence Package 4B ACCEPT entities"
            )
        if str(payload.get("relation_assertion_id")) != str(assertion["assertion_id"]):
            raise LeadOriginalWriteError(
                "original write must bind the Evidence Package 4C ACCEPT assertion"
            )
        for entity in entities:
            if str(entity["revision_id"]) != revision_id:
                raise LeadOriginalWriteError("4B mention must bind the Source Revision")
            if str(entity["representation_id"]) != representation_id:
                raise LeadOriginalWriteError(
                    "4B mention must bind the Discovery Representation"
                )
            if str(entity["passage_id"]) != passage_id:
                raise LeadOriginalWriteError(
                    "4B mention must bind the admitted representation passage"
                )
            if str(entity["item_id"]) in {passage_id, revision_id, representation_id}:
                raise LeadOriginalWriteError("JSON identity is not a passage")
        if str(assertion["passage_id"]) != passage_id:
            raise LeadOriginalWriteError(
                "4C assertion must bind the admitted representation passage"
            )
        mention_texts = (
            _mention_surface(entities[0]["mention_text"]),
            _mention_surface(entities[1]["mention_text"]),
        )
        if not all(mention_texts):
            raise LeadOriginalWriteError("original write requires admitted mention text")
        title, body = _compose_original_yue(
            source_id=source_id,
            package_id=package_id,
            mention_texts=mention_texts,
            predicate=str(assertion["predicate"]),
        )
        selected.append(
            {
                **binding,
                "signal_id": str(row["signal_id"]),
                "canonical_entity_ids": entity_ids,
                "canonical_entity_version_ids": [
                    str(entity["accepted_entity_version_id"]) for entity in entities
                ],
                "mention_texts": list(mention_texts),
                "relation_assertion_id": str(assertion["assertion_id"]),
                "relation_predicate": str(assertion["predicate"]),
                "story_title": title,
                "story_body": body,
                "lead_event_id": str(row["authority_event_id"]),
            }
        )
    selected.sort(
        key=lambda item: (
            WRITE_SOURCE_IDS.index(item["source_id"]),
            item["package_id"],
        )
    )
    return selected, skipped


def _write_value(item: dict[str, Any], *, write_id: str) -> dict[str, object]:
    return {
        "schema": WRITE_SCHEMA,
        "write_id": write_id,
        "package_id": item["package_id"],
        "source_id": item["source_id"],
        "lead_id": item["lead_id"],
        "candidate_id": item["candidate_id"],
        "signal_id": item["signal_id"],
        "revision_id": item["revision_id"],
        "representation_id": item["representation_id"],
        "item_id": item["item_id"],
        "passage_id": item["passage_id"],
        "run_id": item["run_id"],
        "canonical_entity_ids": item["canonical_entity_ids"],
        "canonical_entity_version_ids": item["canonical_entity_version_ids"],
        "mention_texts": item["mention_texts"],
        "relation_assertion_id": item["relation_assertion_id"],
        "relation_predicate": item["relation_predicate"],
        "language": WRITE_LANGUAGE,
        "story_title": item["story_title"],
        "story_body": item["story_body"],
        "authorises_publication": False,
        "authorises_evidence": False,
        "authorises_egress": False,
        "auto_publish": False,
        "invented_evidence": False,
        "publication_decision": False,
        "remint": False,
    }


def _admit_write(
    conn: sqlite3.Connection,
    *,
    item: dict[str, Any],
    recorded_at: str,
) -> dict[str, Any]:
    write_id = _write_id(kind="write", semantic=item["package_id"])
    write_value = _write_value(item, write_id=write_id)
    write_bytes = canonical_json_bytes(write_value)
    semantic_digest = digest_bytes(
        canonical_json_bytes(
            {
                "package_id": item["package_id"],
                "candidate_id": item["candidate_id"],
                "revision_id": item["revision_id"],
                "representation_id": item["representation_id"],
                "passage_id": item["passage_id"],
                "lead_id": item["lead_id"],
                "language": WRITE_LANGUAGE,
            }
        )
    )
    receipt_digest = digest_bytes(write_bytes)
    conn.execute(
        "INSERT INTO original_writes(write_id,package_id,semantic_digest,created_at) "
        "VALUES(?,?,?,?)",
        (write_id, item["package_id"], semantic_digest, recorded_at),
    )
    conn.execute(
        "INSERT INTO original_write_receipts("
        "receipt_digest,write_id,package_id,write_bytes,recorded_at) "
        "VALUES(?,?,?,?,?)",
        (receipt_digest, write_id, item["package_id"], write_bytes, recorded_at),
    )
    conn.execute(
        "INSERT INTO original_write_heads("
        "write_id,package_id,write_bytes,semantic_digest,updated_at) "
        "VALUES(?,?,?,?,?)",
        (write_id, item["package_id"], write_bytes, semantic_digest, recorded_at),
    )
    return {
        "source_id": item["source_id"],
        "lead_id": item["lead_id"],
        "candidate_id": item["candidate_id"],
        "package_id": item["package_id"],
        "write_id": write_id,
        "revision_id": item["revision_id"],
        "representation_id": item["representation_id"],
        "item_id": item["item_id"],
        "passage_id": item["passage_id"],
        "run_id": item["run_id"],
        "language": WRITE_LANGUAGE,
        "story_title": item["story_title"],
        "story_body": item["story_body"],
        "canonical_entity_ids": item["canonical_entity_ids"],
        "relation_assertion_id": item["relation_assertion_id"],
        "authorises_publication": False,
        "authorises_evidence": False,
        "invented_evidence": False,
        "publication_decision": False,
        "remint": False,
        "replayed": False,
    }


def record_first_boot_original_writes(
    path: Path,
    *,
    clock: Callable[[], UtcTimestamp] | None = None,
) -> dict[str, Any]:
    from newsroom.graphiti_adapter.models import REAL_GRAPHITI_RUNTIME_ENABLED

    if REAL_GRAPHITI_RUNTIME_ENABLED:
        raise LeadOriginalWriteError("REAL_GRAPHITI_RUNTIME_ENABLED must stay False")
    _apply_checked_schema(path)
    selected, skipped = _eligible_packages(path)
    if not selected and not skipped:
        raise LeadOriginalWriteError(
            "no retained live-official Evidence Packages in the original-write allowlist"
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
                    "original_writes",
                    "original_write_receipts",
                    "original_write_heads",
                ),
                missing="checked v36 original write migration first",
            )
            for item in selected:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    admitted.append(
                        _admit_write(connection, item=item, recorded_at=recorded_at)
                    )
                    connection.execute("COMMIT")
                except sqlite3.Error as exc:
                    if connection.in_transaction:
                        connection.execute("ROLLBACK")
                    raise LeadOriginalWriteError(str(exc)) from exc
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
        "original_writes": _count_table(path, "original_writes"),
        "original_write_heads": _count_table(path, "original_write_heads"),
        "original_write_receipts": _count_table(path, "original_write_receipts"),
        "event_hypotheses_v2": _count_table(path, "event_hypotheses_v2"),
        "evaluation_handoffs": _count_table(path, "evaluation_handoffs"),
        "admitted": admitted,
        "skipped": skipped,
        "graphiti_runtime_enabled": REAL_GRAPHITI_RUNTIME_ENABLED,
    }


__all__ = [
    "WRITE_SOURCE_IDS",
    "LeadOriginalWriteError",
    "record_first_boot_original_writes",
]
