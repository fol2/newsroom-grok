"""Governed live-official 4B ACCEPT for first-boot News Lead extracts.

Reads the six retained 4A extraction runs and records Entity Mentions,
resolution proposals, and immutable editorial ACCEPT decisions. Canonical
Entities are created only by those ACCEPT rows. Binds Source Revision,
Discovery Representation and the same admitted representation passages the
4A runs already used. Does not remint, extract, admit 4C relations, write
Neo4j, or invent RAD-02 / UK-10 rows.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

from newsroom.authority import (
    AuthenticationProof,
    StaticAuthorizer,
    UtcTimestamp,
)
from newsroom.authority.entity_system import open_governed_entity_authority_system
from newsroom.checks import deterministic_uuid4
from newsroom.discovery_lead_admission import ID_NAMESPACE as LEAD_ID_NAMESPACE
from newsroom.discovery_lead_extraction import (
    EXTRACT_SOURCE_ID_SET,
    EXTRACT_SOURCE_IDS,
)
from newsroom.entities import (
    ENTITY_NORMALISATION_CONTRACT_DIGEST,
    CanonicalEntityId,
    CanonicalEntityVersionId,
    EntityAliasId,
    EntityAliasKind,
    EntityKind,
    EntityMentionAdmissionRequest,
    EntityMentionId,
    EntityReadPolicy,
    EntityResolutionDecisionAction,
    EntityResolutionDecisionRequest,
    EntityResolutionProposalId,
    EntityResolutionProposalKind,
    EntityResolutionProposalRequest,
    EntityResolutionProposalVersionId,
    classify_entity_script,
    normalize_entity_text,
    resolve_mention_text,
)
from newsroom.envelope_grant import OWNER_CREDENTIAL, OWNER_PRINCIPAL
from newsroom.extraction.types import ProposalEnvelopeId
from newsroom.host_store import (
    HOST_DISCOVERY_SCOPES,
    host_authenticator,
    host_authority_registries,
    host_authorizer,
)
from newsroom.sources import SourceDefinitionId

RESOLVE_ID_NAMESPACE = "newsroom.first-boot.entity-v1"
RESOLVE_SOURCE_IDS = EXTRACT_SOURCE_IDS
RESOLVE_SOURCE_ID_SET = EXTRACT_SOURCE_ID_SET
DECISION_POLICY_VERSION = "first-boot-entity-resolution-v1"
DECISION_REASON_CODE = "LIVE_OFFICIAL_ACCEPTED_MATERIAL"


class LeadEntityResolutionError(ValueError):
    """First-boot live-official entity resolution failed closed."""


def _proof() -> AuthenticationProof:
    return AuthenticationProof(method="STATIC_TOKEN", credential=OWNER_CREDENTIAL)


def _resolve_id(identifier_type, *, kind: str, semantic: object):
    return deterministic_uuid4(
        identifier_type,
        namespace=RESOLVE_ID_NAMESPACE,
        semantic_value={"kind": kind, "value": semantic},
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
    return {str(_lead_definition_id(source_id)): source_id for source_id in EXTRACT_SOURCE_IDS}


def _accepted_mention_proposals(path: Path) -> set[str]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='entity_mentions'"
        ).fetchone()
        if present is None:
            return set()
        rows = conn.execute(
            """
            SELECT m.source_proposal_id
            FROM entity_mentions AS m
            JOIN entity_mention_resolutions AS r ON r.mention_id = m.mention_id
            JOIN entity_resolution_decisions AS d ON d.decision_id = r.decision_id
            WHERE d.action = 'ACCEPT'
            """
        ).fetchall()
    finally:
        conn.close()
    return {str(row[0]) for row in rows}


def _load_mention_bindings(path: Path) -> tuple[dict[str, Any], ...]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        for table in (
            "extraction_runs",
            "extraction_outputs",
            "extraction_proposals",
            "news_leads",
        ):
            present = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if present is None:
                raise LeadEntityResolutionError(
                    "extraction_runs is absent; extract-leads first"
                    if table == "extraction_runs"
                    else f"{table} is absent; extract-leads first"
                )
        rows = conn.execute(
            """
            SELECT p.proposal_id, p.canonical_digest, p.subject_placeholder,
                   p.local_id, p.run_id, p.run_version_id,
                   ev.passage_id, ev.start_byte, ev.end_byte, ev.evidence_text_digest,
                   er.definition_id, er.item_id, er.revision_id, er.representation_id,
                   rp.language, nl.lead_id
            FROM extraction_proposals AS p
            JOIN extraction_proposal_evidence AS ev
              ON ev.proposal_id = p.proposal_id
            JOIN extraction_runs AS er ON er.run_id = p.run_id
            JOIN extraction_run_passages AS rp
              ON rp.run_id = p.run_id AND rp.passage_id = ev.passage_id
            JOIN news_leads AS nl
              ON nl.revision_id = er.revision_id
             AND nl.representation_id = er.representation_id
            WHERE p.proposal_kind = 'ENTITY_MENTION'
            ORDER BY p.proposal_id
            """
        ).fetchall()
    finally:
        conn.close()
    return tuple(dict(row) for row in rows)


def _eligible_mentions(
    path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    definitions = _definition_map()
    existing = _accepted_mention_proposals(path)
    selected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in _load_mention_bindings(path):
        source_id = definitions.get(str(row["definition_id"]))
        lead_id = str(row["lead_id"])
        revision_id = str(row["revision_id"])
        representation_id = str(row["representation_id"])
        proposal_id = str(row["proposal_id"])
        binding = {
            "source_id": source_id,
            "lead_id": lead_id,
            "item_id": str(row["item_id"]),
            "revision_id": revision_id,
            "representation_id": representation_id,
            "passage_id": str(row["passage_id"]),
            "run_id": str(row["run_id"]),
            "source_proposal_id": proposal_id,
            "local_id": str(row["local_id"]),
        }
        if source_id not in RESOLVE_SOURCE_ID_SET:
            skipped.append({**binding, "reason": "source-not-in-4b-allowlist"})
            continue
        if proposal_id in existing:
            skipped.append({**binding, "reason": "already-resolved"})
            continue
        mention_text = resolve_mention_text(
            str(row["subject_placeholder"]),
            start_byte=int(row["start_byte"]),
            end_byte=int(row["end_byte"]),
            evidence_text_digest=str(row["evidence_text_digest"]),
        )
        selected.append(
            {
                **binding,
                "canonical_digest": str(row["canonical_digest"]),
                "language": str(row["language"]),
                "mention_text": mention_text,
            }
        )
    selected.sort(
        key=lambda item: (
            RESOLVE_SOURCE_IDS.index(item["source_id"]),
            item["local_id"],
        )
    )
    return selected, skipped


def _entity_authorizer() -> StaticAuthorizer:
    grants = dict(host_authorizer()._grants_by_principal)
    owner = set(grants[OWNER_PRINCIPAL])
    owner.update(HOST_DISCOVERY_SCOPES)
    owner.update(
        {
            "authority.extraction.read",
            "authority.extraction.read_proposals",
            "authority.extraction.read_raw",
            "authority.entity.mention",
            "authority.entity.propose",
            "authority.entity.decide",
            "authority.entity.read_proposals",
            "authority.entity.read_admitted",
            "authority.entity.read_projection",
            "authority.host.read",
        }
    )
    grants[OWNER_PRINCIPAL] = frozenset(owner)
    return StaticAuthorizer(
        policy_version="host-store-entity-v1",
        grants_by_principal=grants,
    )


def _entity_read_policy() -> EntityReadPolicy:
    return EntityReadPolicy(
        policy_id="first-boot-live-official-entity-read-v1",
        purpose="entity.live-official.audit",
        proposal_required_scope="authority.entity.read_proposals",
        admitted_required_scope="authority.entity.read_admitted",
        projection_required_scope="authority.entity.read_projection",
        allowed_principal_ids=frozenset({OWNER_PRINCIPAL}),
        max_results=100,
    )


def _open_entity_system(path: Path, clock):
    registry, schemas = host_authority_registries()
    return open_governed_entity_authority_system(
        path=path,
        registry=registry,
        payload_schemas=schemas,
        authenticator=host_authenticator(),
        authorizer=_entity_authorizer(),
        read_policy=_entity_read_policy(),
        clock=clock,
    )


def _accept_mention(system, *, item: dict[str, Any], proof: AuthenticationProof) -> dict[str, Any]:
    source_proposal_id = ProposalEnvelopeId.parse(item["source_proposal_id"])
    mention_id = _resolve_id(
        EntityMentionId, kind="mention", semantic=item["source_proposal_id"]
    )
    proposal_id = _resolve_id(
        EntityResolutionProposalId,
        kind="resolution-proposal",
        semantic=item["source_proposal_id"],
    )
    proposal_version_id = _resolve_id(
        EntityResolutionProposalVersionId,
        kind="resolution-proposal-version",
        semantic=item["source_proposal_id"],
    )
    entity_id = _resolve_id(
        CanonicalEntityId, kind="canonical-entity", semantic=item["source_proposal_id"]
    )
    entity_version_id = _resolve_id(
        CanonicalEntityVersionId,
        kind="canonical-entity-version",
        semantic=item["source_proposal_id"],
    )
    alias_id = _resolve_id(
        EntityAliasId, kind="alias", semantic=item["source_proposal_id"]
    )
    mention = system.entities.admit_mention(
        EntityMentionAdmissionRequest(
            mention_id=mention_id,
            source_proposal_id=source_proposal_id,
            expected_source_proposal_digest=item["canonical_digest"],
            entity_kind=EntityKind.UNKNOWN,
            language=item["language"],
            script=classify_entity_script(item["mention_text"]),
            normalized_text=normalize_entity_text(item["mention_text"]),
            normalization_contract_digest=ENTITY_NORMALISATION_CONTRACT_DIGEST,
            idempotency_key=f"first-boot-4b-mention-{item['source_proposal_id']}",
        ),
        proof=proof,
    )
    proposal = system.entities.propose_resolution(
        EntityResolutionProposalRequest(
            proposal_id=proposal_id,
            proposal_version_id=proposal_version_id,
            version_number=1,
            expected_previous_version_id=None,
            source_proposal_id=source_proposal_id,
            expected_source_proposal_digest=item["canonical_digest"],
            kind=EntityResolutionProposalKind.MENTION_TO_NEW_ENTITY,
            subject_mention_id=mention.mention_id,
            object_mention_id=None,
            candidate_entity_id=None,
            candidate_entity_version_id=None,
            confidence_basis_points=None,
            uncertainty_codes=(),
            basis_codes=("BOUND_REPRESENTATION_SPAN",),
            idempotency_key=f"first-boot-4b-propose-{item['source_proposal_id']}",
        ),
        proof=proof,
    )
    decision = system.entities.decide_resolution(
        EntityResolutionDecisionRequest(
            proposal_id=proposal.proposal_id,
            expected_proposal_version_id=proposal.proposal_version_id,
            expected_proposal_digest=proposal.canonical_digest,
            action=EntityResolutionDecisionAction.ACCEPT,
            expected_decision_version=0,
            expected_previous_decision_id=None,
            accepted_entity_id=entity_id,
            accepted_entity_version_id=entity_version_id,
            alias_id=alias_id,
            alias_kind=EntityAliasKind.PRIMARY_NAME,
            reason_code=DECISION_REASON_CODE,
            decision_policy_version=DECISION_POLICY_VERSION,
            idempotency_key=f"first-boot-4b-accept-{item['source_proposal_id']}",
        ),
        proof=proof,
    )
    return {
        "source_id": item["source_id"],
        "lead_id": item["lead_id"],
        "run_id": item["run_id"],
        "revision_id": item["revision_id"],
        "representation_id": item["representation_id"],
        "item_id": item["item_id"],
        "passage_id": item["passage_id"],
        "source_proposal_id": item["source_proposal_id"],
        "local_id": item["local_id"],
        "mention_id": str(mention.mention_id),
        "decision_id": str(decision.decision_id),
        "entity_id": str(decision.accepted_entity_id),
        "action": decision.action.value,
        "replayed": mention.replayed and proposal.replayed and decision.replayed,
    }


def record_first_boot_entity_resolution(
    path: Path,
    *,
    clock: Callable[[], UtcTimestamp] | None = None,
) -> dict[str, Any]:
    from newsroom.graphiti_adapter.models import REAL_GRAPHITI_RUNTIME_ENABLED

    if REAL_GRAPHITI_RUNTIME_ENABLED:
        raise LeadEntityResolutionError("REAL_GRAPHITI_RUNTIME_ENABLED must stay False")
    selected, skipped = _eligible_mentions(path)
    if not selected and not skipped:
        raise LeadEntityResolutionError(
            "no retained live-official extracts in the 4B allowlist"
        )
    resolved: list[dict[str, Any]] = []
    if selected:
        proof = _proof()
        now = clock or UtcTimestamp.now
        selected_clock = now if callable(now) else (lambda: now)
        system = _open_entity_system(path, selected_clock)
        try:
            for item in selected:
                resolved.append(_accept_mention(system, item=item, proof=proof))
        finally:
            system.close()
        failed = [item for item in resolved if item["action"] != "ACCEPT"]
        if failed:
            raise LeadEntityResolutionError(
                "live-official entity resolution did not ACCEPT every allowlisted mention"
            )
    return {
        "ok": True,
        "schema_version": _schema_version(path),
        "auto_publish": False,
        "discord": False,
        "public_adapter": False,
        "x_as_publisher": False,
        "graphiti": False,
        "news_leads": _count_table(path, "news_leads"),
        "extraction_runs": _count_table(path, "extraction_runs"),
        "extraction_outputs": _count_table(path, "extraction_outputs"),
        "entity_mentions": _count_table(path, "entity_mentions"),
        "canonical_entities": _count_table(path, "canonical_entities"),
        "entity_resolution_decisions": _count_table(
            path, "entity_resolution_decisions"
        ),
        "editorial_relation_decisions": _count_table(
            path, "editorial_relation_decisions"
        ),
        "resolved": resolved,
        "skipped": skipped,
        "graphiti_runtime_enabled": REAL_GRAPHITI_RUNTIME_ENABLED,
    }


__all__ = [
    "RESOLVE_SOURCE_IDS",
    "LeadEntityResolutionError",
    "record_first_boot_entity_resolution",
]
