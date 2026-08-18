"""Governed live-official 4C ACCEPT for first-boot resolved extracts.

Reads the six retained 4A RELATION envelopes and twelve 4B ACCEPT Canonical
Entities, records immutable Relation Proposals, and admits them with explicit
ACCEPT decisions. Endpoints are the current Entity Versions of those Canonical
Entities, not extractor names. Binds Source Revision, Discovery Representation
and the same admitted representation passages the 4A runs already used.

Does not remint, extract, resolve, write Neo4j, invent Canonical Entities, or
admit RAD-02 / UK-10.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

from newsroom.authority import AuthenticationProof, StaticAuthorizer, UtcTimestamp
from newsroom.authority.editorial_relation_system import (
    open_governed_editorial_relation_authority_system,
)
from newsroom.authority.entity_system import open_governed_entity_authority_system
from newsroom.checks import deterministic_uuid4
from newsroom.discovery_lead_admission import ID_NAMESPACE as LEAD_ID_NAMESPACE
from newsroom.discovery_lead_extraction import (
    EXTRACT_SOURCE_ID_SET,
    EXTRACT_SOURCE_IDS,
)
from newsroom.entities import (
    CanonicalEntityId,
    CanonicalEntityVersionId,
    EntityReadPolicy,
    EntityResolutionDependencyId,
    EntityResolutionDependencyRequest,
    EntityResolutionProposalId,
    EntityResolutionProposalVersionId,
)
from newsroom.envelope_grant import OWNER_CREDENTIAL, OWNER_PRINCIPAL
from newsroom.extraction.types import (
    ExtractionOutputId,
    ExtractionPassageId,
    ExtractionRunId,
    ExtractionRunVersionId,
    ProposalEnvelopeId,
)
from newsroom.host_store import (
    HOST_DISCOVERY_SCOPES,
    host_authenticator,
    host_authority_registries,
    host_authorizer,
)
from newsroom.relations.editorial_models import (
    EDITORIAL_PREDICATE_REGISTRY_V1,
    EDITORIAL_RELATION_ADMISSION_POLICY_VERSION,
    CanonicalEntityRelationEndpoint,
    EditorialRelationDecisionRequest,
    EditorialRelationProducer,
    EditorialRelationProposalRequest,
    EditorialRelationReadPolicy,
    EditorialRelationTemporalScope,
    ExtractionRelationEvidence,
    endpoint_canonical_bytes,
)
from newsroom.relations.editorial_types import (
    EditorialPredicateCode,
    EditorialRelationAssertionId,
    EditorialRelationDecisionAction,
    EditorialRelationDecisionId,
    EditorialRelationProducerKind,
    EditorialRelationProposalId,
    EditorialRelationProposalVersionId,
)
from newsroom.sources import SourceDefinitionId

RELATE_ID_NAMESPACE = "newsroom.first-boot.relation-v1"
RELATE_SOURCE_IDS = EXTRACT_SOURCE_IDS
RELATE_SOURCE_ID_SET = EXTRACT_SOURCE_ID_SET
DECISION_REASON_CODE = "LIVE_OFFICIAL_ACCEPTED_MATERIAL"
PRODUCER_ID = "first-boot.live-official"
PRODUCER_VERSION = "first-boot-live-official-4c-v1"
RELATION_LOCAL_ID = "relation.source-about-item"
MENTION_LOCAL_IDS = frozenset({"entity.item", "entity.source"})


class LeadEditorialRelationError(ValueError):
    """First-boot live-official editorial relation admission failed closed."""


def _proof() -> AuthenticationProof:
    return AuthenticationProof(method="STATIC_TOKEN", credential=OWNER_CREDENTIAL)


def _relate_id(identifier_type, *, kind: str, semantic: object):
    return deterministic_uuid4(
        identifier_type,
        namespace=RELATE_ID_NAMESPACE,
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


def _accepted_relation_proposals(path: Path) -> set[str]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='editorial_relation_extraction_evidence'"
        ).fetchone()
        if present is None:
            return set()
        rows = conn.execute(
            """
            SELECT DISTINCT e.source_proposal_id
            FROM editorial_relation_extraction_evidence AS e
            JOIN editorial_relation_proposal_versions AS v
              ON v.proposal_version_id = e.proposal_version_id
            JOIN editorial_relation_decisions AS d
              ON d.proposal_id = v.proposal_id
            WHERE d.action = 'ACCEPT'
            """
        ).fetchall()
    finally:
        conn.close()
    return {str(row[0]) for row in rows}


def _require_tables(conn: sqlite3.Connection, names: tuple[str, ...], *, missing: str) -> None:
    for table in names:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if present is None:
            raise LeadEditorialRelationError(f"{table} is absent; {missing}")


def _load_relation_rows(path: Path) -> tuple[dict[str, Any], ...]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        _require_tables(
            conn,
            (
                "extraction_runs",
                "extraction_outputs",
                "extraction_proposals",
                "news_leads",
            ),
            missing="extract-leads first",
        )
        _require_tables(
            conn,
            (
                "entity_mentions",
                "canonical_entities",
                "entity_resolution_decisions",
            ),
            missing="resolve-leads first",
        )
        rows = conn.execute(
            """
            SELECT p.proposal_id, p.canonical_digest, p.local_id, p.run_id,
                   p.run_version_id, p.output_id, p.producer_contract_digest,
                   p.confidence_basis_points,
                   er.definition_id, er.item_id, er.revision_id, er.representation_id,
                   nl.lead_id
            FROM extraction_proposals AS p
            JOIN extraction_runs AS er ON er.run_id = p.run_id
            JOIN news_leads AS nl
              ON nl.revision_id = er.revision_id
             AND nl.representation_id = er.representation_id
            WHERE p.proposal_kind = 'RELATION'
              AND p.local_id = ?
            ORDER BY p.proposal_id
            """,
            (RELATION_LOCAL_ID,),
        ).fetchall()
    finally:
        conn.close()
    return tuple(dict(row) for row in rows)


def _load_evidence(path: Path, proposal_id: str) -> tuple[dict[str, Any], ...]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT evidence_ordinal, passage_id, start_byte, end_byte,
                   evidence_text_digest
            FROM extraction_proposal_evidence
            WHERE proposal_id = ?
            ORDER BY evidence_ordinal
            """,
            (proposal_id,),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        raise LeadEditorialRelationError(
            "relation extraction evidence is absent from the bound passage"
        )
    return tuple(dict(row) for row in rows)


def _load_accepted_endpoints(path: Path, run_id: str) -> dict[str, dict[str, Any]]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT p.local_id, p.proposal_id AS mention_proposal_id,
                   m.mention_id, m.mention_text, m.normalized_text,
                   m.passage_id, m.revision_id, m.representation_id, m.item_id,
                   rp.resolution_proposal_id, rpv.proposal_version_id,
                   rpv.canonical_digest AS resolution_digest,
                   d.accepted_entity_id, d.accepted_entity_version_id, d.action,
                   h.current_entity_version_id, h.lifecycle
            FROM extraction_proposals AS p
            JOIN entity_mentions AS m ON m.source_proposal_id = p.proposal_id
            JOIN entity_resolution_proposals AS rp
              ON rp.subject_mention_id = m.mention_id
            JOIN entity_resolution_proposal_heads AS ph
              ON ph.resolution_proposal_id = rp.resolution_proposal_id
            JOIN entity_resolution_proposal_versions AS rpv
              ON rpv.proposal_version_id = ph.current_proposal_version_id
            JOIN entity_resolution_decision_heads AS dh
              ON dh.resolution_proposal_id = rp.resolution_proposal_id
            JOIN entity_resolution_decisions AS d
              ON d.decision_id = dh.current_decision_id
            JOIN canonical_entity_heads AS h
              ON h.entity_id = d.accepted_entity_id
            WHERE p.run_id = ?
              AND p.proposal_kind = 'ENTITY_MENTION'
            ORDER BY p.local_id
            """,
            (run_id,),
        ).fetchall()
    finally:
        conn.close()
    by_local = {str(row["local_id"]): dict(row) for row in rows}
    if frozenset(by_local) != MENTION_LOCAL_IDS:
        raise LeadEditorialRelationError(
            "relation endpoints require the two 4B ACCEPT Canonical Entities"
        )
    for item in by_local.values():
        if str(item["action"]) != "ACCEPT":
            raise LeadEditorialRelationError(
                "relation endpoints require current 4B ACCEPT decisions"
            )
        if str(item["lifecycle"]) != "ACTIVE":
            raise LeadEditorialRelationError(
                "relation endpoints require an active Canonical Entity"
            )
        if not item["current_entity_version_id"]:
            raise LeadEditorialRelationError(
                "relation endpoints require the current Entity Version"
            )
        if str(item["accepted_entity_id"]) in {
            str(item["mention_text"]),
            str(item["normalized_text"]),
            str(item["mention_text"]).strip('"'),
        }:
            raise LeadEditorialRelationError(
                "Canonical Entity identity cannot be an extractor name"
            )
    return by_local


def _ordered_endpoints(
    endpoints: dict[str, dict[str, Any]],
) -> tuple[CanonicalEntityRelationEndpoint, CanonicalEntityRelationEndpoint]:
    bound = tuple(
        CanonicalEntityRelationEndpoint(
            entity_id=CanonicalEntityId.parse(item["accepted_entity_id"]),
            entity_version_id=CanonicalEntityVersionId.parse(
                item["current_entity_version_id"]
            ),
        )
        for item in (endpoints["entity.source"], endpoints["entity.item"])
    )
    if endpoint_canonical_bytes(bound[0]) <= endpoint_canonical_bytes(bound[1]):
        return bound[0], bound[1]
    return bound[1], bound[0]


def _eligible_relations(
    path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    definitions = _definition_map()
    existing = _accepted_relation_proposals(path)
    selected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in _load_relation_rows(path):
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
            "run_id": str(row["run_id"]),
            "source_proposal_id": proposal_id,
            "local_id": str(row["local_id"]),
        }
        if source_id not in RELATE_SOURCE_ID_SET:
            skipped.append({**binding, "reason": "source-not-in-4c-allowlist"})
            continue
        if proposal_id in existing:
            skipped.append({**binding, "reason": "already-related"})
            continue
        endpoints = _load_accepted_endpoints(path, str(row["run_id"]))
        subject, obj = _ordered_endpoints(endpoints)
        evidence_rows = _load_evidence(path, proposal_id)
        selected.append(
            {
                **binding,
                "canonical_digest": str(row["canonical_digest"]),
                "run_version_id": str(row["run_version_id"]),
                "output_id": str(row["output_id"]),
                "producer_contract_digest": str(row["producer_contract_digest"]),
                "confidence_basis_points": int(row["confidence_basis_points"]),
                "subject": subject,
                "object": obj,
                "endpoints": endpoints,
                "evidence_rows": evidence_rows,
                "passage_id": str(evidence_rows[0]["passage_id"]),
            }
        )
    selected.sort(
        key=lambda item: (
            RELATE_SOURCE_IDS.index(item["source_id"]),
            item["source_proposal_id"],
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
            "authority.entity.dependency",
            "authority.entity.read_proposals",
            "authority.entity.read_admitted",
            "authority.entity.read_projection",
            "authority.host.read",
        }
    )
    grants[OWNER_PRINCIPAL] = frozenset(owner)
    return StaticAuthorizer(
        policy_version="host-store-entity-relation-v1",
        grants_by_principal=grants,
    )


def _relation_authorizer() -> StaticAuthorizer:
    grants = dict(host_authorizer()._grants_by_principal)
    owner = set(grants[OWNER_PRINCIPAL])
    owner.update(HOST_DISCOVERY_SCOPES)
    owner.update(
        {
            "authority.extraction.read",
            "authority.extraction.read_proposals",
            "authority.extraction.read_raw",
            "authority.entity.read_proposals",
            "authority.entity.read_admitted",
            "authority.relation.propose",
            "authority.relation.decide",
            "authority.relation.read_proposals",
            "authority.relation.read_admitted",
            "authority.relation.read_projection",
            "authority.host.read",
        }
    )
    grants[OWNER_PRINCIPAL] = frozenset(owner)
    return StaticAuthorizer(
        policy_version="host-store-editorial-relation-v1",
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


def _relation_read_policy() -> EditorialRelationReadPolicy:
    return EditorialRelationReadPolicy(
        policy_id="first-boot-live-official-relation-read-v1",
        purpose="editorial.relation.live-official.audit",
        proposal_required_scope="authority.relation.read_proposals",
        admitted_required_scope="authority.relation.read_admitted",
        projection_required_scope="authority.relation.read_projection",
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


def _open_relation_system(path: Path, clock):
    registry, schemas = host_authority_registries()
    return open_governed_editorial_relation_authority_system(
        path=path,
        registry=registry,
        payload_schemas=schemas,
        authenticator=host_authenticator(),
        authorizer=_relation_authorizer(),
        read_policy=_relation_read_policy(),
        clock=clock,
    )


def _bind_dependencies(system, *, item: dict[str, Any], proof: AuthenticationProof) -> None:
    dependent_id = ProposalEnvelopeId.parse(item["source_proposal_id"])
    bound: list[EntityResolutionDependencyId] = []
    for local_id, endpoint in item["endpoints"].items():
        dependency_id = _relate_id(
            EntityResolutionDependencyId,
            kind="resolution-dependency",
            semantic={
                "source_proposal_id": item["source_proposal_id"],
                "local_id": local_id,
            },
        )
        dependency = system.entities.bind_resolution_dependency(
            EntityResolutionDependencyRequest(
                dependency_id=dependency_id,
                dependent_proposal_id=dependent_id,
                expected_dependent_proposal_digest=item["canonical_digest"],
                resolution_proposal_id=EntityResolutionProposalId.parse(
                    endpoint["resolution_proposal_id"]
                ),
                expected_resolution_proposal_version_id=(
                    EntityResolutionProposalVersionId.parse(
                        endpoint["proposal_version_id"]
                    )
                ),
                expected_resolution_proposal_digest=endpoint["resolution_digest"],
                material=True,
                idempotency_key=(
                    f"first-boot-4c-dependency-{item['source_proposal_id']}-{local_id}"
                ),
            ),
            proof=proof,
        )
        bound.append(dependency.dependency_id)
    item["dependency_ids"] = tuple(sorted(bound, key=str))


def _relation_evidence(item: dict[str, Any]) -> tuple[ExtractionRelationEvidence, ...]:
    source_proposal_id = ProposalEnvelopeId.parse(item["source_proposal_id"])
    run_id = ExtractionRunId.parse(item["run_id"])
    run_version_id = ExtractionRunVersionId.parse(item["run_version_id"])
    output_id = ExtractionOutputId.parse(item["output_id"])
    evidence = []
    for row in item["evidence_rows"]:
        evidence.append(
            ExtractionRelationEvidence(
                source_proposal_id=source_proposal_id,
                source_proposal_digest=item["canonical_digest"],
                run_id=run_id,
                run_version_id=run_version_id,
                output_id=output_id,
                passage_id=ExtractionPassageId.parse(row["passage_id"]),
                source_evidence_ordinal=int(row["evidence_ordinal"]) - 1,
                start_byte=int(row["start_byte"]),
                end_byte=int(row["end_byte"]),
                evidence_text_digest=str(row["evidence_text_digest"]),
            )
        )
    return tuple(sorted(evidence, key=lambda row: row.canonical_bytes))


def _accept_relation(
    system,
    *,
    item: dict[str, Any],
    proof: AuthenticationProof,
    observed_at: UtcTimestamp,
) -> dict[str, Any]:
    predicate = EditorialPredicateCode.SAME_PROCESS_AS
    contract = EDITORIAL_PREDICATE_REGISTRY_V1.contract(predicate)
    proposal_id = _relate_id(
        EditorialRelationProposalId,
        kind="relation-proposal",
        semantic=item["source_proposal_id"],
    )
    proposal_version_id = _relate_id(
        EditorialRelationProposalVersionId,
        kind="relation-proposal-version",
        semantic=item["source_proposal_id"],
    )
    decision_id = _relate_id(
        EditorialRelationDecisionId,
        kind="relation-decision",
        semantic=item["source_proposal_id"],
    )
    assertion_id = _relate_id(
        EditorialRelationAssertionId,
        kind="relation-assertion",
        semantic=item["source_proposal_id"],
    )
    proposal = system.relations.propose(
        EditorialRelationProposalRequest(
            proposal_id=proposal_id,
            proposal_version_id=proposal_version_id,
            version_number=1,
            expected_previous_version_id=None,
            predicate_registry_digest=EDITORIAL_PREDICATE_REGISTRY_V1.digest,
            predicate_contract_digest=contract.digest,
            predicate=predicate,
            subject=item["subject"],
            object=item["object"],
            temporal_scope=EditorialRelationTemporalScope(
                valid_from=None,
                valid_until=None,
                observed_at=observed_at,
            ),
            evidence=_relation_evidence(item),
            resolution_dependency_ids=item["dependency_ids"],
            producer=EditorialRelationProducer(
                kind=EditorialRelationProducerKind.EXTRACTION_RUN,
                producer_id=PRODUCER_ID,
                producer_version=PRODUCER_VERSION,
                contract_digest=item["producer_contract_digest"],
            ),
            statement=(
                "Admitted Canonical Entity endpoints from the bound representation "
                "participate in the same first-boot process."
            ),
            confidence_basis_points=item["confidence_basis_points"],
            uncertainty_codes=("REQUIRES_RELATION_ADMISSION",),
            basis_codes=("BOUND_REPRESENTATION_SPAN",),
            idempotency_key=f"first-boot-4c-propose-{item['source_proposal_id']}",
        ),
        proof=proof,
    )
    decision = system.relations.decide(
        EditorialRelationDecisionRequest(
            decision_id=decision_id,
            action=EditorialRelationDecisionAction.ACCEPT,
            proposal_id=proposal.proposal_id,
            proposal_version_id=proposal.proposal_version_id,
            expected_proposal_version_digest=proposal.canonical_digest,
            expected_previous_decision_id=None,
            expected_previous_decision_version=0,
            assertion_id=assertion_id,
            target_assertion_id=None,
            successor_assertion_id=None,
            supersession_id=None,
            reason_code=DECISION_REASON_CODE,
            decision_policy_version=EDITORIAL_RELATION_ADMISSION_POLICY_VERSION,
            idempotency_key=f"first-boot-4c-accept-{item['source_proposal_id']}",
        ),
        proof=proof,
    )
    source = item["endpoints"]["entity.source"]
    item_endpoint = item["endpoints"]["entity.item"]
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
        "proposal_id": str(proposal.proposal_id),
        "decision_id": str(decision.decision_id),
        "assertion_id": str(decision.assertion_id),
        "predicate": predicate.value,
        "subject_entity_id": str(item["subject"].entity_id),
        "subject_entity_version_id": str(item["subject"].entity_version_id),
        "object_entity_id": str(item["object"].entity_id),
        "object_entity_version_id": str(item["object"].entity_version_id),
        "source_entity_id": str(source["accepted_entity_id"]),
        "item_entity_id": str(item_endpoint["accepted_entity_id"]),
        "action": decision.action.value,
        "replayed": proposal.replayed and decision.replayed,
    }


def record_first_boot_editorial_relations(
    path: Path,
    *,
    clock: Callable[[], UtcTimestamp] | None = None,
) -> dict[str, Any]:
    from newsroom.graphiti_adapter.models import REAL_GRAPHITI_RUNTIME_ENABLED

    if REAL_GRAPHITI_RUNTIME_ENABLED:
        raise LeadEditorialRelationError("REAL_GRAPHITI_RUNTIME_ENABLED must stay False")
    selected, skipped = _eligible_relations(path)
    if not selected and not skipped:
        raise LeadEditorialRelationError(
            "no retained live-official extracts in the 4C allowlist"
        )
    related: list[dict[str, Any]] = []
    if selected:
        proof = _proof()
        now = clock or UtcTimestamp.now
        selected_clock = now if callable(now) else (lambda: now)
        observed_at = selected_clock()
        entity_system = _open_entity_system(path, selected_clock)
        try:
            for item in selected:
                _bind_dependencies(entity_system, item=item, proof=proof)
        finally:
            entity_system.close()
        relation_system = _open_relation_system(path, selected_clock)
        try:
            for item in selected:
                related.append(
                    _accept_relation(
                        relation_system,
                        item=item,
                        proof=proof,
                        observed_at=observed_at,
                    )
                )
        finally:
            relation_system.close()
        failed = [item for item in related if item["action"] != "ACCEPT"]
        if failed:
            raise LeadEditorialRelationError(
                "live-official editorial relation did not ACCEPT every allowlisted extract"
            )
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
        "editorial_relation_proposals": _count_table(
            path, "editorial_relation_proposals"
        ),
        "editorial_relation_decisions": _count_table(
            path, "editorial_relation_decisions"
        ),
        "editorial_relation_assertions": _count_table(
            path, "editorial_relation_assertions"
        ),
        "related": related,
        "skipped": skipped,
        "graphiti_runtime_enabled": REAL_GRAPHITI_RUNTIME_ENABLED,
    }


__all__ = [
    "RELATE_SOURCE_IDS",
    "LeadEditorialRelationError",
    "record_first_boot_editorial_relations",
]
