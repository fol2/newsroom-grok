"""Governed live-official 4A extract for first-boot News Leads.

Binds each eligible lead's Source Revision, Discovery Representation and one
admitted representation passage. Does not use DeterministicFixtureExtractor,
Graphiti, 4B/4C, remint, or invented 3C rows.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

from newsroom.authority import (
    AuthenticationProof,
    EventReadPolicy,
    HydrationPolicyContract,
    HydrationPolicyRegistry,
    HydrationRequest,
    MetadataClass,
    ObjectAdmissionDefinition,
    ObjectAdmissionRegistry,
    ObjectAdmissionRequest,
    ObjectLimits,
    RightsPolicyContract,
    RightsPolicyRegistry,
    StaticAuthorizer,
    TrustScope,
    UtcTimestamp,
    canonical_json_bytes,
    digest_bytes,
    open_governed_object_authority_system,
)
from newsroom.authority.extraction_system import open_governed_extraction_authority_system
from newsroom.authority.object_policy import merge_authority_registries
from newsroom.checks import deterministic_uuid4
from newsroom.discovery_lead_admission import (
    ID_NAMESPACE as LEAD_ID_NAMESPACE,
    _SOURCE_PROFILES,
    read_ingest_admissions,
)
from newsroom.envelope_grant import OWNER_CREDENTIAL, OWNER_PRINCIPAL
from newsroom.extraction import (
    ExtractionBudget,
    ExtractionInputBinding,
    ExtractionPassageId,
    ExtractionPassageInput,
    ExtractionReadPolicy,
    ExtractionRunId,
    ExtractionRunRequest,
    ExtractionRunVersionId,
    ExtractorContractId,
    live_official_contract_request,
)
from newsroom.host_store import (
    HOST_DISCOVERY_SCOPES,
    host_authenticator,
    host_authority_registries,
    host_authorizer,
)
from newsroom.sources import (
    DiscoveryRepresentationId,
    SourceDefinitionId,
    SourceDefinitionVersionId,
    SourceItemId,
    SourceRevisionId,
)
from newsroom.x_search_ingest import DEFAULT_SOURCE_ID as X_SEARCH_SOURCE_ID

EXTRACT_ID_NAMESPACE = "newsroom.first-boot.extract-v1"
EXTRACT_SOURCE_IDS = (
    "HK-01",
    "HK-04",
    "UK-01",
    "UK-05",
    "RAD-01",
    X_SEARCH_SOURCE_ID,
)
EXTRACT_SOURCE_ID_SET = frozenset(EXTRACT_SOURCE_IDS)
ADMISSION_TYPE = "discovery.representation"
OBJECT_CLASS = "discovery_representation"
PURPOSE = "extraction.live-official"
ALLOWED_USE = "extraction.live-official"
SECURITY_SCOPE = "authority.extraction"
RETENTION_SCOPE = "extraction.live-official"
CONTRACT_IDEMPOTENCY_KEY = "increment-4a-live-official-contract-v1"


class LeadExtractionError(ValueError):
    """First-boot live-official extraction failed closed."""


def _proof() -> AuthenticationProof:
    return AuthenticationProof(method="STATIC_TOKEN", credential=OWNER_CREDENTIAL)


def _extract_id(identifier_type, *, kind: str, semantic: object):
    return deterministic_uuid4(
        identifier_type,
        namespace=EXTRACT_ID_NAMESPACE,
        semantic_value={"kind": kind, "value": semantic},
    )


def _lead_definition_id(source_id: str) -> SourceDefinitionId:
    return deterministic_uuid4(
        SourceDefinitionId,
        namespace=LEAD_ID_NAMESPACE,
        semantic_value={"kind": "definition", "value": source_id},
    )


def object_root_for_ledger(path: Path) -> Path:
    return Path(path).resolve().parent / "objects"


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


def _definition_map(*, source_ids: tuple[str, ...] | frozenset[str] | None = None) -> dict[str, str]:
    selected = tuple(source_ids) if source_ids is not None else tuple(_SOURCE_PROFILES)
    return {str(_lead_definition_id(source_id)): source_id for source_id in selected}


def _existing_run_bindings(path: Path) -> set[tuple[str, str]]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='extraction_runs'"
        ).fetchone()
        if present is None:
            return set()
        rows = conn.execute(
            "SELECT revision_id, representation_id FROM extraction_runs"
        ).fetchall()
    finally:
        conn.close()
    return {(str(revision_id), str(representation_id)) for revision_id, representation_id in rows}


def _load_leads(path: Path) -> tuple[dict[str, Any], ...]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='news_leads'"
        ).fetchone()
        if present is None:
            raise LeadExtractionError("news_leads is absent; admit-leads first")
        rows = conn.execute(
            "SELECT nl.lead_id,nl.signal_id,nl.definition_id,nl.definition_version_id,"
            "nl.item_id,nl.revision_id,nl.representation_id,"
            "si.source_native_id,sr.permitted_state_digest,dr.representation_digest "
            "FROM news_leads nl "
            "JOIN source_items si ON si.item_id=nl.item_id AND si.definition_id=nl.definition_id "
            "JOIN source_revisions sr ON sr.revision_id=nl.revision_id AND sr.item_id=nl.item_id "
            "JOIN discovery_representations dr "
            "ON dr.representation_id=nl.representation_id AND dr.revision_id=nl.revision_id "
            "ORDER BY nl.lead_id"
        ).fetchall()
    finally:
        conn.close()
    return tuple(dict(row) for row in rows)


def _ingest_by_item(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    indexed: dict[tuple[str, str], dict[str, Any]] = {}
    for admission in read_ingest_admissions(path):
        payload = admission["payload"]
        indexed[(str(payload["source_id"]), str(payload["item_id"]))] = payload
    return indexed


def _representation_bytes(*, source_id: str, native_item_id: str, url: str) -> bytes:
    return canonical_json_bytes(
        {"item_id": native_item_id, "source_id": source_id, "url": url}
    )


def _eligible_leads(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    definitions = _definition_map()
    ingest = _ingest_by_item(path)
    existing = _existing_run_bindings(path)
    selected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in _load_leads(path):
        source_id = definitions.get(str(row["definition_id"]))
        lead_id = str(row["lead_id"])
        revision_id = str(row["revision_id"])
        representation_id = str(row["representation_id"])
        if source_id not in EXTRACT_SOURCE_ID_SET:
            skipped.append(
                {
                    "source_id": source_id,
                    "lead_id": lead_id,
                    "item_id": str(row["item_id"]),
                    "revision_id": revision_id,
                    "representation_id": representation_id,
                    "reason": "source-not-in-4a-allowlist",
                }
            )
            continue
        if (revision_id, representation_id) in existing:
            skipped.append(
                {
                    "source_id": source_id,
                    "lead_id": lead_id,
                    "item_id": str(row["item_id"]),
                    "revision_id": revision_id,
                    "representation_id": representation_id,
                    "reason": "already-extracted",
                }
            )
            continue
        payload = ingest.get((source_id, str(row["source_native_id"])))
        if payload is None:
            raise LeadExtractionError(
                f"ingest payload missing for {source_id} representation binding"
            )
        blob = _representation_bytes(
            source_id=source_id,
            native_item_id=str(row["source_native_id"]),
            url=str(payload["url"]),
        )
        digest = digest_bytes(blob)
        if digest != str(row["representation_digest"]) or digest != str(
            row["permitted_state_digest"]
        ):
            raise LeadExtractionError(
                f"{source_id} representation digest does not match the bound revision"
            )
        selected.append(
            {
                "source_id": source_id,
                "language": _SOURCE_PROFILES[source_id].language,
                "blob": blob,
                "digest": digest,
                **{key: str(row[key]) for key in row.keys()},
            }
        )
    selected.sort(key=lambda item: EXTRACT_SOURCE_IDS.index(item["source_id"]))
    return selected, skipped


def _extract_authorizer() -> StaticAuthorizer:
    grants = dict(host_authorizer()._grants_by_principal)
    owner = set(grants[OWNER_PRINCIPAL])
    owner.update(HOST_DISCOVERY_SCOPES)
    owner.update(
        {
            "authority.objects.admit",
            "authority.objects.read",
            "authority.objects.manage",
            "authority.objects.lifecycle.write",
            "authority.admitted.write",
            "authority.observed.write",
            "authority.extraction.manage",
            "authority.extraction.execute",
            "authority.extraction.read",
            "authority.extraction.read_proposals",
            "authority.extraction.read_raw",
            "authority.host.read",
        }
    )
    grants[OWNER_PRINCIPAL] = frozenset(owner)
    return StaticAuthorizer(
        policy_version="host-store-extract-v1",
        grants_by_principal=grants,
    )


def _object_policies() -> tuple[
    RightsPolicyRegistry,
    HydrationPolicyRegistry,
    ObjectAdmissionRegistry,
]:
    rights_contract = RightsPolicyContract(
        policy_key="live-official-extract",
        contract_version="rights-v1",
        implementation_version="rights-static-v1",
        preflight_allowed=True,
        reason_code="PERMITTED",
    )
    rights = RightsPolicyRegistry((rights_contract,))
    hydration_contract = HydrationPolicyContract(
        policy_id="live-official-extract-hydration-v1",
        contract_version="hydration-v1",
        implementation_version="hydration-static-v1",
        purpose=PURPOSE,
        required_scope="authority.objects.read",
        allowed_principal_ids=frozenset({OWNER_PRINCIPAL}),
        allowed_authority_domains=frozenset({"newsroom.host"}),
        allowed_object_classes=frozenset({OBJECT_CLASS}),
        allowed_uses=frozenset({ALLOWED_USE}),
        allowed_security_scopes=frozenset({SECURITY_SCOPE}),
        allowed_retention_scopes=frozenset({RETENTION_SCOPE}),
        max_bytes=64 * 1024,
        allow_ranges=False,
    )
    hydration = HydrationPolicyRegistry((hydration_contract,))
    admissions = ObjectAdmissionRegistry(
        (
            ObjectAdmissionDefinition(
                admission_type=ADMISSION_TYPE,
                definition_version="admission-v1",
                object_class=OBJECT_CLASS,
                allowed_use=ALLOWED_USE,
                security_scope=SECURITY_SCOPE,
                retention_scope=RETENTION_SCOPE,
                required_write_scope="authority.objects.admit",
                required_read_scope="authority.objects.read",
                required_manage_scope="authority.objects.manage",
                rights_policy_contract_digest=rights_contract.contract_digest,
                hydration_policy_contract_digests=frozenset(
                    {hydration_contract.contract_digest}
                ),
            ),
        ),
        rights_policies=rights,
        hydration_policies=hydration,
    )
    return rights, hydration, admissions


def _event_read_policy() -> EventReadPolicy:
    return EventReadPolicy(
        policy_id="host-extract-read-v1",
        purpose="host.extract.audit",
        required_scope="authority.host.read",
        allowed_principal_ids=frozenset({OWNER_PRINCIPAL}),
        allowed_security_scopes=frozenset(
            {
                "authority.host",
                "authority.envelope",
                "authority.discovery",
                "authority.publication",
                "authority.source_registry",
                "authority.discovery_checks",
                "authority.audit",
                "authority.object_lifecycle",
                "authority.extraction",
                "authority.protected",
                "authority.internal",
            }
        ),
        allowed_trust_scopes=frozenset(
            {TrustScope.OBSERVED, TrustScope.ADMITTED, TrustScope.PROPOSED}
        ),
        metadata_classes=frozenset(
            {
                MetadataClass.ROUTING,
                MetadataClass.PROVENANCE,
                MetadataClass.RESULT,
            }
        ),
    )


def _extraction_read_policy() -> ExtractionReadPolicy:
    return ExtractionReadPolicy(
        policy_id="first-boot-live-official-read-v1",
        purpose="extraction.live-official.audit",
        metadata_required_scope="authority.extraction.read",
        proposal_required_scope="authority.extraction.read_proposals",
        raw_output_required_scope="authority.extraction.read_raw",
        allowed_principal_ids=frozenset({OWNER_PRINCIPAL}),
        max_results=100,
    )


def _open_object_system(path: Path, object_root: Path, clock):
    registry, schemas = host_authority_registries()
    rights, hydration, admissions = _object_policies()
    object_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    object_root.chmod(0o700)
    return open_governed_object_authority_system(
        path=path,
        object_root=object_root,
        registry=registry,
        payload_schemas=schemas,
        admission_registry=admissions,
        rights_policies=rights,
        hydration_policies=hydration,
        authenticator=host_authenticator(),
        authorizer=_extract_authorizer(),
        event_read_policy=_event_read_policy(),
        object_limits=ObjectLimits(
            global_max_bytes=1024 * 1024,
            class_max_bytes={OBJECT_CLASS: 64 * 1024},
            max_read_bytes=64 * 1024,
            min_free_bytes=0,
        ),
        clock=clock,
    )


def _open_extraction_system(path: Path, clock):
    registry, schemas = host_authority_registries()
    registry, schemas = merge_authority_registries(
        command_registry=registry,
        payload_schemas=schemas,
    )
    return open_governed_extraction_authority_system(
        path=path,
        registry=registry,
        payload_schemas=schemas,
        authenticator=host_authenticator(),
        authorizer=_extract_authorizer(),
        read_policy=_extraction_read_policy(),
        clock=clock,
    )


def _hydrate_passages(
    path: Path,
    *,
    object_root: Path,
    leads: list[dict[str, Any]],
    proof: AuthenticationProof,
    clock,
) -> dict[str, ExtractionPassageInput]:
    system = _open_object_system(path, object_root, clock)
    passages: dict[str, ExtractionPassageInput] = {}
    try:
        for lead in leads:
            admitted = system.objects.admit(
                ObjectAdmissionRequest(
                    ADMISSION_TYPE,
                    f"first-boot-extract-admit-{lead['lead_id']}",
                ),
                lead["blob"],
                proof=proof,
            ).admission
            hydrated = system.objects.hydrate(
                HydrationRequest(admitted.admission_id, PURPOSE),
                proof=proof,
            )
            decision = hydrated.decision
            text = hydrated.data.decode("utf-8")
            passages[lead["lead_id"]] = ExtractionPassageInput(
                passage_id=_extract_id(
                    ExtractionPassageId,
                    kind="passage",
                    semantic=lead["lead_id"],
                ),
                admission_id=admitted.admission_id,
                access_decision_id=decision.access_decision_id,
                hydration_policy_contract_digest=decision.policy_contract_digest,
                principal_id=decision.principal_id,
                authority_domain=decision.authority_domain,
                purpose=decision.purpose,
                object_class=decision.object_class,
                allowed_use=decision.allowed_use,
                security_scope=decision.security_scope,
                retention_scope=decision.retention_scope,
                byte_offset=decision.offset,
                byte_length=decision.allowed_bytes,
                blob_digest=admitted.blob.blob_digest,
                text_digest=admitted.blob.blob_digest,
                language=lead["language"],
                text=text,
            )
    finally:
        system.close()
    return passages


def record_first_boot_extraction(
    path: Path,
    *,
    object_root: Path | None = None,
    clock: Callable[[], UtcTimestamp] | None = None,
) -> dict[str, Any]:
    from newsroom.graphiti_adapter.models import REAL_GRAPHITI_RUNTIME_ENABLED

    if REAL_GRAPHITI_RUNTIME_ENABLED:
        raise LeadExtractionError("REAL_GRAPHITI_RUNTIME_ENABLED must stay False")
    selected, skipped = _eligible_leads(path)
    if not selected and not skipped:
        raise LeadExtractionError("no queued first-boot leads in the 4A allowlist")
    extracted: list[dict[str, Any]] = []
    if selected:
        proof = _proof()
        now = clock or UtcTimestamp.now
        selected_clock = now if callable(now) else (lambda: now)
        cas_root = object_root or object_root_for_ledger(path)
        passages = _hydrate_passages(
            path,
            object_root=cas_root,
            leads=selected,
            proof=proof,
            clock=selected_clock,
        )
        contract_request = live_official_contract_request(
            contract_id=_extract_id(
                ExtractorContractId, kind="contract", semantic="live-official-v1"
            ),
            idempotency_key=CONTRACT_IDEMPOTENCY_KEY,
        )
        system = _open_extraction_system(path, selected_clock)
        try:
            system.extraction.register_contract(contract_request, proof=proof)
            for lead in selected:
                passage = passages[lead["lead_id"]]
                run_id = _extract_id(
                    ExtractionRunId, kind="run", semantic=lead["lead_id"]
                )
                run_version_id = _extract_id(
                    ExtractionRunVersionId,
                    kind="run-version",
                    semantic=lead["lead_id"],
                )
                result = system.extraction.execute(
                    ExtractionRunRequest(
                        run_id=run_id,
                        run_version_id=run_version_id,
                        version_number=1,
                        expected_previous_version_id=None,
                        contract_id=contract_request.contract_id,
                        input_binding=ExtractionInputBinding(
                            definition_id=SourceDefinitionId.parse(
                                lead["definition_id"]
                            ),
                            definition_version_id=SourceDefinitionVersionId.parse(
                                lead["definition_version_id"]
                            ),
                            item_id=SourceItemId.parse(lead["item_id"]),
                            revision_id=SourceRevisionId.parse(lead["revision_id"]),
                            representation_id=DiscoveryRepresentationId.parse(
                                lead["representation_id"]
                            ),
                            passages=(passage,),
                        ),
                        budget=ExtractionBudget(
                            timeout_ms=10_000,
                            max_input_bytes=64 * 1024,
                            max_output_bytes=256 * 1024,
                            max_proposals=100,
                            max_evidence_ranges=500,
                            max_request_tokens=0,
                            max_response_tokens=0,
                            max_cost_microunits=0,
                        ),
                        idempotency_key=f"first-boot-extract-run-{lead['lead_id']}",
                    ),
                    proof=proof,
                )
                extracted.append(
                    {
                        "source_id": lead["source_id"],
                        "lead_id": lead["lead_id"],
                        "run_id": str(run_id),
                        "revision_id": lead["revision_id"],
                        "representation_id": lead["representation_id"],
                        "item_id": lead["item_id"],
                        "outcome": result.outcome.value,
                        "replayed": result.replayed,
                    }
                )
        finally:
            system.close()
        failed = [item for item in extracted if item["outcome"] != "SUCCESS"]
        if failed:
            raise LeadExtractionError(
                "live-official extraction did not retain SUCCESS for every allowlisted lead"
            )
    return {
        "ok": True,
        "auto_publish": False,
        "discord": False,
        "public_adapter": False,
        "x_as_publisher": False,
        "graphiti": False,
        "news_leads": _count_table(path, "news_leads"),
        "extraction_runs": _count_table(path, "extraction_runs"),
        "extracted": extracted,
        "skipped": skipped,
        "editorial_relation_decisions": _count_table(
            path, "editorial_relation_decisions"
        ),
        "graphiti_runtime_enabled": REAL_GRAPHITI_RUNTIME_ENABLED,
    }


__all__ = [
    "EXTRACT_SOURCE_IDS",
    "LeadExtractionError",
    "object_root_for_ledger",
    "record_first_boot_extraction",
]
