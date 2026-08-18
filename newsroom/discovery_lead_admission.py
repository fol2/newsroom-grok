"""Admit first-boot ledger signals through Increment 3C lineage into News Leads.

Reads already-admitted `discovery.signal.ingest` / `discovery.x_search.ingest`
rows, retains exact Source/Check lineage bound to those source_id + item_id
values, runs deterministic gates, then calls `admit_signal_to_lead`.

Does not remint publication bundles, write extraction/editorial rows, or
execute Increment 4A.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from newsroom.authority import AuthenticationProof, canonical_json_bytes, digest_bytes
from newsroom.authority.types import UtcTimestamp
from newsroom.checks import (
    BaselineDecisionId,
    BaselineDecisionKind,
    BaselineDecisionRequest,
    BaselineDisposition,
    BaselineEntryDisposition,
    BaselineManifestEntry,
    CandidateObservationRef,
    CheckAttemptId,
    CheckAttemptKind,
    CheckAttemptRequest,
    CheckOutcomeId,
    CheckOutcomeKind,
    CheckOutcomeRequest,
    CheckRequestId,
    CheckRequestRequest,
    CoverageBasis,
    ObservableTransitionId,
    ObservableTransitionKind,
    ObservableTransitionRequest,
    QuarantineDisposition,
    TransitionBasis,
    TriggerKind,
    TriggerRef,
    deterministic_uuid4,
)
from newsroom.discovery_adapters import AdapterRequestId, ObservationProposalId
from newsroom.discovery import (
    DecisionTerminality,
    DiscoverySignalId,
    DiscoverySignalRequest,
    GateBasis,
    GateDecisionId,
    GateDecisionRequest,
    GateOutcome,
    LeadDispositionDecisionId,
    LeadDispositionDecisionRequest,
    LeadDispositionOutcome,
    NewsLeadId,
    NewsLeadRequest,
    NextAction,
    NextActionKind,
    ObservableNewness,
    ReasonBasisClass,
    ReasonReference,
    ScopeDisposition,
    SignalLeadAdmissionRequest,
    StructuredReason,
    TimeValidity,
    UrgencyBasis,
    UrgencyRoute,
    deterministic_gate_outcome,
)
from newsroom.discovery_ingest import (
    COMMAND_TYPE as RSS_INGEST_COMMAND,
    MAX_BODY_BYTES,
    SKIP_COMMAND_TYPE as RSS_SKIP_COMMAND,
    SOURCE_URLS,
    FeedFetchError,
    all_feed_item_ids,
    load_official_rss_body,
    resolve_rss_source_id,
)
from newsroom.envelope_grant import OWNER_CREDENTIAL
from newsroom.increment9.proving import ALLOWED_HOSTS, ProvingError, assert_allowed_url
from newsroom.sources import (
    BaselinePolicy,
    BaselinePolicyKind,
    CoverageContribution,
    CoverageMapping,
    CoverageResponsibility,
    DiscoveryOccurrenceId,
    DiscoveryOccurrenceKind,
    DiscoveryOccurrenceRequest,
    DiscoveryRepresentationId,
    DiscoveryRepresentationRequest,
    ObservationModel,
    PortfolioFunction,
    RightsReference,
    SourceDefinitionId,
    SourceDefinitionRequest,
    SourceDefinitionVersionId,
    SourceDefinitionVersionRequest,
    SourceDependency,
    SourceDependencyKind,
    SourceItemId,
    SourceItemIdentityKind,
    SourceItemRequest,
    SourceLifecycleStage,
    SourceRevisionId,
    SourceRevisionRequest,
    SourceRole,
    SourceRoleAssignment,
    SourceTime,
    VersionedPolicyRef,
)
from newsroom.x_search_ingest import (
    COMMAND_TYPE as X_SEARCH_INGEST_COMMAND,
    DEFAULT_SOURCE_ID as X_SEARCH_SOURCE_ID,
)

INGEST_COMMAND_TYPES = frozenset({RSS_INGEST_COMMAND, X_SEARCH_INGEST_COMMAND})
SKIP_COMMAND_TYPES = frozenset({RSS_SKIP_COMMAND})
SKIP_SOURCE_IDS = frozenset({"UK-10"})
_ROW_TABLES = frozenset(
    {"news_leads", "source_definitions", "discovery_signals", "source_items"}
)
ITEM_IDENTITY_NAMESPACE = "increment-3c-source-item-v1"
ID_NAMESPACE = "newsroom.first-boot.lead-v1"
POLICY_VERSION = "v1"
RIGHTS_POLICY = "first-boot-rights-v1"
ALLOWED_USE = "discovery.first-boot-lead"
COVERAGE_OBLIGATION = "COV.FB01"
FIXTURE_SIGNAL_ID = DiscoverySignalId.parse("00000000-0000-4000-8000-000000007009")

FeedLoader = Callable[[str], tuple[str, str, bytes]]


class LeadAdmissionError(ValueError):
    """First-boot Signal-to-Lead admission failed closed."""


@dataclass(frozen=True, slots=True)
class _SourceProfile:
    name: str
    purpose: str
    role: SourceRole
    geography: str
    language: str


_SOURCE_PROFILES = {
    "HK-01": _SourceProfile(
        "Hong Kong Government News",
        "Observe official Hong Kong Government News items.",
        SourceRole.ORIGINATING_AUTHORITY,
        "HK",
        "zh-Hant",
    ),
    "HK-04": _SourceProfile(
        "Hong Kong Education Bureau",
        "Observe official Hong Kong Education Bureau items.",
        SourceRole.ORIGINATING_AUTHORITY,
        "HK",
        "zh-Hant",
    ),
    "RAD-01": _SourceProfile(
        "RTHK News",
        "Observe RTHK radar items admitted from the official feed.",
        SourceRole.ESTABLISHED_MEDIA_RADAR,
        "HK",
        "zh-Hant",
    ),
    "RAD-02": _SourceProfile(
        "BBC News UK",
        "Observe BBC UK radar items admitted from the official feed.",
        SourceRole.ESTABLISHED_MEDIA_RADAR,
        "UK",
        "en-GB",
    ),
    "UK-01": _SourceProfile(
        "UK Home Office and UKVI",
        "Observe Home Office and UKVI Atom items.",
        SourceRole.ORIGINATING_AUTHORITY,
        "UK",
        "en-GB",
    ),
    "UK-05": _SourceProfile(
        "UK Department for Education",
        "Observe Department for Education Atom items.",
        SourceRole.ORIGINATING_AUTHORITY,
        "UK",
        "en-GB",
    ),
    X_SEARCH_SOURCE_ID: _SourceProfile(
        "Official Hong Kong government X posts",
        "Reuse the gated first-boot X-search admission; no user-X enroll.",
        SourceRole.SPECIALIST_OR_LOCAL_RADAR,
        "HK",
        "zh-Hant",
    ),
}


def _policy(name: str) -> VersionedPolicyRef:
    return VersionedPolicyRef(f"first-boot-{name}", POLICY_VERSION)


def _proof() -> AuthenticationProof:
    return AuthenticationProof(method="STATIC_TOKEN", credential=OWNER_CREDENTIAL)


def _id(identifier_type, *, kind: str, semantic: object):
    return deterministic_uuid4(
        identifier_type,
        namespace=ID_NAMESPACE,
        semantic_value={"kind": kind, "value": semantic},
    )


def _item_key(item_id: str) -> str:
    return digest_bytes(item_id.encode("utf-8"))


def _observed_item_id(*, definition_id: SourceDefinitionId, item_key: str) -> SourceItemId:
    return deterministic_uuid4(
        SourceItemId,
        namespace=ITEM_IDENTITY_NAMESPACE,
        semantic_value={"definition_id": str(definition_id), "item_key": item_key},
    )


def resolve_atom_tag(item_id: str) -> str:
    if not isinstance(item_id, str) or not item_id.startswith("tag:"):
        raise LeadAdmissionError("Atom tag cannot be resolved")
    rest = item_id[4:]
    comma = rest.find(",")
    if comma < 1:
        raise LeadAdmissionError("Atom tag cannot be resolved")
    colon = rest.find(":", comma + 1)
    if colon < 0 or colon == len(rest) - 1:
        raise LeadAdmissionError("Atom tag cannot be resolved")
    authority = rest[:comma]
    specific = rest[colon + 1 :]
    if not authority or "/" in authority or " " in authority:
        raise LeadAdmissionError("Atom tag cannot be resolved")
    if not specific.startswith("/"):
        specific = "/" + specific
    return f"https://{authority}{specific}"


def item_identity_url(item_id: str) -> str | None:
    if item_id.startswith("https://"):
        return item_id.split("#", 1)[0]
    if item_id.startswith("tag:"):
        return resolve_atom_tag(item_id)
    return None


def _host_allowed(url: str) -> bool:
    try:
        assert_allowed_url(url)
    except ProvingError:
        return False
    host = (urlsplit(url).hostname or "").lower()
    return host in ALLOWED_HOSTS


def _default_feed_loader(source_id: str) -> tuple[str, str, bytes]:
    feed_dir = os.environ.get("NEWSROOM_LEAD_FEED_DIR", "").strip()
    if feed_dir:
        selected = resolve_rss_source_id(source_id)
        url = SOURCE_URLS[selected]
        assert_allowed_url(url)
        path = Path(feed_dir) / f"{selected}.xml"
        if path.is_symlink() or not path.is_file():
            raise FeedFetchError(f"recorded official feed missing: {selected}")
        body = path.read_bytes()
        if not body or len(body) > MAX_BODY_BYTES:
            raise FeedFetchError("recorded official feed is unusable")
        return selected, url, body
    return load_official_rss_body(source_id)


def _has_row(path: Path, table: str, column: str, value: str) -> bool:
    if table not in _ROW_TABLES:
        raise LeadAdmissionError("unknown first-boot lead table")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if present is None:
            return False
        row = conn.execute(
            f"SELECT 1 FROM {table} WHERE {column}=? LIMIT 1",
            (value,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def _payload_rows(path: Path, command_types: frozenset[str]) -> tuple[dict[str, Any], ...]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        rows = conn.execute(
            "SELECT e.ledger_seq, e.event_type, c.command_type, p.payload_bytes "
            "FROM ledger_events e "
            "JOIN authority_commands c ON c.command_id=e.command_id "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "ORDER BY e.ledger_seq"
        ).fetchall()
    finally:
        conn.close()
    found: list[dict[str, Any]] = []
    for seq, event_type, command_type, payload_bytes in rows:
        if str(command_type) not in command_types:
            continue
        payload = json.loads(bytes(payload_bytes))
        if not isinstance(payload, dict):
            raise LeadAdmissionError("ingest payload must be an object")
        found.append(
            {
                "ledger_seq": int(seq),
                "event_type": str(event_type),
                "command_type": str(command_type),
                "payload": payload,
            }
        )
    return tuple(found)


def read_ingest_admissions(path: Path) -> tuple[dict[str, Any], ...]:
    return _payload_rows(path, INGEST_COMMAND_TYPES)


def read_ingest_skips(path: Path) -> tuple[dict[str, Any], ...]:
    return _payload_rows(path, SKIP_COMMAND_TYPES)


def _hold_reason(admission: dict[str, Any], *, feed_loader: FeedLoader) -> str | None:
    payload = admission["payload"]
    source_id = payload["source_id"]
    item_id = payload["item_id"]
    if source_id in SKIP_SOURCE_IDS:
        return "uk-10-empty-feed-skip"
    if source_id == X_SEARCH_SOURCE_ID:
        return None
    try:
        resolve_rss_source_id(source_id)
    except ValueError:
        return "source-not-official-rss"
    try:
        item_url = item_identity_url(item_id)
    except LeadAdmissionError:
        return "atom-tag-unresolved"
    if item_id.startswith("tag:") and item_url is not None and not _host_allowed(item_url):
        return "host-not-allowed"
    try:
        loaded_id, _url, body = feed_loader(source_id)
    except FeedFetchError:
        return "official-feed-fetch-failed"
    except (ProvingError, ValueError):
        return "official-feed-fetch-failed"
    if loaded_id != source_id:
        return "official-feed-fetch-failed"
    if item_id not in all_feed_item_ids(body):
        return "item-gone"
    return None


def _source_locator(payload: dict[str, Any]) -> str:
    source_id = payload["source_id"]
    if source_id == X_SEARCH_SOURCE_ID:
        return payload["url"]
    return SOURCE_URLS[source_id]


def _retain_source_contract(system, *, payload: dict[str, Any], proof):
    source_id = payload["source_id"]
    profile = _SOURCE_PROFILES[source_id]
    definition_id = _id(SourceDefinitionId, kind="definition", semantic=source_id)
    version_id = _id(SourceDefinitionVersionId, kind="version", semantic=source_id)
    rights_id = str(_id(SourceDefinitionId, kind="rights", semantic=source_id))
    roles = (
        SourceRoleAssignment(
            profile.role,
            profile.purpose,
            ("First-boot admitted item only.", "No public adapter."),
        ),
    )
    mappings = (
        CoverageMapping(
            COVERAGE_OBLIGATION,
            CoverageResponsibility.ACTIVE,
            CoverageContribution.DETECTION_PATH,
            (profile.geography,),
            (profile.language,),
            ("First-boot admitted Signal to Lead only.",),
        ),
    )
    dependencies = (
        SourceDependency(
            f"origin-{source_id}",
            SourceDependencyKind.ORIGINATING_MATERIAL,
            "Official first-boot source material for this admitted item.",
        ),
    )
    system.sources.register_definition(
        SourceDefinitionRequest(
            definition_id=definition_id,
            name=profile.name,
            editorial_purpose=profile.purpose,
            idempotency_key=f"first-boot-source-def-{source_id}-v1",
        ),
        proof=proof,
    )
    system.sources.record_definition_version(
        SourceDefinitionVersionRequest(
            version_id=version_id,
            definition_id=definition_id,
            version_number=1,
            expected_previous_version_id=None,
            locator=_source_locator(payload),
            adapter_contract=_policy("adapter"),
            extraction_scope=("body", "source_updated_time", "title"),
            rights=RightsReference(
                rights_decision_id=rights_id,
                rights_policy_version=RIGHTS_POLICY,
                allowed_use=ALLOWED_USE,
                retention_scope="authority.audit",
            ),
            roles=roles,
            portfolio_functions=(PortfolioFunction.ANCHOR,),
            coverage_mappings=mappings,
            dependencies=dependencies,
            explicit_gaps=(),
            observation_model=ObservationModel.MUTABLE_ITEM,
            baseline_policy=BaselinePolicy(
                reference=_policy("baseline"),
                kind=BaselinePolicyKind.MAINTAINED_DOCUMENT,
                reset_requires_decision=True,
                notes="First observation of one admitted item.",
            ),
            item_identity_policy=_policy("item-identity"),
            revision_policy=_policy("revision"),
            canonicalization_policy=_policy("canonicalizer"),
            lifecycle_stage=SourceLifecycleStage.PRODUCTION_ELIGIBLE,
            change_reason="Retain first-boot admitted source contract.",
            idempotency_key=f"first-boot-source-ver-{source_id}-v1",
        ),
        proof=proof,
    )
    coverage = CoverageBasis(
        COVERAGE_OBLIGATION,
        CoverageResponsibility.ACTIVE,
        CoverageContribution.DETECTION_PATH,
        _policy("coverage"),
    )
    return definition_id, version_id, rights_id, roles, dependencies, coverage


def _admit_one(
    system,
    *,
    path: Path,
    admission: dict[str, Any],
    proof,
    now: UtcTimestamp,
) -> dict[str, Any]:
    payload = admission["payload"]
    source_id = payload["source_id"]
    native_item_id = payload["item_id"]
    item_key = _item_key(native_item_id)
    semantic = {
        "source_id": source_id,
        "item_id": native_item_id,
        "item_key": item_key,
    }
    signal_id = _id(DiscoverySignalId, kind="signal", semantic=semantic)
    lead_id = _id(NewsLeadId, kind="lead", semantic=semantic)
    definition_id = _id(SourceDefinitionId, kind="definition", semantic=source_id)
    if signal_id == FIXTURE_SIGNAL_ID:
        raise LeadAdmissionError("live Signal identity must not reuse the 3D fixture")
    if _has_row(path, "news_leads", "lead_id", str(lead_id)):
        status = system.discovery.current_status(signal_id, proof=proof)
        return {
            "source_id": source_id,
            "item_id": native_item_id,
            "signal_id": str(signal_id),
            "lead_id": str(lead_id),
            "phase": status.phase.value,
            "gate_outcome": status.current_gate.request.outcome.value,
            "disposition": (
                None
                if status.current_disposition is None
                else status.current_disposition.request.outcome.value
            ),
            "replayed": True,
        }
    if _has_row(path, "source_definitions", "definition_id", str(definition_id)):
        raise LeadAdmissionError(
            "partial first-boot 3C lineage is present without a Lead"
        )
    (
        definition_id,
        version_id,
        rights_id,
        roles,
        dependencies,
        coverage,
    ) = _retain_source_contract(system, payload=payload, proof=proof)
    item_id = _observed_item_id(definition_id=definition_id, item_key=item_key)
    representation_digest = digest_bytes(
        canonical_json_bytes(
            {
                "item_id": native_item_id,
                "source_id": source_id,
                "url": payload["url"],
            }
        )
    )
    observation = CandidateObservationRef(item_key, representation_digest)
    request_id = _id(CheckRequestId, kind="check-request", semantic=semantic)
    attempt_id = _id(CheckAttemptId, kind="check-attempt", semantic=semantic)
    outcome_id = _id(CheckOutcomeId, kind="check-outcome", semantic=semantic)
    revision_id = _id(SourceRevisionId, kind="revision", semantic=semantic)
    representation_id = _id(
        DiscoveryRepresentationId, kind="representation", semantic=semantic
    )
    occurrence_id = _id(DiscoveryOccurrenceId, kind="occurrence", semantic=semantic)
    transition_id = _id(ObservableTransitionId, kind="transition", semantic=semantic)
    baseline_id = _id(BaselineDecisionId, kind="baseline", semantic=semantic)
    adapter_request_id = _id(AdapterRequestId, kind="adapter-request", semantic=semantic)
    proposal_id = _id(ObservationProposalId, kind="proposal", semantic=semantic)
    adapter_digest = digest_bytes(canonical_json_bytes(payload))
    slot_digest = digest_bytes(source_id.encode("utf-8"))
    key_suffix = item_key.replace("sha256:", "")[:32]

    system.checks.register_request(
        CheckRequestRequest(
            request_id=request_id,
            definition_id=definition_id,
            definition_version_id=version_id,
            trigger=TriggerRef(
                TriggerKind.DELIVERED_INPUT,
                "first-boot-admitted-signal",
                POLICY_VERSION,
            ),
            coverage=coverage,
            rights_decision_id=rights_id,
            rights_policy_version=RIGHTS_POLICY,
            adapter_request_digest=adapter_digest,
            producer_slot_digest=slot_digest,
            baseline_policy=_policy("baseline"),
            revision_policy=_policy("revision"),
            transition_policy=_policy("transition"),
            validator_policy=_policy("validator"),
            purpose="Retain first-boot admitted item as Check lineage.",
            requested_at=now,
            idempotency_key=f"first-boot-check-req-{source_id}-{key_suffix}",
        ),
        proof=proof,
    )
    system.checks.start_attempt(
        CheckAttemptRequest(
            attempt_id=attempt_id,
            request_id=request_id,
            attempt_number=1,
            kind=CheckAttemptKind.PRIMARY,
            prior_attempt_id=None,
            adapter_request_id=adapter_request_id,
            adapter_request_digest=adapter_digest,
            started_at=now,
            idempotency_key=f"first-boot-check-att-{source_id}-{key_suffix}",
        ),
        proof=proof,
    )
    system.checks.record_outcome(
        CheckOutcomeRequest(
            outcome_id=outcome_id,
            request_id=request_id,
            attempt_id=attempt_id,
            proposal_id=proposal_id,
            definition_id=definition_id,
            definition_version_id=version_id,
            kind=CheckOutcomeKind.SUCCESS_CHANGED,
            reason_codes=("OBSERVABLE_CHANGE_CANDIDATES",),
            quarantine=QuarantineDisposition.NONE,
            incomplete=False,
            receipt_digest=adapter_digest,
            capture_digest=adapter_digest,
            parser_result_digest=adapter_digest,
            source_body_digest=representation_digest,
            producer_slot_digest=slot_digest,
            representation_digest=representation_digest,
            validator_digest=None,
            candidate_observations=(observation,),
            observed_items=(observation,),
            completed_at=now,
            idempotency_key=f"first-boot-check-out-{source_id}-{key_suffix}",
        ),
        proof=proof,
    )
    system.sources.register_item(
        SourceItemRequest(
            item_id=item_id,
            definition_id=definition_id,
            definition_version_id=version_id,
            identity_kind=SourceItemIdentityKind.SOURCE_NATIVE,
            identity_policy=_policy("item-identity"),
            source_native_id=native_item_id,
            identity_components=(),
            uncertainties=(),
            idempotency_key=f"first-boot-item-{source_id}-{key_suffix}",
        ),
        proof=proof,
    )
    system.sources.record_revision(
        SourceRevisionRequest(
            revision_id=revision_id,
            item_id=item_id,
            definition_version_id=version_id,
            prior_revision_id=None,
            source_native_revision_token=native_item_id,
            permitted_state_digest=representation_digest,
            revision_policy=_policy("revision"),
            canonicalizer_version="first-boot-canonicalizer-v1",
            source_published_time=SourceTime.unknown(),
            source_updated_time=SourceTime.unknown(),
            observed_at=now,
            idempotency_key=f"first-boot-rev-{source_id}-{key_suffix}",
        ),
        proof=proof,
    )
    system.sources.record_representation(
        DiscoveryRepresentationRequest(
            representation_id=representation_id,
            revision_id=revision_id,
            definition_version_id=version_id,
            adapter_version="first-boot-adapter-v1",
            parser_version="first-boot-parser-v1",
            normalizer_version="first-boot-normalizer-v1",
            extraction_scope_version="first-boot-scope-v1",
            permitted_fields_digest=digest_bytes(b"item_id,source_id,url"),
            representation_digest=representation_digest,
            produced_at=now,
            idempotency_key=f"first-boot-repr-{source_id}-{key_suffix}",
        ),
        proof=proof,
    )
    system.sources.record_occurrence(
        DiscoveryOccurrenceRequest(
            occurrence_id=occurrence_id,
            check_outcome_id=outcome_id,
            revision_id=revision_id,
            representation_id=representation_id,
            definition_version_id=version_id,
            kind=DiscoveryOccurrenceKind.FIRST_OBSERVED,
            observed_at=now,
            receipt_digest=adapter_digest,
            source_asserted_time=SourceTime.unknown(),
            idempotency_key=f"first-boot-occ-{source_id}-{key_suffix}",
        ),
        proof=proof,
    )
    system.checks.decide_baseline(
        BaselineDecisionRequest(
            decision_id=baseline_id,
            definition_id=definition_id,
            definition_version_id=version_id,
            check_request_id=request_id,
            check_outcome_id=outcome_id,
            kind=BaselineDecisionKind.ESTABLISH,
            disposition=BaselineDisposition.MAINTAINED_BASELINE_ONLY,
            observation_model=ObservationModel.MUTABLE_ITEM,
            baseline_policy=_policy("baseline"),
            previous_decision_id=None,
            entries=(
                BaselineManifestEntry(
                    item_key=item_key,
                    disposition=BaselineEntryDisposition.INCLUDED,
                    reason_code="INITIAL_INCLUDED",
                    item_id=item_id,
                    revision_id=revision_id,
                ),
            ),
            source_body_digest=representation_digest,
            producer_slot_digest=slot_digest,
            representation_digest=representation_digest,
            validator_digest=None,
            reason_codes=("BASELINE_DECIDED",),
            decided_at=now,
            idempotency_key=f"first-boot-base-{source_id}-{key_suffix}",
        ),
        proof=proof,
    )
    system.checks.record_transition(
        ObservableTransitionRequest(
            transition_id=transition_id,
            definition_id=definition_id,
            definition_version_id=version_id,
            check_outcome_id=outcome_id,
            item_id=item_id,
            kind=ObservableTransitionKind.FIRST_OBSERVED,
            basis=TransitionBasis.REVISION,
            observation_model=ObservationModel.MUTABLE_ITEM,
            prior_revision_id=None,
            current_revision_id=revision_id,
            representation_id=representation_id,
            related_item_id=None,
            change_facets=(),
            transition_policy=_policy("transition"),
            absence_guard=None,
            agenda_guard=None,
            source_asserted_time=SourceTime.unknown(),
            observed_at=now,
            transition_discriminator="first-observed",
            idempotency_key=f"first-boot-trans-{source_id}-{key_suffix}",
        ),
        proof=proof,
    )
    gate_id = _id(GateDecisionId, kind="gate", semantic=semantic)
    disposition_id = _id(LeadDispositionDecisionId, kind="disposition", semantic=semantic)
    basis = GateBasis(
        identity_integrity=True,
        duplicate_signal_id=None,
        duplicate_rule=None,
        observable_newness=ObservableNewness.GENUINE_TRANSITION,
        time_validity=TimeValidity.CURRENT,
        scope_disposition=ScopeDisposition.ACCEPTED,
        clear_exclusion_rule=None,
        rights_current=True,
        policy_current=True,
        operationally_executable=True,
    )
    outcome = deterministic_gate_outcome(basis)
    if outcome is not GateOutcome.PROMOTED_TO_LEAD:
        raise LeadAdmissionError("first admission gate did not promote")
    transition_ref = ReasonReference("OBSERVABLE_TRANSITION", str(transition_id))
    reason = StructuredReason(
        "CHANGE.GENUINE_TRANSITION",
        ReasonBasisClass.DETERMINISTIC_OBSERVATION,
        (transition_ref,),
        "First observation of this admitted source item.",
    )
    next_action = NextAction(
        NextActionKind.QUEUE_TRIAGE,
        "QUEUE_FOR_TRIAGE",
        instructions="Create one immutable Lead and initial disposition.",
    )
    urgency = UrgencyBasis(
        UrgencyRoute.ROUTINE,
        StructuredReason(
            "TIME.ROUTINE_SOURCE_CHANGE",
            ReasonBasisClass.DETERMINISTIC_OBSERVATION,
            (transition_ref,),
            "First-boot admitted item has no exact urgent or planned deadline.",
        ),
    )
    plan = SignalLeadAdmissionRequest(
        signal=DiscoverySignalRequest(
            signal_id=signal_id,
            definition_id=definition_id,
            definition_version_id=version_id,
            item_id=item_id,
            revision_id=revision_id,
            representation_id=representation_id,
            check_outcome_id=outcome_id,
            occurrence_id=occurrence_id,
            transition_id=transition_id,
            purpose="SOURCE_TRANSITION",
            discriminator="primary",
            admission_policy=_policy("signal-admission"),
            incomplete=False,
            operational_finding_ids=(),
            admitted_at=now,
            idempotency_key=f"first-boot-signal-{source_id}-{key_suffix}",
        ),
        gate=GateDecisionRequest(
            decision_id=gate_id,
            signal_id=signal_id,
            decision_ordinal=1,
            previous_decision_id=None,
            evaluated_definition_version_id=version_id,
            coverage=coverage,
            rights_decision_id=rights_id,
            rights_policy_version=RIGHTS_POLICY,
            signal_admission_policy=_policy("signal-admission"),
            gate_policy=_policy("gate"),
            duplicate_policy=_policy("duplicate"),
            newness_policy=_policy("newness"),
            time_validity_policy=_policy("time-validity"),
            exclusion_policy=_policy("exclusion"),
            basis=basis,
            outcome=outcome,
            terminality=DecisionTerminality.TERMINAL_EXACT_VERSION,
            primary_reason=reason,
            supporting_reasons=(),
            reason_taxonomy_version="first-boot-reasons-v1",
            outcome_taxonomy_version="first-boot-outcomes-v1",
            next_action=next_action,
            decided_at=now,
            idempotency_key=f"first-boot-gate-{source_id}-{key_suffix}",
        ),
        lead=NewsLeadRequest(
            lead_id=lead_id,
            signal_id=signal_id,
            promoting_gate_decision_id=gate_id,
            definition_id=definition_id,
            definition_version_id=version_id,
            item_id=item_id,
            revision_id=revision_id,
            representation_id=representation_id,
            occurrence_id=occurrence_id,
            transition_id=transition_id,
            transition_kind=ObservableTransitionKind.FIRST_OBSERVED,
            coverage=coverage,
            source_roles=roles,
            portfolio_functions=(PortfolioFunction.ANCHOR,),
            source_dependencies=dependencies,
            incompleteness_warnings=(),
            urgency=urgency,
            lead_policy=_policy("lead"),
            reason_taxonomy_version="first-boot-reasons-v1",
            outcome_taxonomy_version="first-boot-outcomes-v1",
            created_at=now,
            idempotency_key=f"first-boot-lead-{source_id}-{key_suffix}",
        ),
        initial_disposition=LeadDispositionDecisionRequest(
            decision_id=disposition_id,
            lead_id=lead_id,
            gate_decision_id=gate_id,
            decision_ordinal=1,
            previous_decision_id=None,
            outcome=LeadDispositionOutcome.QUEUED_FOR_TRIAGE,
            terminality=DecisionTerminality.PENDING_CONDITION,
            primary_reason=StructuredReason(
                "CHANGE.LEAD_CREATED",
                ReasonBasisClass.DETERMINISTIC_OBSERVATION,
                (transition_ref,),
                "Promoted first-boot Signal created one immutable Lead.",
            ),
            supporting_reasons=(),
            watch_condition_id=None,
            next_action=NextAction(
                NextActionKind.QUEUE_TRIAGE,
                "QUEUE_FOR_TRIAGE",
                instructions="Queue without creating a Triage Work Item.",
            ),
            urgency_route=urgency,
            disposition_policy=_policy("lead-disposition"),
            reason_taxonomy_version="first-boot-reasons-v1",
            outcome_taxonomy_version="first-boot-outcomes-v1",
            decided_at=now,
            idempotency_key=f"first-boot-disp-{source_id}-{key_suffix}",
        ),
    )
    result = system.discovery.admit_signal_to_lead(plan, proof=proof)
    status = system.discovery.current_status(signal_id, proof=proof)
    return {
        "source_id": source_id,
        "item_id": native_item_id,
        "signal_id": str(signal_id),
        "lead_id": str(lead_id),
        "phase": status.phase.value,
        "gate_outcome": result.gate.request.outcome.value,
        "disposition": (
            None
            if result.initial_disposition is None
            else result.initial_disposition.request.outcome.value
        ),
        "replayed": False,
    }


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


def record_first_boot_leads(
    path: Path,
    *,
    feed_loader: FeedLoader | None = None,
    clock: Callable[[], UtcTimestamp] | None = None,
) -> dict[str, Any]:
    from newsroom.graphiti_adapter.models import REAL_GRAPHITI_RUNTIME_ENABLED
    from newsroom.host_store import open_host_discovery_system

    if REAL_GRAPHITI_RUNTIME_ENABLED:
        raise LeadAdmissionError("REAL_GRAPHITI_RUNTIME_ENABLED must stay False")
    loader = feed_loader or _default_feed_loader
    admissions = read_ingest_admissions(path)
    promoted: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for skip in read_ingest_skips(path):
        payload = skip["payload"]
        skipped.append(
            {
                "source_id": payload.get("source_id"),
                "item_id": payload.get("item_id"),
                "reason": payload.get("reason", "uk-10-empty-feed-skip"),
            }
        )
    proof = _proof()
    now = (clock or UtcTimestamp.now)()
    system = open_host_discovery_system(path, clock=lambda: now)
    try:
        for admission in admissions:
            payload = admission["payload"]
            source_id = payload["source_id"]
            item_id = payload.get("item_id")
            if source_id in SKIP_SOURCE_IDS:
                skipped.append(
                    {
                        "source_id": source_id,
                        "item_id": item_id,
                        "reason": "uk-10-empty-feed-skip",
                    }
                )
                continue
            if source_id not in _SOURCE_PROFILES:
                held.append(
                    {
                        "source_id": source_id,
                        "item_id": item_id,
                        "reason": "source-not-in-first-boot-cover",
                    }
                )
                continue
            reason = _hold_reason(admission, feed_loader=loader)
            if reason is not None:
                held.append(
                    {"source_id": source_id, "item_id": item_id, "reason": reason}
                )
                continue
            promoted.append(
                _admit_one(
                    system,
                    path=path,
                    admission=admission,
                    proof=proof,
                    now=now,
                )
            )
    finally:
        system.close()
    news_leads = _count_table(path, "news_leads")
    return {
        "ok": news_leads > 0,
        "auto_publish": False,
        "discord": False,
        "public_adapter": False,
        "news_leads": news_leads,
        "promoted": promoted,
        "held": held,
        "skipped": skipped,
        "extraction_runs": _count_table(path, "extraction_runs"),
        "editorial_relation_decisions": _count_table(
            path, "editorial_relation_decisions"
        ),
        "graphiti_runtime_enabled": REAL_GRAPHITI_RUNTIME_ENABLED,
    }


__all__ = [
    "INGEST_COMMAND_TYPES",
    "LeadAdmissionError",
    "SKIP_SOURCE_IDS",
    "item_identity_url",
    "read_ingest_admissions",
    "read_ingest_skips",
    "record_first_boot_leads",
    "resolve_atom_tag",
]
