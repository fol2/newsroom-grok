"""Agent-turn Publication Decision for one bundle built from the first Discovery Signal."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from newsroom.authority import (
    AggregateId,
    AuthenticationProof,
    CommandDefinition,
    InlinePayload,
    PayloadGoldenVector,
    PayloadMode,
    PayloadSchemaContract,
    SemanticCommand,
    TrustScope,
    canonical_json_bytes,
    digest_canonical,
    validate_sha256_digest,
)
from newsroom.discovery_ingest import (
    EVENT_TYPE as SIGNAL_EVENT_TYPE,
    SAMPLE_PAYLOAD as SIGNAL_SAMPLE,
)
from newsroom.envelope_grant import CONTROLLER_ID


CONTROLLER_CREDENTIAL = "agent-turn-controller"
COMMAND_TYPE = "publication.decision.mint"
EVENT_TYPE = "publication.decision.authorised"
AGGREGATE_TYPE = "publication.decision"
IDEMPOTENCY_KEY = "first-production-publication-decision-v1"
DECISION_AGGREGATE_ID = AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa30")
PAYLOAD_KEYS = (
    "authorising",
    "auto_publish",
    "bundle_digest",
    "controller",
    "discord",
    "hermes_publication_admission",
    "public_adapter",
)


def bundle_digest_for_signal(payload: dict[str, Any]) -> str:
    return digest_canonical(
        {
            "adapter": payload["adapter"],
            "item_id": payload["item_id"],
            "source_id": payload["source_id"],
            "url": payload["url"],
        }
    )


SAMPLE_PAYLOAD: dict[str, Any] = {
    "authorising": True,
    "auto_publish": False,
    "bundle_digest": bundle_digest_for_signal(SIGNAL_SAMPLE),
    "controller": CONTROLLER_ID,
    "discord": False,
    "hermes_publication_admission": False,
    "public_adapter": False,
}


def canonicalize_decision_payload(value: object) -> bytes:
    if not isinstance(value, dict):
        raise ValueError("publication decision payload must be an object")
    if set(value) != set(PAYLOAD_KEYS):
        raise ValueError("publication decision payload keys are exact")
    if value["controller"] != CONTROLLER_ID:
        raise ValueError("controller must be the Grok Bot agent-turn controller")
    if value["authorising"] is not True:
        raise ValueError("decision must be authorising")
    if value["auto_publish"] is not False:
        raise ValueError("AUTO_PUBLISH stays off")
    if value["discord"] is not False:
        raise ValueError("Discord stays off")
    if value["public_adapter"] is not False:
        raise ValueError("public adapters stay off")
    if value["hermes_publication_admission"] is not False:
        raise ValueError("Hermes Publication Admission is out of scope")
    digest = value["bundle_digest"]
    if not isinstance(digest, str):
        raise ValueError("bundle digest must be canonical text")
    validate_sha256_digest(digest, field="bundle_digest")
    return canonical_json_bytes(
        {
            "authorising": True,
            "auto_publish": False,
            "bundle_digest": digest,
            "controller": CONTROLLER_ID,
            "discord": False,
            "hermes_publication_admission": False,
            "public_adapter": False,
        }
    )


def decision_payload_contract() -> PayloadSchemaContract:
    expected = canonicalize_decision_payload(SAMPLE_PAYLOAD)
    return PayloadSchemaContract(
        schema_version="publication_decision_mint_v1",
        payload_mode=PayloadMode.INLINE,
        contract_version="publication-decision-mint-contract-v1",
        canonicalizer_implementation_version="publication-decision-mint-v1",
        canonicalizer=canonicalize_decision_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="agent_turn_first_bundle_authorising",
                input_identity="grok-bot-first-publication-decision-v1",
                value=SAMPLE_PAYLOAD,
                expected_bytes=expected,
            ),
        ),
    )


def decision_command_definition(
    contract: PayloadSchemaContract | None = None,
) -> CommandDefinition:
    selected = contract or decision_payload_contract()
    return CommandDefinition(
        command_type=COMMAND_TYPE,
        definition_version="v1",
        aggregate_type=AGGREGATE_TYPE,
        event_type=EVENT_TYPE,
        event_schema_version=1,
        payload_mode=PayloadMode.INLINE,
        payload_schema_version=selected.schema_version,
        payload_schema_contract_version=selected.contract_version,
        payload_schema_contract_digest=selected.contract_digest,
        payload_canonicalizer_version=selected.canonicalizer_implementation_version,
        trust_scope=TrustScope.ADMITTED,
        security_scope="authority.publication",
        retention_scope="authority.publication",
        required_scope="authority.publication.decide",
        max_inline_bytes=4096,
    )


def load_first_discovery_signal(path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        row = conn.execute(
            "SELECT p.payload_bytes FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type=? "
            "ORDER BY e.ledger_seq ASC LIMIT 1",
            (SIGNAL_EVENT_TYPE,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise ValueError(
            "discovery signal missing; run newsroom-first-boot ingest-signal first"
        )
    payload = json.loads(bytes(row[0]))
    if not isinstance(payload, dict):
        raise ValueError("discovery signal payload must be an object")
    return payload


def record_publication_decision(
    path: Path, *, signal_payload: dict[str, Any]
) -> dict[str, Any]:
    from newsroom.host_store import open_host_store

    payload = {
        "authorising": True,
        "auto_publish": False,
        "bundle_digest": bundle_digest_for_signal(signal_payload),
        "controller": CONTROLLER_ID,
        "discord": False,
        "hermes_publication_admission": False,
        "public_adapter": False,
    }
    system = open_host_store(path)
    try:
        system.commands.execute(
            SemanticCommand(
                command_type=COMMAND_TYPE,
                aggregate_id=DECISION_AGGREGATE_ID,
                expected_aggregate_version=0,
                payload=InlinePayload(payload),
                idempotency_key=IDEMPOTENCY_KEY,
            ),
            proof=AuthenticationProof(
                method="STATIC_TOKEN",
                credential=CONTROLLER_CREDENTIAL,
            ),
        )
    finally:
        system.close()
    return payload
