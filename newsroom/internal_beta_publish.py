"""Host-dispatched publish Target Operation to internal.beta.origin."""

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
    validate_sha256_digest,
)
from newsroom.internal_beta_grant import (
    BUNDLE_DIGEST,
    EVENT_TYPE as GRANT_EVENT_TYPE,
    OPERATION,
    TARGET,
)
from newsroom.target_operation import load_first_authorising_decision


HOST_CREDENTIAL = "host-process"
DISPATCHER_ID = "host.newsroom"
COMMAND_TYPE = "internal_beta.publish.dispatch"
EVENT_TYPE = "target.operation.dispatched"
AGGREGATE_TYPE = "internal_beta.publish"
IDEMPOTENCY_KEY = "internal-beta-publish-target-operation-v1"
OPERATION_AGGREGATE_ID = AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa53")
PAYLOAD_KEYS = (
    "auto_publish",
    "bundle_digest",
    "discord",
    "dispatcher",
    "operation",
    "public_adapter",
    "target",
    "x_as_publisher",
)

SAMPLE_PAYLOAD: dict[str, Any] = {
    "auto_publish": False,
    "bundle_digest": BUNDLE_DIGEST,
    "discord": False,
    "dispatcher": DISPATCHER_ID,
    "operation": OPERATION,
    "public_adapter": False,
    "target": TARGET,
    "x_as_publisher": False,
}


def canonicalize_publish_payload(value: object) -> bytes:
    if not isinstance(value, dict):
        raise ValueError("internal beta publish payload must be an object")
    if set(value) != set(PAYLOAD_KEYS):
        raise ValueError("internal beta publish payload keys are exact")
    if value["dispatcher"] != DISPATCHER_ID:
        raise ValueError("dispatcher must be the host process")
    if value["target"] != TARGET:
        raise ValueError("target stays internal.beta.origin")
    if value["operation"] != OPERATION:
        raise ValueError("operation stays publish")
    if value["auto_publish"] is not False:
        raise ValueError("AUTO_PUBLISH stays off")
    if value["discord"] is not False:
        raise ValueError("Discord stays off")
    if value["public_adapter"] is not False:
        raise ValueError("public adapters stay off")
    if value["x_as_publisher"] is not False:
        raise ValueError("X-as-publisher stays off")
    digest = value["bundle_digest"]
    if not isinstance(digest, str):
        raise ValueError("bundle digest must be canonical text")
    validate_sha256_digest(digest, field="bundle_digest")
    if digest != BUNDLE_DIGEST:
        raise ValueError("bundle digest stays the authorised HK-01 bundle")
    return canonical_json_bytes(
        {
            "auto_publish": False,
            "bundle_digest": BUNDLE_DIGEST,
            "discord": False,
            "dispatcher": DISPATCHER_ID,
            "operation": OPERATION,
            "public_adapter": False,
            "target": TARGET,
            "x_as_publisher": False,
        }
    )


def publish_payload_contract() -> PayloadSchemaContract:
    expected = canonicalize_publish_payload(SAMPLE_PAYLOAD)
    return PayloadSchemaContract(
        schema_version="internal_beta_publish_dispatch_v1",
        payload_mode=PayloadMode.INLINE,
        contract_version="internal-beta-publish-dispatch-contract-v1",
        canonicalizer_implementation_version="internal-beta-publish-dispatch-v1",
        canonicalizer=canonicalize_publish_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="host_internal_beta_publish_target_operation",
                input_identity="grok-bot-internal-beta-publish-v1",
                value=SAMPLE_PAYLOAD,
                expected_bytes=expected,
            ),
        ),
    )


def publish_command_definition(
    contract: PayloadSchemaContract | None = None,
) -> CommandDefinition:
    selected = contract or publish_payload_contract()
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
        required_scope="authority.publication.dispatch",
        max_inline_bytes=4096,
    )


def load_internal_beta_grant(path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        row = conn.execute(
            "SELECT p.payload_bytes FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type=? "
            "ORDER BY e.ledger_seq ASC LIMIT 1",
            (GRANT_EVENT_TYPE,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise ValueError(
            "internal_beta grant missing; run newsroom-first-boot grant-internal-beta first"
        )
    payload = json.loads(bytes(row[0]))
    if not isinstance(payload, dict):
        raise ValueError("internal_beta grant payload must be an object")
    if payload.get("target") != TARGET:
        raise ValueError("internal_beta grant target stays internal.beta.origin")
    if payload.get("operation") != OPERATION:
        raise ValueError("internal_beta grant operation stays publish")
    digest = payload.get("bundle_digest")
    if not isinstance(digest, str):
        raise ValueError("internal_beta grant must name a bundle digest")
    validate_sha256_digest(digest, field="bundle_digest")
    if digest != BUNDLE_DIGEST:
        raise ValueError("internal_beta grant digest stays the authorised HK-01 bundle")
    if payload.get("controller_may_arm") is not False:
        raise ValueError("agent-turn controller may not arm")
    if payload.get("auto_publish") is not False:
        raise ValueError("public AUTO_PUBLISH stays off")
    if payload.get("public_adapter") is not False:
        raise ValueError("public adapters stay off")
    if payload.get("discord") is not False:
        raise ValueError("Discord stays off")
    if payload.get("x_as_publisher") is not False:
        raise ValueError("X-as-publisher stays off")
    return payload


def record_internal_beta_publish(path: Path) -> dict[str, Any]:
    from newsroom.host_store import open_host_store

    grant = load_internal_beta_grant(path)
    load_first_authorising_decision(path)
    payload = {
        "auto_publish": False,
        "bundle_digest": str(grant["bundle_digest"]),
        "discord": False,
        "dispatcher": DISPATCHER_ID,
        "operation": OPERATION,
        "public_adapter": False,
        "target": TARGET,
        "x_as_publisher": False,
    }
    system = open_host_store(path)
    try:
        system.commands.execute(
            SemanticCommand(
                command_type=COMMAND_TYPE,
                aggregate_id=OPERATION_AGGREGATE_ID,
                expected_aggregate_version=0,
                payload=InlinePayload(payload),
                idempotency_key=IDEMPOTENCY_KEY,
            ),
            proof=AuthenticationProof(
                method="STATIC_TOKEN",
                credential=HOST_CREDENTIAL,
            ),
        )
    finally:
        system.close()
    return payload
