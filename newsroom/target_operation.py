"""Host-dispatched Target Operation for one authorised Publication Bundle."""

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
from newsroom.publication_decision import (
    EVENT_TYPE as DECISION_EVENT_TYPE,
    SAMPLE_PAYLOAD as DECISION_SAMPLE,
)


HOST_CREDENTIAL = "host-process"
DISPATCHER_ID = "host.newsroom"
COMMAND_TYPE = "target.operation.dispatch"
EVENT_TYPE = "target.operation.dispatched"
AGGREGATE_TYPE = "target.operation"
IDEMPOTENCY_KEY = "first-production-target-operation-v1"
OPERATION_AGGREGATE_ID = AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa31")
LEDGER_TARGET = "host.authority.ledger"
OPERATION = "record"
PAYLOAD_KEYS = (
    "auto_publish",
    "bundle_digest",
    "discord",
    "dispatcher",
    "operation",
    "public_adapter",
    "target",
)


SAMPLE_PAYLOAD: dict[str, Any] = {
    "auto_publish": False,
    "bundle_digest": DECISION_SAMPLE["bundle_digest"],
    "discord": False,
    "dispatcher": DISPATCHER_ID,
    "operation": OPERATION,
    "public_adapter": False,
    "target": LEDGER_TARGET,
}


def canonicalize_operation_payload(value: object) -> bytes:
    if not isinstance(value, dict):
        raise ValueError("target operation payload must be an object")
    if set(value) != set(PAYLOAD_KEYS):
        raise ValueError("target operation payload keys are exact")
    if value["dispatcher"] != DISPATCHER_ID:
        raise ValueError("dispatcher must be the host process")
    if value["target"] != LEDGER_TARGET:
        raise ValueError("controlled target is the host authority ledger")
    if value["operation"] != OPERATION:
        raise ValueError("operation must be a ledger record")
    if value["auto_publish"] is not False:
        raise ValueError("AUTO_PUBLISH stays off")
    if value["discord"] is not False:
        raise ValueError("Discord stays off")
    if value["public_adapter"] is not False:
        raise ValueError("public adapters stay off")
    digest = value["bundle_digest"]
    if not isinstance(digest, str):
        raise ValueError("bundle digest must be canonical text")
    validate_sha256_digest(digest, field="bundle_digest")
    return canonical_json_bytes(
        {
            "auto_publish": False,
            "bundle_digest": digest,
            "discord": False,
            "dispatcher": DISPATCHER_ID,
            "operation": OPERATION,
            "public_adapter": False,
            "target": LEDGER_TARGET,
        }
    )


def operation_payload_contract() -> PayloadSchemaContract:
    expected = canonicalize_operation_payload(SAMPLE_PAYLOAD)
    return PayloadSchemaContract(
        schema_version="target_operation_dispatch_v1",
        payload_mode=PayloadMode.INLINE,
        contract_version="target-operation-dispatch-contract-v1",
        canonicalizer_implementation_version="target-operation-dispatch-v1",
        canonicalizer=canonicalize_operation_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="host_ledger_first_target_operation",
                input_identity="grok-bot-first-target-operation-v1",
                value=SAMPLE_PAYLOAD,
                expected_bytes=expected,
            ),
        ),
    )


def operation_command_definition(
    contract: PayloadSchemaContract | None = None,
) -> CommandDefinition:
    selected = contract or operation_payload_contract()
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


def load_first_authorising_decision(path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        row = conn.execute(
            "SELECT p.payload_bytes FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type=? "
            "ORDER BY e.ledger_seq ASC LIMIT 1",
            (DECISION_EVENT_TYPE,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise ValueError(
            "publication decision missing; run newsroom-first-boot mint-decision first"
        )
    payload = json.loads(bytes(row[0]))
    if not isinstance(payload, dict):
        raise ValueError("publication decision payload must be an object")
    if payload.get("authorising") is not True:
        raise ValueError("publication decision must be authorising")
    digest = payload.get("bundle_digest")
    if not isinstance(digest, str):
        raise ValueError("publication decision must name a bundle digest")
    validate_sha256_digest(digest, field="bundle_digest")
    return payload


def record_target_operation(path: Path, *, bundle_digest: str) -> dict[str, Any]:
    from newsroom.host_store import open_host_store

    payload = {
        "auto_publish": False,
        "bundle_digest": bundle_digest,
        "discord": False,
        "dispatcher": DISPATCHER_ID,
        "operation": OPERATION,
        "public_adapter": False,
        "target": LEDGER_TARGET,
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
