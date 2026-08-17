"""Owner-signed AUTO_PUBLISH grant: AUTO-010, ledger-only, owner only."""

from __future__ import annotations

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
)


OWNER_PRINCIPAL = "owner.newsroom"
OWNER_CREDENTIAL = "owner-signed"
COMMAND_TYPE = "auto_publish.grant"
EVENT_TYPE = "auto_publish.granted"
AGGREGATE_TYPE = "auto_publish.grant"
IDEMPOTENCY_KEY = "owner-signed-auto-publish-grant-v1"
GRANT_AGGREGATE_ID = AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa41")
SEMANTIC = "AUTO-010"
FIRST_TARGET = "host.authority.ledger"
OPERATION = "record"

GRANT_PAYLOAD: dict[str, Any] = {
    "auto_publish": False,
    "controller_may_arm": False,
    "discord": False,
    "emergency_stop_retained": True,
    "every_applicable_gate_passed": True,
    "fail_closed_gates_retained": True,
    "first_target": FIRST_TARGET,
    "hermes_publication_admission": False,
    "hold": False,
    "operation": OPERATION,
    "prohibited_effect_retained": True,
    "public_adapter": False,
    "semantic": SEMANTIC,
    "x_as_publisher": False,
}


def canonicalize_grant_payload(value: object) -> bytes:
    if not isinstance(value, dict):
        raise ValueError("AUTO_PUBLISH grant payload must be an object")
    if set(value) != set(GRANT_PAYLOAD):
        raise ValueError("AUTO_PUBLISH grant payload keys are exact")
    if value["semantic"] != SEMANTIC:
        raise ValueError("semantic must be AUTO-010")
    if value["every_applicable_gate_passed"] is not True:
        raise ValueError("AUTO-010 requires every applicable gate passed")
    if value["hold"] is not False:
        raise ValueError("AUTO-010 requires no hold")
    if value["first_target"] != FIRST_TARGET:
        raise ValueError("first target stays host.authority.ledger")
    if value["operation"] != OPERATION:
        raise ValueError("operation stays record")
    if value["controller_may_arm"] is not False:
        raise ValueError("agent-turn controller may not arm")
    if value["auto_publish"] is not False:
        raise ValueError("public AUTO_PUBLISH stays off")
    if value["public_adapter"] is not False:
        raise ValueError("public adapters stay off")
    if value["discord"] is not False:
        raise ValueError("Discord stays off")
    if value["x_as_publisher"] is not False:
        raise ValueError("X-as-publisher stays off")
    if value["emergency_stop_retained"] is not True:
        raise ValueError("emergency stop must remain")
    if value["fail_closed_gates_retained"] is not True:
        raise ValueError("fail-closed gates must remain")
    if value["prohibited_effect_retained"] is not True:
        raise ValueError("9A1 ProhibitedEffect must remain")
    if value["hermes_publication_admission"] is not False:
        raise ValueError("Hermes Publication Admission is dead")
    return canonical_json_bytes(
        {
            "auto_publish": False,
            "controller_may_arm": False,
            "discord": False,
            "emergency_stop_retained": True,
            "every_applicable_gate_passed": True,
            "fail_closed_gates_retained": True,
            "first_target": FIRST_TARGET,
            "hermes_publication_admission": False,
            "hold": False,
            "operation": OPERATION,
            "prohibited_effect_retained": True,
            "public_adapter": False,
            "semantic": SEMANTIC,
            "x_as_publisher": False,
        }
    )


def auto_publish_payload_contract() -> PayloadSchemaContract:
    expected = canonicalize_grant_payload(GRANT_PAYLOAD)
    return PayloadSchemaContract(
        schema_version="auto_publish_grant_v1",
        payload_mode=PayloadMode.INLINE,
        contract_version="auto-publish-grant-contract-v1",
        canonicalizer_implementation_version="auto-publish-grant-v1",
        canonicalizer=canonicalize_grant_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="owner_signed_auto_010_grant",
                input_identity="grok-bot-auto-publish-grant-v1",
                value=GRANT_PAYLOAD,
                expected_bytes=expected,
            ),
        ),
    )


def auto_publish_command_definition(
    contract: PayloadSchemaContract | None = None,
) -> CommandDefinition:
    selected = contract or auto_publish_payload_contract()
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
        security_scope="authority.envelope",
        retention_scope="authority.envelope",
        required_scope="authority.autopublish.grant",
        max_inline_bytes=4096,
    )


def record_auto_publish_grant(path: Path) -> None:
    from newsroom.host_store import open_host_store

    system = open_host_store(path)
    try:
        system.commands.execute(
            SemanticCommand(
                command_type=COMMAND_TYPE,
                aggregate_id=GRANT_AGGREGATE_ID,
                expected_aggregate_version=0,
                payload=InlinePayload(dict(GRANT_PAYLOAD)),
                idempotency_key=IDEMPOTENCY_KEY,
            ),
            proof=AuthenticationProof(
                method="STATIC_TOKEN",
                credential=OWNER_CREDENTIAL,
            ),
        )
    finally:
        system.close()
