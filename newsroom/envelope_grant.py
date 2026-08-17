"""Owner-signed Autonomy Envelope grant for the Grok Bot agent-turn controller."""

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


CONTROLLER_ID = "grok_bot.agent_turn_controller"
OWNER_PRINCIPAL = "owner.newsroom"
OWNER_CREDENTIAL = "owner-signed"
COMMAND_TYPE = "envelope.grant"
EVENT_TYPE = "autonomy.envelope.granted"
AGGREGATE_TYPE = "autonomy.envelope"
IDEMPOTENCY_KEY = "owner-signed-envelope-grant-v1"
ENVELOPE_AGGREGATE_ID = AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa28")
ALLOWED_ACTS = (
    "planning",
    "publication_decisions",
    "gated_x_search",
    "official_source_definition_rss",
)

GRANT_PAYLOAD: dict[str, Any] = {
    "allowed_acts": list(ALLOWED_ACTS),
    "auto_publish": False,
    "controller": CONTROLLER_ID,
    "emergency_stop_retained": True,
    "fail_closed_gates_retained": True,
    "hermes_control_plane": False,
    "public_adapter": False,
}


def canonicalize_grant_payload(value: object) -> bytes:
    if not isinstance(value, dict):
        raise ValueError("envelope grant payload must be an object")
    if set(value) != set(GRANT_PAYLOAD):
        raise ValueError("envelope grant payload keys are exact")
    if value["controller"] != CONTROLLER_ID:
        raise ValueError("controller must be the Grok Bot agent-turn controller")
    acts = value["allowed_acts"]
    if not isinstance(acts, list) or set(acts) != set(ALLOWED_ACTS) or len(acts) != len(ALLOWED_ACTS):
        raise ValueError("allowed acts are exact")
    if value["auto_publish"] is not False:
        raise ValueError("AUTO_PUBLISH stays off")
    if value["hermes_control_plane"] is not False:
        raise ValueError("Hermes Control Plane is out of scope")
    if value["public_adapter"] is not False:
        raise ValueError("public adapters stay off")
    if value["emergency_stop_retained"] is not True:
        raise ValueError("emergency stop must remain")
    if value["fail_closed_gates_retained"] is not True:
        raise ValueError("fail-closed gates must remain")
    return canonical_json_bytes(value)


def envelope_payload_contract() -> PayloadSchemaContract:
    expected = canonicalize_grant_payload(GRANT_PAYLOAD)
    return PayloadSchemaContract(
        schema_version="autonomy_envelope_grant_v1",
        payload_mode=PayloadMode.INLINE,
        contract_version="autonomy-envelope-grant-contract-v1",
        canonicalizer_implementation_version="autonomy-envelope-grant-v1",
        canonicalizer=canonicalize_grant_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="owner_signed_grok_bot_grant",
                input_identity="grok-bot-envelope-v1",
                value=GRANT_PAYLOAD,
                expected_bytes=expected,
            ),
        ),
    )


def envelope_command_definition(
    contract: PayloadSchemaContract | None = None,
) -> CommandDefinition:
    selected = contract or envelope_payload_contract()
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
        required_scope="authority.envelope.grant",
        max_inline_bytes=4096,
    )


def record_envelope_grant(path: Path) -> None:
    from newsroom.host_store import open_host_store

    payload = {
        "allowed_acts": list(ALLOWED_ACTS),
        "auto_publish": False,
        "controller": CONTROLLER_ID,
        "emergency_stop_retained": True,
        "fail_closed_gates_retained": True,
        "hermes_control_plane": False,
        "public_adapter": False,
    }
    system = open_host_store(path)
    try:
        system.commands.execute(
            SemanticCommand(
                command_type=COMMAND_TYPE,
                aggregate_id=ENVELOPE_AGGREGATE_ID,
                expected_aggregate_version=0,
                payload=InlinePayload(payload),
                idempotency_key=IDEMPOTENCY_KEY,
            ),
            proof=AuthenticationProof(
                method="STATIC_TOKEN",
                credential=OWNER_CREDENTIAL,
            ),
        )
    finally:
        system.close()
