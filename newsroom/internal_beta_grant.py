"""Owner-signed internal_beta.granted grant: HK-01 to internal.beta.origin, owner only."""

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
    validate_sha256_digest,
)


OWNER_PRINCIPAL = "owner.newsroom"
OWNER_CREDENTIAL = "owner-signed"
COMMAND_TYPE = "internal_beta.grant"
EVENT_TYPE = "internal_beta.granted"
AGGREGATE_TYPE = "internal_beta.grant"
IDEMPOTENCY_KEY = "owner-signed-internal-beta-grant-v1"
GRANT_AGGREGATE_ID = AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa52")
SOURCE_ID = "HK-01"
BUNDLE_DIGEST = (
    "sha256:c487ece7149f0fcf7afa6808f717f2896d5e89c932d7c08519883b7ae09b1b94"
)
TARGET = "internal.beta.origin"
OPERATION = "publish"

GRANT_PAYLOAD: dict[str, Any] = {
    "auto_publish": False,
    "bundle_digest": BUNDLE_DIGEST,
    "controller_may_arm": False,
    "discord": False,
    "emergency_stop_retained": True,
    "fail_closed_gates_retained": True,
    "hermes_publication_admission": False,
    "operation": OPERATION,
    "prohibited_effect_retained": True,
    "public_adapter": False,
    "source_id": SOURCE_ID,
    "target": TARGET,
    "x_as_publisher": False,
}


def canonicalize_grant_payload(value: object) -> bytes:
    if not isinstance(value, dict):
        raise ValueError("internal_beta grant payload must be an object")
    if set(value) != set(GRANT_PAYLOAD):
        raise ValueError("internal_beta grant payload keys are exact")
    if value["source_id"] != SOURCE_ID:
        raise ValueError("source_id stays HK-01")
    digest = value["bundle_digest"]
    if not isinstance(digest, str):
        raise ValueError("bundle digest must be canonical text")
    validate_sha256_digest(digest, field="bundle_digest")
    if digest != BUNDLE_DIGEST:
        raise ValueError("bundle digest stays the authorised HK-01 bundle")
    if value["target"] != TARGET:
        raise ValueError("target stays internal.beta.origin")
    if value["operation"] != OPERATION:
        raise ValueError("operation stays publish")
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
            "bundle_digest": BUNDLE_DIGEST,
            "controller_may_arm": False,
            "discord": False,
            "emergency_stop_retained": True,
            "fail_closed_gates_retained": True,
            "hermes_publication_admission": False,
            "operation": OPERATION,
            "prohibited_effect_retained": True,
            "public_adapter": False,
            "source_id": SOURCE_ID,
            "target": TARGET,
            "x_as_publisher": False,
        }
    )


def internal_beta_payload_contract() -> PayloadSchemaContract:
    expected = canonicalize_grant_payload(GRANT_PAYLOAD)
    return PayloadSchemaContract(
        schema_version="internal_beta_grant_v1",
        payload_mode=PayloadMode.INLINE,
        contract_version="internal-beta-grant-contract-v1",
        canonicalizer_implementation_version="internal-beta-grant-v1",
        canonicalizer=canonicalize_grant_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="owner_signed_internal_beta_grant",
                input_identity="grok-bot-internal-beta-grant-v1",
                value=GRANT_PAYLOAD,
                expected_bytes=expected,
            ),
        ),
    )


def internal_beta_command_definition(
    contract: PayloadSchemaContract | None = None,
) -> CommandDefinition:
    selected = contract or internal_beta_payload_contract()
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
        required_scope="authority.internal_beta.grant",
        max_inline_bytes=4096,
    )


def record_internal_beta_grant(path: Path) -> None:
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
