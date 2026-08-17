"""Shared host authority store: hold, envelope grant, ingest, mint, dispatch."""

from __future__ import annotations

from pathlib import Path

from newsroom.authority import (
    CommandDefinition,
    CommandRegistry,
    EventReadPolicy,
    MetadataClass,
    PayloadGoldenVector,
    PayloadMode,
    PayloadSchemaContract,
    PayloadSchemaRegistry,
    StaticAuthenticator,
    StaticAuthorizer,
    StaticPrincipal,
    TrustScope,
    open_authority_event_system,
)
from newsroom.discovery_ingest import (
    signal_command_definition,
    signal_payload_contract,
    skip_command_definition,
    skip_payload_contract,
)
from newsroom.auto_publish_grant import (
    auto_publish_command_definition,
    auto_publish_payload_contract,
)
from newsroom.internal_beta_grant import (
    internal_beta_command_definition,
    internal_beta_payload_contract,
)
from newsroom.envelope_grant import (
    CONTROLLER_ID,
    OWNER_CREDENTIAL,
    OWNER_PRINCIPAL,
    envelope_command_definition,
    envelope_payload_contract,
)
from newsroom.publication_decision import (
    CONTROLLER_CREDENTIAL,
    decision_command_definition,
    decision_payload_contract,
)
from newsroom.internal_beta_publish import (
    publish_command_definition,
    publish_payload_contract,
)
from newsroom.target_operation import (
    operation_command_definition,
    operation_payload_contract,
)


def canonicalize_hold_payload(value: object) -> bytes:
    if value is None:
        return b""
    raise ValueError("host hold accepts no payload")


def hold_payload_contract() -> PayloadSchemaContract:
    return PayloadSchemaContract(
        schema_version="host_hold_v1",
        payload_mode=PayloadMode.NO_PAYLOAD,
        contract_version="host-hold-contract-v1",
        canonicalizer_implementation_version="host-hold-none-v1",
        canonicalizer=canonicalize_hold_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="empty",
                input_identity="none-v1",
                value=None,
                expected_bytes=b"",
            ),
        ),
    )


def hold_command_definition(
    contract: PayloadSchemaContract | None = None,
) -> CommandDefinition:
    selected = contract or hold_payload_contract()
    return CommandDefinition(
        command_type="host.hold",
        definition_version="v1",
        aggregate_type="host.process",
        event_type="host.hold.recorded",
        event_schema_version=1,
        payload_mode=PayloadMode.NO_PAYLOAD,
        payload_schema_version=selected.schema_version,
        payload_schema_contract_version=selected.contract_version,
        payload_schema_contract_digest=selected.contract_digest,
        payload_canonicalizer_version=selected.canonicalizer_implementation_version,
        trust_scope=TrustScope.OBSERVED,
        security_scope="authority.host",
        retention_scope="authority.host",
        required_scope="authority.host.hold",
    )


def open_host_store(path: Path) -> object:
    hold_contract = hold_payload_contract()
    envelope_contract = envelope_payload_contract()
    auto_publish_contract = auto_publish_payload_contract()
    internal_beta_contract = internal_beta_payload_contract()
    signal_contract = signal_payload_contract()
    skip_contract = skip_payload_contract()
    decision_contract = decision_payload_contract()
    operation_contract = operation_payload_contract()
    publish_contract = publish_payload_contract()
    return open_authority_event_system(
        path=path,
        registry=CommandRegistry(
            [
                hold_command_definition(hold_contract),
                envelope_command_definition(envelope_contract),
                auto_publish_command_definition(auto_publish_contract),
                internal_beta_command_definition(internal_beta_contract),
                signal_command_definition(signal_contract),
                skip_command_definition(skip_contract),
                decision_command_definition(decision_contract),
                operation_command_definition(operation_contract),
                publish_command_definition(publish_contract),
            ]
        ),
        payload_schemas=PayloadSchemaRegistry(
            (
                hold_contract,
                envelope_contract,
                auto_publish_contract,
                internal_beta_contract,
                signal_contract,
                skip_contract,
                decision_contract,
                operation_contract,
                publish_contract,
            )
        ),
        authenticator=StaticAuthenticator(
            credentials={
                "host-process": StaticPrincipal(
                    "host.newsroom",
                    assurance_class="HOST_PROCESS",
                ),
                OWNER_CREDENTIAL: StaticPrincipal(
                    OWNER_PRINCIPAL,
                    assurance_class="OWNER_SIGNED",
                ),
                CONTROLLER_CREDENTIAL: StaticPrincipal(
                    CONTROLLER_ID,
                    assurance_class="AGENT_TURN_CONTROLLER",
                ),
            },
            authority_domain="newsroom.host",
        ),
        authorizer=StaticAuthorizer(
            policy_version="host-store-v1",
            grants_by_principal={
                "host.newsroom": frozenset(
                    {
                        "authority.host.hold",
                        "authority.host.read",
                        "authority.publication.dispatch",
                    }
                ),
                OWNER_PRINCIPAL: frozenset(
                    {
                        "authority.envelope.grant",
                        "authority.autopublish.grant",
                        "authority.internal_beta.grant",
                        "authority.discovery.ingest",
                        "authority.host.read",
                    }
                ),
                CONTROLLER_ID: frozenset(
                    {
                        "authority.publication.decide",
                        "authority.host.read",
                    }
                ),
            },
        ),
        event_read_policy=EventReadPolicy(
            policy_id="host-store-read-v1",
            purpose="host.store.audit",
            required_scope="authority.host.read",
            allowed_principal_ids=frozenset(
                {"host.newsroom", OWNER_PRINCIPAL, CONTROLLER_ID}
            ),
            allowed_security_scopes=frozenset(
                {
                    "authority.host",
                    "authority.envelope",
                    "authority.discovery",
                    "authority.publication",
                }
            ),
            allowed_trust_scopes=frozenset(
                {TrustScope.OBSERVED, TrustScope.ADMITTED}
            ),
            metadata_classes=frozenset(
                {
                    MetadataClass.ROUTING,
                    MetadataClass.PROVENANCE,
                    MetadataClass.RESULT,
                }
            ),
        ),
    )
