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
from newsroom.authority.discovery_system import (
    open_governed_discovery_authority_system,
)
from newsroom.authority.object_policy import merge_authority_registries
from newsroom.authority.types import UtcTimestamp
from newsroom.checks.policy import merge_discovery_check_authority_registries
from newsroom.checks.read_policy import DiscoveryCheckReadPolicy
from newsroom.discovery.policy import merge_discovery_signal_lead_registries
from newsroom.discovery.types import DiscoveryReadPolicy
from newsroom.extraction.policy import merge_extraction_authority_registries
from newsroom.sources import SourceRegistryReadPolicy
from newsroom.sources.policy import merge_source_registry_authority_registries
from newsroom.discovery_ingest import (
    signal_command_definition,
    signal_payload_contract,
    skip_command_definition,
    skip_payload_contract,
)
from newsroom.x_search_ingest import (
    signal_command_definition as x_search_command_definition,
    signal_payload_contract as x_search_payload_contract,
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
from newsroom.publication_bundle import (
    bundle_command_definition,
    bundle_payload_contract,
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


HOST_DISCOVERY_SCOPES = frozenset(
    {
        "authority.sources.manage",
        "authority.sources.observe",
        "authority.sources.read",
        "authority.sources.read_sensitive",
        "authority.checks.manage",
        "authority.checks.execute",
        "authority.checks.observe",
        "authority.checks.decide",
        "authority.findings.manage",
        "authority.findings.observe",
        "authority.checks.read",
        "authority.checks.read_sensitive",
        "authority.discovery.signals.admit",
        "authority.discovery.gates.decide",
        "authority.discovery.leads.open",
        "authority.discovery.watch.manage",
        "authority.discovery.leads.disposition",
        "authority.discovery.read",
        "authority.discovery.read_sensitive",
    }
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


def _host_contracts() -> tuple[PayloadSchemaContract, ...]:
    return (
        hold_payload_contract(),
        envelope_payload_contract(),
        auto_publish_payload_contract(),
        internal_beta_payload_contract(),
        signal_payload_contract(),
        skip_payload_contract(),
        x_search_payload_contract(),
        decision_payload_contract(),
        operation_payload_contract(),
        publish_payload_contract(),
        bundle_payload_contract(),
    )


def _host_command_registry(
    contracts: tuple[PayloadSchemaContract, ...],
) -> CommandRegistry:
    (
        hold_contract,
        envelope_contract,
        auto_publish_contract,
        internal_beta_contract,
        signal_contract,
        skip_contract,
        x_search_contract,
        decision_contract,
        operation_contract,
        publish_contract,
        bundle_contract,
    ) = contracts
    return CommandRegistry(
        [
            hold_command_definition(hold_contract),
            envelope_command_definition(envelope_contract),
            auto_publish_command_definition(auto_publish_contract),
            internal_beta_command_definition(internal_beta_contract),
            signal_command_definition(signal_contract),
            skip_command_definition(skip_contract),
            x_search_command_definition(x_search_contract),
            decision_command_definition(decision_contract),
            operation_command_definition(operation_contract),
            publish_command_definition(publish_contract),
            bundle_command_definition(bundle_contract),
        ]
    )


def host_authority_registries() -> tuple[CommandRegistry, PayloadSchemaRegistry]:
    contracts = _host_contracts()
    registry, schemas = merge_source_registry_authority_registries(
        command_registry=_host_command_registry(contracts),
        payload_schemas=PayloadSchemaRegistry(contracts),
    )
    registry, schemas = merge_discovery_check_authority_registries(
        command_registry=registry,
        payload_schemas=schemas,
    )
    registry, schemas = merge_discovery_signal_lead_registries(
        command_registry=registry,
        payload_schemas=schemas,
    )
    registry, schemas = merge_authority_registries(
        command_registry=registry,
        payload_schemas=schemas,
    )
    return merge_extraction_authority_registries(
        command_registry=registry,
        payload_schemas=schemas,
    )


def host_authenticator() -> StaticAuthenticator:
    return StaticAuthenticator(
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
    )


def host_authorizer() -> StaticAuthorizer:
    return StaticAuthorizer(
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
                | HOST_DISCOVERY_SCOPES
            ),
            CONTROLLER_ID: frozenset(
                {
                    "authority.publication.decide",
                    "authority.publication.bundle",
                    "authority.host.read",
                }
            ),
        },
    )


def host_source_read_policy() -> SourceRegistryReadPolicy:
    return SourceRegistryReadPolicy(
        policy_id="host-source-registry-read-v1",
        purpose="host.source.audit",
        metadata_required_scope="authority.sources.read",
        sensitive_required_scope="authority.sources.read_sensitive",
        allowed_principal_ids=frozenset({OWNER_PRINCIPAL}),
        max_results=100,
    )


def host_check_read_policy() -> DiscoveryCheckReadPolicy:
    return DiscoveryCheckReadPolicy(
        policy_id="host-discovery-check-read-v1",
        purpose="host.check.audit",
        metadata_required_scope="authority.checks.read",
        sensitive_required_scope="authority.checks.read_sensitive",
        allowed_principal_ids=frozenset({OWNER_PRINCIPAL}),
        max_results=100,
    )


def host_discovery_read_policy() -> DiscoveryReadPolicy:
    return DiscoveryReadPolicy(
        policy_id="host-discovery-signal-lead-read-v1",
        purpose="host.discovery.audit",
        metadata_required_scope="authority.discovery.read",
        sensitive_required_scope="authority.discovery.read_sensitive",
        allowed_principal_ids=frozenset({OWNER_PRINCIPAL}),
        max_results=100,
    )


def open_host_store(path: Path) -> object:
    registry, schemas = host_authority_registries()
    return open_authority_event_system(
        path=path,
        registry=registry,
        payload_schemas=schemas,
        authenticator=host_authenticator(),
        authorizer=host_authorizer(),
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
                    "authority.source_registry",
                    "authority.discovery_checks",
                    "authority.audit",
                    "authority.object_lifecycle",
                    "authority.extraction",
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


def open_host_discovery_system(
    path: Path,
    *,
    clock=UtcTimestamp.now,
):
    registry, schemas = host_authority_registries()
    return open_governed_discovery_authority_system(
        path=path,
        registry=registry,
        payload_schemas=schemas,
        authenticator=host_authenticator(),
        authorizer=host_authorizer(),
        source_read_policy=host_source_read_policy(),
        check_read_policy=host_check_read_policy(),
        discovery_read_policy=host_discovery_read_policy(),
        clock=clock,
    )
