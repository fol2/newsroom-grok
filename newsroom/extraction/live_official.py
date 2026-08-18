from __future__ import annotations

from newsroom.authority.canonical import digest_canonical

from .fixtures import (
    FIXTURE_ALLOWED_TEXT_DIGESTS,
    FIXTURE_HOMONYM_ALLOWED_TEXT_DIGESTS,
)
from .models import ExtractorContractRequest
from .output_schema import live_official_output_schema_contract
from .types import (
    ExtractionContractError,
    ExtractionExecutionProfile,
    ExtractorContractId,
    VersionedExtractionComponent,
)

LIVE_OFFICIAL_PRODUCER_KIND = "DETERMINISTIC_LIVE_OFFICIAL"
LIVE_OFFICIAL_FORBIDDEN_TEXT_DIGESTS = frozenset(
    FIXTURE_ALLOWED_TEXT_DIGESTS + FIXTURE_HOMONYM_ALLOWED_TEXT_DIGESTS
)


def _component(
    component_id: str,
    version: str,
    contract: object,
) -> VersionedExtractionComponent:
    return VersionedExtractionComponent(
        component_id=component_id,
        component_version=version,
        contract_digest=digest_canonical(contract),
    )


LIVE_OFFICIAL_FRAMEWORK_COMPONENT = _component(
    "newsroom.live-official.framework",
    "v1",
    {"runtime": "repository-owned-pure-python", "network": False, "graphiti": False},
)
LIVE_OFFICIAL_MODEL_COMPONENT = _component(
    "newsroom.live-official.no-model",
    "v1",
    {"model": None, "provider": None, "credentials": False, "graphiti": False},
)
LIVE_OFFICIAL_PROMPT_COMPONENT = _component(
    "newsroom.live-official.prompt",
    "v1",
    {
        "instruction": "emit-entity-equivalence-relation-from-bound-passages-only",
        "source_is_untrusted_data": True,
        "fixture_replay": False,
    },
)
LIVE_OFFICIAL_OUTPUT_SCHEMA_COMPONENT = VersionedExtractionComponent(
    *live_official_output_schema_contract()
)
LIVE_OFFICIAL_CODE_COMPONENT = _component(
    "newsroom.live-official.producer-code",
    "v1",
    {"implementation": "DeterministicLiveOfficialExtractor", "side_effects": []},
)
LIVE_OFFICIAL_NORMALISATION_COMPONENT = _component(
    "newsroom.live-official.normalisation",
    "v1",
    {"unicode": "preserve", "bytes": "utf-8", "ordering": "canonical-json"},
)
LIVE_OFFICIAL_POLICY_COMPONENT = _component(
    "newsroom.live-official.policy",
    "v1",
    {
        "profiles": [ExtractionExecutionProfile.LIVE_OFFICIAL.value],
        "forbidden_text_digests": sorted(LIVE_OFFICIAL_FORBIDDEN_TEXT_DIGESTS),
        "real_runtime": False,
        "graphiti": False,
    },
)


def live_official_contract_request(
    *,
    contract_id: ExtractorContractId,
    idempotency_key: str = "increment-4a-live-official-contract-v1",
) -> ExtractorContractRequest:
    return ExtractorContractRequest(
        contract_id=contract_id,
        framework=LIVE_OFFICIAL_FRAMEWORK_COMPONENT,
        model=LIVE_OFFICIAL_MODEL_COMPONENT,
        prompt=LIVE_OFFICIAL_PROMPT_COMPONENT,
        output_schema=LIVE_OFFICIAL_OUTPUT_SCHEMA_COMPONENT,
        code=LIVE_OFFICIAL_CODE_COMPONENT,
        normalisation=LIVE_OFFICIAL_NORMALISATION_COMPONENT,
        policy=LIVE_OFFICIAL_POLICY_COMPONENT,
        execution_profile=ExtractionExecutionProfile.LIVE_OFFICIAL,
        producer_kind=LIVE_OFFICIAL_PRODUCER_KIND,
        idempotency_key=idempotency_key,
    )


_EXPECTED_CONTRACT_ID = ExtractorContractId.parse(
    "00000000-0000-4000-8000-000000004201"
)
EXPECTED_LIVE_OFFICIAL_CONTRACT_SEMANTIC_DIGEST = live_official_contract_request(
    contract_id=_EXPECTED_CONTRACT_ID
).semantic_digest


def require_live_official_contract(contract: ExtractorContractRequest) -> None:
    if not isinstance(contract, ExtractorContractRequest):
        raise TypeError("live-official extractor needs a typed contract")
    if (
        contract.execution_profile is not ExtractionExecutionProfile.LIVE_OFFICIAL
        or contract.producer_kind != LIVE_OFFICIAL_PRODUCER_KIND
        or contract.semantic_digest != EXPECTED_LIVE_OFFICIAL_CONTRACT_SEMANTIC_DIGEST
    ):
        raise ExtractionContractError(
            "live-official extractor rejects an incompatible contract"
        )
    component_id, component_version, component_digest = (
        live_official_output_schema_contract()
    )
    if (
        contract.output_schema.component_id != component_id
        or contract.output_schema.component_version != component_version
        or contract.output_schema.contract_digest != component_digest
    ):
        raise ExtractionContractError(
            "live-official output-schema contract is incompatible"
        )


__all__ = [
    "EXPECTED_LIVE_OFFICIAL_CONTRACT_SEMANTIC_DIGEST",
    "LIVE_OFFICIAL_FORBIDDEN_TEXT_DIGESTS",
    "LIVE_OFFICIAL_PRODUCER_KIND",
    "live_official_contract_request",
    "require_live_official_contract",
]
