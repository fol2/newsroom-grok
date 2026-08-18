"""Live-official Increment 4A producer is not the fixture extractor."""

from __future__ import annotations

from dataclasses import replace

import pytest

from newsroom.authority.canonical import canonical_json_bytes, digest_bytes
from newsroom.authority.objects import ObjectAccessDecisionId
from newsroom.authority.types import ObjectAdmissionId
from newsroom.extraction import (
    DeterministicFixtureExtractor,
    DeterministicLiveOfficialExtractor,
    ExtractionBudget,
    ExtractionContractError,
    ExtractionInputBinding,
    ExtractionPassageId,
    ExtractionPassageInput,
    ExtractionRunId,
    ExtractionRunRequest,
    ExtractionRunVersionId,
    ExtractorContractId,
    FIXTURE_EN_LANGUAGE,
    FIXTURE_EN_TEXT,
    FIXTURE_ZH_HK_LANGUAGE,
    live_official_contract_request,
)
from newsroom.extraction.fixtures import deterministic_fixture_contract_request
from newsroom.sources import (
    DiscoveryRepresentationId,
    SourceDefinitionId,
    SourceDefinitionVersionId,
    SourceItemId,
    SourceRevisionId,
)

_DIGEST = "sha256:" + "a" * 64
_LIVE_JSON = canonical_json_bytes(
    {
        "item_id": "https://www.news.gov.hk/live-item",
        "source_id": "HK-01",
        "url": "https://www.news.gov.hk/live-item",
    }
).decode("utf-8")


def _passage(text: str) -> ExtractionPassageInput:
    data = text.encode("utf-8")
    digest = digest_bytes(data)
    return ExtractionPassageInput(
        passage_id=ExtractionPassageId.parse("00000000-0000-4000-8000-000000004301"),
        admission_id=ObjectAdmissionId.parse("00000000-0000-4000-8000-000000004302"),
        access_decision_id=ObjectAccessDecisionId.parse(
            "00000000-0000-4000-8000-000000004303"
        ),
        hydration_policy_contract_digest=_DIGEST,
        principal_id="owner.newsroom",
        authority_domain="newsroom.host",
        purpose="extraction.live-official",
        object_class="discovery_representation",
        allowed_use="extraction.live-official",
        security_scope="authority.extraction",
        retention_scope="extraction.live-official",
        byte_offset=0,
        byte_length=len(data),
        blob_digest=digest,
        text_digest=digest,
        language="zh-Hant",
        text=text,
    )


def _request(text: str, *, item_id: str | None = None) -> ExtractionRunRequest:
    selected_item = item_id or "00000000-0000-4000-8000-000000004313"
    return ExtractionRunRequest(
        run_id=ExtractionRunId.parse("00000000-0000-4000-8000-000000004304"),
        run_version_id=ExtractionRunVersionId.parse(
            "00000000-0000-4000-8000-000000004305"
        ),
        version_number=1,
        expected_previous_version_id=None,
        contract_id=ExtractorContractId.parse("00000000-0000-4000-8000-000000004201"),
        input_binding=ExtractionInputBinding(
            definition_id=SourceDefinitionId.parse(
                "00000000-0000-4000-8000-000000004310"
            ),
            definition_version_id=SourceDefinitionVersionId.parse(
                "00000000-0000-4000-8000-000000004311"
            ),
            item_id=SourceItemId.parse(selected_item),
            revision_id=SourceRevisionId.parse(
                "00000000-0000-4000-8000-000000004314"
            ),
            representation_id=DiscoveryRepresentationId.parse(
                "00000000-0000-4000-8000-000000004315"
            ),
            passages=(_passage(text),),
        ),
        budget=ExtractionBudget(
            timeout_ms=10_000,
            max_input_bytes=64 * 1024,
            max_output_bytes=256 * 1024,
            max_proposals=100,
            max_evidence_ranges=500,
            max_request_tokens=0,
            max_response_tokens=0,
            max_cost_microunits=0,
        ),
        idempotency_key="live-official-unit-v1",
    )


def test_fixture_extractor_rejects_live_representation_bytes() -> None:
    with pytest.raises(
        ExtractionContractError,
        match="exact en-GB and zh-HK passages",
    ):
        DeterministicFixtureExtractor().produce(
            contract=deterministic_fixture_contract_request(
                contract_id=ExtractorContractId.parse(
                    "00000000-0000-4000-8000-000000004101"
                )
            ),
            request=_request(_LIVE_JSON),
        )


def test_fixture_extractor_rejects_live_bytes_even_with_fixture_languages() -> None:
    en = replace(_passage(_LIVE_JSON), language=FIXTURE_EN_LANGUAGE)
    zh = replace(
        _passage(_LIVE_JSON),
        passage_id=ExtractionPassageId.parse("00000000-0000-4000-8000-000000004306"),
        admission_id=ObjectAdmissionId.parse("00000000-0000-4000-8000-000000004307"),
        access_decision_id=ObjectAccessDecisionId.parse(
            "00000000-0000-4000-8000-000000004308"
        ),
        language=FIXTURE_ZH_HK_LANGUAGE,
    )
    request = ExtractionRunRequest(
        run_id=ExtractionRunId.parse("00000000-0000-4000-8000-000000004304"),
        run_version_id=ExtractionRunVersionId.parse(
            "00000000-0000-4000-8000-000000004305"
        ),
        version_number=1,
        expected_previous_version_id=None,
        contract_id=ExtractorContractId.parse("00000000-0000-4000-8000-000000004101"),
        input_binding=ExtractionInputBinding(
            definition_id=SourceDefinitionId.parse(
                "00000000-0000-4000-8000-000000004310"
            ),
            definition_version_id=SourceDefinitionVersionId.parse(
                "00000000-0000-4000-8000-000000004311"
            ),
            item_id=SourceItemId.parse("00000000-0000-4000-8000-000000004313"),
            revision_id=SourceRevisionId.parse(
                "00000000-0000-4000-8000-000000004314"
            ),
            representation_id=DiscoveryRepresentationId.parse(
                "00000000-0000-4000-8000-000000004315"
            ),
            passages=(en, zh),
        ),
        budget=ExtractionBudget(
            timeout_ms=10_000,
            max_input_bytes=64 * 1024,
            max_output_bytes=256 * 1024,
            max_proposals=100,
            max_evidence_ranges=500,
            max_request_tokens=0,
            max_response_tokens=0,
            max_cost_microunits=0,
        ),
        idempotency_key="live-official-unit-v1",
    )
    with pytest.raises(ExtractionContractError, match="approved fixture bytes"):
        DeterministicFixtureExtractor().produce(
            contract=deterministic_fixture_contract_request(
                contract_id=ExtractorContractId.parse(
                    "00000000-0000-4000-8000-000000004101"
                )
            ),
            request=request,
        )


def test_live_official_extractor_rejects_fixture_bytes() -> None:
    with pytest.raises(ExtractionContractError, match="repository fixture bytes"):
        DeterministicLiveOfficialExtractor().produce(
            contract=live_official_contract_request(
                contract_id=ExtractorContractId.parse(
                    "00000000-0000-4000-8000-000000004201"
                )
            ),
            request=_request(FIXTURE_EN_TEXT),
        )


def test_live_official_extract_reads_bound_passage_not_item_id() -> None:
    contract = live_official_contract_request(
        contract_id=ExtractorContractId.parse("00000000-0000-4000-8000-000000004201")
    )
    produced = DeterministicLiveOfficialExtractor().produce(
        contract=contract,
        request=_request(
            _LIVE_JSON,
            item_id="00000000-0000-4000-8000-000000004399",
        ),
    )
    mentions = {
        item.subject_placeholder
        for item in produced.proposals
        if item.kind.value == "ENTITY_MENTION"
    }
    assert "HK-01" in mentions
    assert "https://www.news.gov.hk/live-item" in mentions
    assert "00000000-0000-4000-8000-000000004399" not in mentions


def test_dispatcher_does_not_send_live_items_to_fixture_extractor(monkeypatch: pytest.MonkeyPatch) -> None:
    from newsroom.extraction.live_official_producer import ExtractionProducerDispatcher
    from newsroom.extraction.producer import DeterministicFixtureExtractor

    def forbidden(*_args, **_kwargs):
        raise AssertionError("fixture extractor must not run against live items")

    monkeypatch.setattr(DeterministicFixtureExtractor, "produce", forbidden)
    produced = ExtractionProducerDispatcher().produce(
        contract=live_official_contract_request(
            contract_id=ExtractorContractId.parse("00000000-0000-4000-8000-000000004201")
        ),
        request=_request(_LIVE_JSON),
    )
    assert produced.outcome.value == "SUCCESS"
    mentions = {
        item.subject_placeholder
        for item in produced.proposals
        if item.kind.value == "ENTITY_MENTION"
    }
    assert "HK-01" in mentions
