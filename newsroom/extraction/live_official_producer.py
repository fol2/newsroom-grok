from __future__ import annotations

import json

from newsroom.authority.canonical import canonical_json_bytes, digest_bytes

from .live_official import (
    LIVE_OFFICIAL_FORBIDDEN_TEXT_DIGESTS,
    LIVE_OFFICIAL_PRODUCER_KIND,
    require_live_official_contract,
)
from .models import (
    ExtractorContractRequest,
    ExtractionRunRequest,
    ProducedExtraction,
    ProposalDraft,
)
from .output_schema import LIVE_OFFICIAL_OUTPUT_SCHEMA_NAME
from .producer import DeterministicFixtureExtractor
from .types import (
    EvidenceRange,
    ExtractionContractError,
    ExtractionExecutionProfile,
    ExtractionFailureCode,
    ExtractionOutcome,
    ExtractionOutputValidation,
    ExtractionProposalKind,
    ExtractionUsage,
    ProposalPredicateHint,
)


def _json_value_range(text: str, key: str, value: str, passage_id) -> EvidenceRange:
    data = text.encode("utf-8")
    key_json = json.dumps(key, ensure_ascii=False)
    value_json = json.dumps(value, ensure_ascii=False)
    marker = f"{key_json}:{value_json}".encode("utf-8")
    start = data.find(marker)
    if start < 0:
        raise ExtractionContractError(
            "live-official evidence key is absent from the bound passage"
        )
    value_start = start + len(key_json.encode("utf-8")) + 1
    value_end = value_start + len(value_json.encode("utf-8"))
    encoded_value = value_json.encode("utf-8")
    if data[value_start:value_end] != encoded_value:
        raise ExtractionContractError(
            "live-official evidence span differs from the bound passage"
        )
    return EvidenceRange(
        passage_id=passage_id,
        start_byte=value_start,
        end_byte=value_end,
        evidence_text_digest=digest_bytes(data[value_start:value_end]),
    )


class DeterministicLiveOfficialExtractor:
    """In-process 4A producer over admitted live passages. No Graphiti or model."""

    producer_kind = LIVE_OFFICIAL_PRODUCER_KIND

    @staticmethod
    def _usage(
        request: ExtractionRunRequest,
        raw: dict[str, object] | None,
        proposals: tuple[ProposalDraft, ...],
    ) -> ExtractionUsage:
        output_bytes = 0 if raw is None else len(canonical_json_bytes(raw))
        return ExtractionUsage(
            elapsed_ms=0,
            input_bytes=request.input_binding.input_bytes,
            output_bytes=output_bytes,
            proposal_count=len(proposals),
            evidence_range_count=sum(len(item.evidence) for item in proposals),
            request_tokens=0,
            response_tokens=0,
            cost_microunits=0,
        )

    def produce(
        self,
        *,
        contract: ExtractorContractRequest,
        request: ExtractionRunRequest,
    ) -> ProducedExtraction:
        require_live_official_contract(contract)
        if not isinstance(request, ExtractionRunRequest):
            raise TypeError("live-official extractor needs a typed run request")
        if contract.execution_profile is ExtractionExecutionProfile.FIXTURE_REPLAY_ONLY:
            raise ExtractionContractError(
                "live-official extractor rejects fixture replay"
            )
        passages = request.input_binding.passages
        if len(passages) != 1:
            raise ExtractionContractError(
                "live-official extraction binds one admitted representation passage"
            )
        passage = passages[0]
        text = passage.require_text()
        digest = digest_bytes(text.encode("utf-8"))
        if digest in LIVE_OFFICIAL_FORBIDDEN_TEXT_DIGESTS:
            raise ExtractionContractError(
                "live-official extractor rejects repository fixture bytes"
            )
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ExtractionContractError(
                "live-official passage is not the bound representation"
            ) from exc
        if (
            not isinstance(payload, dict)
            or frozenset(payload) != frozenset({"item_id", "source_id", "url"})
            or any(not isinstance(payload[key], str) or not payload[key] for key in payload)
        ):
            raise ExtractionContractError(
                "live-official passage is not the bound representation"
            )
        source_id = payload["source_id"]
        item_id = payload["item_id"]
        source_range = _json_value_range(text, "source_id", source_id, passage.passage_id)
        item_range = _json_value_range(text, "item_id", item_id, passage.passage_id)
        proposals = (
            ProposalDraft(
                local_id="entity.source",
                kind=ExtractionProposalKind.ENTITY_MENTION,
                subject_placeholder=source_id,
                object_placeholder=None,
                predicate_hint=None,
                confidence_basis_points=9_800,
                uncertainty_codes=(),
                rationale_codes=("BOUND_REPRESENTATION_SPAN",),
                evidence=(source_range,),
            ),
            ProposalDraft(
                local_id="entity.item",
                kind=ExtractionProposalKind.ENTITY_MENTION,
                subject_placeholder=item_id,
                object_placeholder=None,
                predicate_hint=None,
                confidence_basis_points=9_800,
                uncertainty_codes=(),
                rationale_codes=("BOUND_REPRESENTATION_SPAN",),
                evidence=(item_range,),
            ),
            ProposalDraft(
                local_id="relation.source-about-item",
                kind=ExtractionProposalKind.RELATION,
                subject_placeholder=source_id,
                object_placeholder=item_id,
                predicate_hint=ProposalPredicateHint.ABOUT_EVENT,
                confidence_basis_points=9_000,
                uncertainty_codes=("REQUIRES_RELATION_ADMISSION",),
                rationale_codes=("BOUND_REPRESENTATION_SPAN",),
                evidence=(source_range, item_range),
            ),
        )
        raw = {
            "schema_version": LIVE_OFFICIAL_OUTPUT_SCHEMA_NAME,
            "entities": [
                {"local_id": item.local_id, "text": item.subject_placeholder}
                for item in proposals
                if item.kind is ExtractionProposalKind.ENTITY_MENTION
            ],
            "equivalences": [],
            "relations": [
                {
                    "local_id": item.local_id,
                    "subject": item.subject_placeholder,
                    "object": item.object_placeholder,
                    "predicate": item.predicate_hint.value,
                }
                for item in proposals
                if item.kind is ExtractionProposalKind.RELATION
            ],
        }
        return ProducedExtraction(
            outcome=ExtractionOutcome.SUCCESS,
            failure_code=ExtractionFailureCode.NONE,
            validation=ExtractionOutputValidation.VALID,
            raw_output_value=raw,
            proposals=proposals,
            usage=self._usage(request, raw, proposals),
        )


class ExtractionProducerDispatcher:
    """Route by contract profile. Fixture replay stays on the fixture producer."""

    producer_kind = "DISPATCHED"

    def produce(
        self,
        *,
        contract: ExtractorContractRequest,
        request: ExtractionRunRequest,
    ) -> ProducedExtraction:
        if contract.execution_profile is ExtractionExecutionProfile.LIVE_OFFICIAL:
            return DeterministicLiveOfficialExtractor().produce(
                contract=contract,
                request=request,
            )
        return DeterministicFixtureExtractor().produce(
            contract=contract,
            request=request,
        )


__all__ = [
    "DeterministicLiveOfficialExtractor",
    "ExtractionProducerDispatcher",
]
