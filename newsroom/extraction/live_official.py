"""Live-official Increment 4A proposal producer.

Derives Proposal Envelopes from retained official bytes. This is not the
repository bilingual fixture producer and not mint-bundle-body.
"""

from __future__ import annotations

import re

from newsroom.authority.canonical import digest_bytes
from newsroom.extraction.models import ProposalDraft
from newsroom.extraction.types import (
    EvidenceRange,
    ExtractionContractError,
    ExtractionPassageId,
    ExtractionProposalKind,
)

LIVE_OFFICIAL_PRODUCER_KIND = "LIVE_OFFICIAL"
LIVE_OFFICIAL_PROFILE = "LIVE_OFFICIAL"
LIVE_OFFICIAL_RATIONALE = "OFFICIAL_TITLE_SPAN"
MAX_TITLE_CHARS = 300
MAX_BODY_CHARS = 32_000

PASSAGE_IDS = {
    "HK-04": ExtractionPassageId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa91"),
    "RAD-01": ExtractionPassageId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa92"),
    "RAD-02": ExtractionPassageId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa93"),
    "UK-01": ExtractionPassageId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa94"),
    "UK-05": ExtractionPassageId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa95"),
    "X-SEARCH-POSTS": ExtractionPassageId.parse(
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa96"
    ),
}

SOURCE_LANGUAGES = {
    "HK-04": "zh-HK",
    "RAD-01": "zh-HK",
    "RAD-02": "en-GB",
    "UK-01": "en-GB",
    "UK-05": "en-GB",
    "X-SEARCH-POSTS": "en-GB",
}


def clean_official_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def official_passage_text(*, title: str, body: str = "") -> str:
    clean_title = clean_official_text(title)
    clean_body = "\n\n".join(
        part
        for part in (clean_official_text(block) for block in body.split("\n\n"))
        if part
    )
    if not clean_title:
        raise ExtractionContractError("official title is missing from retained bytes")
    if len(clean_title) > MAX_TITLE_CHARS:
        raise ExtractionContractError("official title exceeds its bound")
    if len(clean_body) > MAX_BODY_CHARS:
        raise ExtractionContractError("official body exceeds its bound")
    if clean_body and clean_body != clean_title:
        return f"{clean_title}\n\n{clean_body}"
    return clean_title


def title_evidence_range(
    *,
    text: str,
    title: str,
    passage_id: ExtractionPassageId,
) -> EvidenceRange:
    data = text.encode("utf-8")
    phrase = title.encode("utf-8")
    start = data.find(phrase)
    if start < 0:
        raise ExtractionContractError(
            "official title span is absent from retained passage bytes"
        )
    end = start + len(phrase)
    return EvidenceRange(
        passage_id=passage_id,
        start_byte=start,
        end_byte=end,
        evidence_text_digest=digest_bytes(data[start:end]),
    )


class LiveOfficialExtractor:
    """Proposal producer bound to retained official bytes, not fixture text."""

    producer_kind = LIVE_OFFICIAL_PRODUCER_KIND

    def produce_from_official_bytes(
        self,
        *,
        source_id: str,
        title: str,
        body: str = "",
    ) -> tuple[str, str, str, tuple[ProposalDraft, ...]]:
        if source_id not in PASSAGE_IDS:
            raise ExtractionContractError(
                "live official extract is not authorised for this source"
            )
        text = official_passage_text(title=title, body=body)
        clean_title = clean_official_text(title)
        passage_id = PASSAGE_IDS[source_id]
        evidence = title_evidence_range(
            text=text,
            title=clean_title,
            passage_id=passage_id,
        )
        proposal = ProposalDraft(
            local_id="entity.title",
            kind=ExtractionProposalKind.ENTITY_MENTION,
            subject_placeholder=clean_title,
            object_placeholder=None,
            predicate_hint=None,
            confidence_basis_points=10_000,
            uncertainty_codes=(),
            rationale_codes=(LIVE_OFFICIAL_RATIONALE,),
            evidence=(evidence,),
        )
        return (
            text,
            digest_bytes(text.encode("utf-8")),
            SOURCE_LANGUAGES[source_id],
            (proposal,),
        )


def assert_not_fixture_text(text: str) -> None:
    from newsroom.extraction.fixtures import (
        FIXTURE_EN_TEXT,
        FIXTURE_HOMONYM_EN_TEXT,
        FIXTURE_HOMONYM_ZH_HK_TEXT,
        FIXTURE_ZH_HK_TEXT,
    )

    forbidden = {
        FIXTURE_EN_TEXT,
        FIXTURE_ZH_HK_TEXT,
        FIXTURE_HOMONYM_EN_TEXT,
        FIXTURE_HOMONYM_ZH_HK_TEXT,
    }
    if text in forbidden:
        raise ExtractionContractError(
            "live official extract rejects increment-4A fixture bytes"
        )
