"""Run a live Increment 4A extract on one admitted first-boot source.

mint-bundle-body is not extract. DeterministicFixtureExtractor is not the
live producer. UK-10 stays skip. X-SEARCH-POSTS uses the admitted seq-14 URL
only.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

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
from newsroom.authority.canonical import digest_bytes
from newsroom.discovery_ingest import (
    EVENT_TYPE as SIGNAL_EVENT_TYPE,
    OFFICIAL_RSS_ADAPTER,
    SKIP_EVENT_TYPE,
    SOURCE_URLS,
    FeedFetchError,
    load_official_rss_body,
)
from newsroom.extraction.live_official import (
    LIVE_OFFICIAL_PRODUCER_KIND,
    LIVE_OFFICIAL_PROFILE,
    LiveOfficialExtractor,
    assert_not_fixture_text,
    clean_official_text,
)
from newsroom.extraction.producer import DeterministicFixtureExtractor
from newsroom.increment9.proving import assert_allowed_url
from newsroom.x_search_ingest import GATED_X_SEARCH_ADAPTER


OWNER_CREDENTIAL = "owner-signed"
COMMAND_TYPE = "extraction.live_official.execute"
EVENT_TYPE = "extraction.run.executed"
AGGREGATE_TYPE = "extraction.run"
MAX_ITEM_ID_BYTES = 256
MAX_TITLE_CHARS = 300
EXTRACT_SOURCE_IDS = frozenset(
    {"HK-04", "RAD-01", "RAD-02", "UK-01", "UK-05", "X-SEARCH-POSTS"}
)
RSS_EXTRACT_SOURCE_IDS = frozenset(
    {"HK-04", "RAD-01", "RAD-02", "UK-01", "UK-05"}
)
X_SEARCH_SOURCE_ID = "X-SEARCH-POSTS"
EXTRACT_AGGREGATE_IDS = {
    "HK-04": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa91"),
    "RAD-01": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa92"),
    "RAD-02": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa93"),
    "UK-01": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa94"),
    "UK-05": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa95"),
    "X-SEARCH-POSTS": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa96"),
}
PAYLOAD_KEYS = (
    "auto_publish",
    "discord",
    "item_id",
    "language",
    "outcome",
    "producer_kind",
    "proposal_count",
    "proposals",
    "public_adapter",
    "source_id",
    "text_digest",
    "title",
    "url",
)
PROPOSAL_KEYS = (
    "confidence_basis_points",
    "evidence",
    "kind",
    "local_id",
    "rationale_codes",
    "subject_placeholder",
)
EVIDENCE_KEYS = (
    "end_byte",
    "evidence_text_digest",
    "passage_id",
    "start_byte",
)

SAMPLE_PAYLOAD: dict[str, Any] = {
    "auto_publish": False,
    "discord": False,
    "item_id": (
        "https://www.edb.gov.hk/tc/student-parents/parents-related/"
        "ebulletin-for-parents/2025-2026/20260814.html"
    ),
    "language": "zh-HK",
    "outcome": "SUCCESS",
    "producer_kind": LIVE_OFFICIAL_PRODUCER_KIND,
    "proposal_count": 1,
    "proposals": [
        {
            "confidence_basis_points": 10000,
            "evidence": [
                {
                    "end_byte": 12,
                    "evidence_text_digest": digest_bytes("家長電子通訊".encode("utf-8")),
                    "passage_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa91",
                    "start_byte": 0,
                }
            ],
            "kind": "ENTITY_MENTION",
            "local_id": "entity.title",
            "rationale_codes": ["OFFICIAL_TITLE_SPAN"],
            "subject_placeholder": "家長電子通訊",
        }
    ],
    "public_adapter": False,
    "source_id": "HK-04",
    "text_digest": digest_bytes("家長電子通訊".encode("utf-8")),
    "title": "家長電子通訊",
    "url": SOURCE_URLS["HK-04"],
}


class ExtractSignalError(ValueError):
    """Live official extract failed closed."""


def resolve_extract_source_id(source_id: str | None) -> str:
    if source_id == "UK-10":
        raise ExtractSignalError("UK-10 stays skip; no fake extract")
    if source_id == "HK-01":
        raise ExtractSignalError(
            "HK-01 extract is out of this pass; do not remint seq 15"
        )
    if not isinstance(source_id, str) or source_id not in EXTRACT_SOURCE_IDS:
        raise ExtractSignalError(
            "source must be an admitted extract source "
            "(HK-04, RAD-01, RAD-02, UK-01, UK-05, X-SEARCH-POSTS)"
        )
    return source_id


def extract_idempotency_key(source_id: str) -> str:
    return f"live-official-extract-{resolve_extract_source_id(source_id)}-v1"


def _bound_item_id(value: object) -> str:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise ExtractSignalError("item identity must be a single-line token")
    if len(value.encode("utf-8")) > MAX_ITEM_ID_BYTES:
        raise ExtractSignalError("item identity exceeds bound")
    return value


def _bound_title(value: object) -> str:
    if not isinstance(value, str):
        raise ExtractSignalError("official title must be text")
    title = clean_official_text(value)
    if not title or len(title) > MAX_TITLE_CHARS:
        raise ExtractSignalError("official title is missing or exceeds bound")
    return title


def _canonical_proposal(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(PROPOSAL_KEYS):
        raise ExtractSignalError("proposal keys are exact")
    evidence = value["evidence"]
    if not isinstance(evidence, list) or len(evidence) != 1:
        raise ExtractSignalError("exactly one evidence range is required")
    item = evidence[0]
    if not isinstance(item, dict) or set(item) != set(EVIDENCE_KEYS):
        raise ExtractSignalError("evidence keys are exact")
    digest = item["evidence_text_digest"]
    if (
        not isinstance(digest, str)
        or not digest.startswith("sha256:")
        or len(digest) != 71
    ):
        raise ExtractSignalError("evidence digest is invalid")
    start = item["start_byte"]
    end = item["end_byte"]
    if (
        isinstance(start, bool)
        or isinstance(end, bool)
        or not isinstance(start, int)
        or not isinstance(end, int)
        or start < 0
        or end <= start
    ):
        raise ExtractSignalError("evidence range is invalid")
    codes = value["rationale_codes"]
    if not isinstance(codes, list) or codes != ["OFFICIAL_TITLE_SPAN"]:
        raise ExtractSignalError("rationale must be the official title span")
    confidence = value["confidence_basis_points"]
    if (
        isinstance(confidence, bool)
        or not isinstance(confidence, int)
        or confidence != 10_000
    ):
        raise ExtractSignalError("live official confidence is exact")
    if value["kind"] != "ENTITY_MENTION":
        raise ExtractSignalError("live official proposal kind is entity mention")
    if value["local_id"] != "entity.title":
        raise ExtractSignalError("live official local id is entity.title")
    title = _bound_title(value["subject_placeholder"])
    passage_id = item["passage_id"]
    if not isinstance(passage_id, str) or len(passage_id) != 36:
        raise ExtractSignalError("passage identity is invalid")
    return {
        "confidence_basis_points": 10_000,
        "evidence": [
            {
                "end_byte": end,
                "evidence_text_digest": digest,
                "passage_id": passage_id,
                "start_byte": start,
            }
        ],
        "kind": "ENTITY_MENTION",
        "local_id": "entity.title",
        "rationale_codes": ["OFFICIAL_TITLE_SPAN"],
        "subject_placeholder": title,
    }


def canonicalize_extract_payload(value: object) -> bytes:
    if not isinstance(value, dict):
        raise ExtractSignalError("extract payload must be an object")
    if set(value) != set(PAYLOAD_KEYS):
        raise ExtractSignalError("extract payload keys are exact")
    if value["auto_publish"] is not False:
        raise ExtractSignalError("AUTO_PUBLISH stays off")
    if value["discord"] is not False:
        raise ExtractSignalError("Discord stays off")
    if value["public_adapter"] is not False:
        raise ExtractSignalError("public adapters stay off")
    if value["producer_kind"] != LIVE_OFFICIAL_PRODUCER_KIND:
        raise ExtractSignalError("fixture producer is not the live producer")
    if value["outcome"] != "SUCCESS":
        raise ExtractSignalError("recorded extract outcome is SUCCESS")
    source_id = resolve_extract_source_id(value["source_id"])
    item_id = _bound_item_id(value["item_id"])
    title = _bound_title(value["title"])
    url = value["url"]
    if not isinstance(url, str) or not url:
        raise ExtractSignalError("url is required")
    if source_id == X_SEARCH_SOURCE_ID:
        if value["language"] != "en-GB":
            raise ExtractSignalError("X-SEARCH language is en-GB")
        if url != item_id:
            raise ExtractSignalError("X-SEARCH extract uses the admitted URL only")
        host = (urlsplit(url).hostname or "").lower()
        if host not in {"x.com", "www.x.com"}:
            raise ExtractSignalError("X-SEARCH URL host is invalid")
    else:
        if url != SOURCE_URLS[source_id]:
            raise ExtractSignalError("url must match the official Source Definition")
        assert_allowed_url(url)
    digest = value["text_digest"]
    if (
        not isinstance(digest, str)
        or not digest.startswith("sha256:")
        or len(digest) != 71
    ):
        raise ExtractSignalError("text digest is invalid")
    proposals = value["proposals"]
    if not isinstance(proposals, list) or len(proposals) != 1:
        raise ExtractSignalError("exactly one proposal envelope is required")
    canonical_proposal = _canonical_proposal(proposals[0])
    if canonical_proposal["subject_placeholder"] != title:
        raise ExtractSignalError("proposal subject must be the official title")
    if value["proposal_count"] != 1:
        raise ExtractSignalError("proposal count must be one")
    language = value["language"]
    if language not in {"zh-HK", "en-GB"}:
        raise ExtractSignalError("language must be zh-HK or en-GB")
    lowered = json.dumps(value, ensure_ascii=False).lower()
    if "brave" in lowered or "gdelt" in lowered:
        raise ExtractSignalError("Brave and GDELT stay retired")
    if "determinist" in lowered or "fixture_replay" in lowered:
        raise ExtractSignalError("fixture producer is not the live producer")
    return canonical_json_bytes(
        {
            "auto_publish": False,
            "discord": False,
            "item_id": item_id,
            "language": language,
            "outcome": "SUCCESS",
            "producer_kind": LIVE_OFFICIAL_PRODUCER_KIND,
            "proposal_count": 1,
            "proposals": [canonical_proposal],
            "public_adapter": False,
            "source_id": source_id,
            "text_digest": digest,
            "title": title,
            "url": url,
        }
    )


def extract_payload_contract() -> PayloadSchemaContract:
    expected = canonicalize_extract_payload(SAMPLE_PAYLOAD)
    return PayloadSchemaContract(
        schema_version="extraction_live_official_v1",
        payload_mode=PayloadMode.INLINE,
        contract_version="extraction-live-official-contract-v1",
        canonicalizer_implementation_version="extraction-live-official-v1",
        canonicalizer=canonicalize_extract_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="hk04_live_official_title",
                input_identity="grok-bot-live-official-extract-v1",
                value=SAMPLE_PAYLOAD,
                expected_bytes=expected,
            ),
        ),
    )


def extract_command_definition(
    contract: PayloadSchemaContract | None = None,
) -> CommandDefinition:
    selected = contract or extract_payload_contract()
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
        security_scope="authority.extraction",
        retention_scope="authority.extraction",
        required_scope="authority.extraction.execute",
        max_inline_bytes=8192,
    )


def _local_name(element: Any) -> str:
    from lxml import etree

    tag = getattr(element, "tag", None)
    if not isinstance(tag, str):
        return ""
    return etree.QName(element).localname.lower()


def _child_text(element: Any, names: set[str]) -> str:
    for child in element:
        if _local_name(child) not in names:
            continue
        text = clean_official_text("".join(child.itertext()))
        if text:
            return text
        href = child.get("href")
        if isinstance(href, str) and href.strip():
            return href.strip()
    return ""


def _html_text(fragment: str) -> str:
    from lxml import etree, html

    if not fragment:
        return ""
    try:
        return clean_official_text(
            " ".join(html.fromstring(f"<div>{fragment}</div>").itertext())
        )
    except (etree.ParserError, ValueError):
        return clean_official_text(fragment)


def _identity_candidates(item_id: str) -> set[str]:
    candidates = {item_id}
    if "#" in item_id:
        candidates.add(item_id.split("#", 1)[0])
    return candidates


def find_official_feed_item(body: bytes, item_id: str) -> tuple[str, str]:
    from lxml import etree

    if type(body) is not bytes or not body:
        raise ExtractSignalError("official feed body is required")
    lowered = body.lower()
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        raise ExtractSignalError("XML DTD/entity is prohibited")
    parser = etree.XMLParser(
        resolve_entities=False,
        no_network=True,
        dtd_validation=False,
        huge_tree=False,
    )
    try:
        root = etree.fromstring(body, parser=parser)
    except etree.XMLSyntaxError as exc:
        raise ExtractSignalError("RSS/Atom body is malformed") from exc
    wanted = _identity_candidates(item_id)
    for element in root.iter():
        if _local_name(element) not in {"item", "entry"}:
            continue
        guid = _child_text(element, {"guid", "id"})
        link = _child_text(element, {"link"})
        identities = _identity_candidates(guid) | _identity_candidates(link)
        if wanted.isdisjoint(identities):
            continue
        title = _child_text(element, {"title"})
        summary = _html_text(
            _child_text(element, {"description", "summary", "content"})
        )
        if not title:
            raise ExtractSignalError("admitted item has no official title")
        return title, summary
    raise ExtractSignalError("admitted item is missing from the official feed")


def load_admitted_item(path: Path, source_id: str) -> dict[str, str]:
    selected = resolve_extract_source_id(source_id)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        rows = conn.execute(
            "SELECT e.event_type, p.payload_bytes FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type IN (?, ?) ORDER BY e.ledger_seq",
            (SIGNAL_EVENT_TYPE, SKIP_EVENT_TYPE),
        ).fetchall()
    finally:
        conn.close()
    admitted: list[dict[str, Any]] = []
    skipped = False
    for event_type, payload_bytes in rows:
        payload = json.loads(bytes(payload_bytes))
        if not isinstance(payload, dict):
            continue
        if payload.get("source_id") != selected:
            continue
        if event_type == SKIP_EVENT_TYPE:
            skipped = True
            continue
        if event_type == SIGNAL_EVENT_TYPE:
            admitted.append(payload)
    if skipped and not admitted:
        raise ExtractSignalError("UK-10 stays skip; no fake extract")
    if len(admitted) != 1:
        raise ExtractSignalError("admitted item is missing or ambiguous")
    payload = admitted[0]
    item_id = _bound_item_id(payload.get("item_id"))
    url = payload.get("url")
    adapter = payload.get("adapter")
    if selected == X_SEARCH_SOURCE_ID:
        if adapter != GATED_X_SEARCH_ADAPTER:
            raise ExtractSignalError("X-SEARCH extract requires gated_x_search")
        if url != item_id:
            raise ExtractSignalError("X-SEARCH extract uses the admitted URL only")
        return {"adapter": GATED_X_SEARCH_ADAPTER, "item_id": item_id, "url": item_id}
    if adapter != OFFICIAL_RSS_ADAPTER:
        raise ExtractSignalError("extract requires official Source Definition RSS")
    if url != SOURCE_URLS[selected]:
        raise ExtractSignalError("url must match the official Source Definition")
    return {"adapter": OFFICIAL_RSS_ADAPTER, "item_id": item_id, "url": url}


def load_official_extract(
    path: Path,
    source_id: str,
    *,
    extractor: LiveOfficialExtractor | None = None,
) -> dict[str, Any]:
    selected = resolve_extract_source_id(source_id)
    admitted = load_admitted_item(path, selected)
    producer = extractor or LiveOfficialExtractor()
    if type(producer) is DeterministicFixtureExtractor:
        raise ExtractSignalError("fixture producer is not the live producer")
    if getattr(producer, "producer_kind", None) != LIVE_OFFICIAL_PRODUCER_KIND:
        raise ExtractSignalError("fixture producer is not the live producer")
    if selected == X_SEARCH_SOURCE_ID:
        title = admitted["item_id"]
        body = ""
        url = admitted["item_id"]
    else:
        try:
            _loaded_id, url, feed_body = load_official_rss_body(selected)
        except FeedFetchError as exc:
            raise ExtractSignalError(str(exc)) from exc
        if url != admitted["url"]:
            raise ExtractSignalError("fetched feed URL differs from the admitted source")
        title, body = find_official_feed_item(feed_body, admitted["item_id"])
    assert_not_fixture_text(title)
    assert_not_fixture_text(body)
    text, text_digest, language, proposals = producer.produce_from_official_bytes(
        source_id=selected,
        title=title,
        body=body,
    )
    assert_not_fixture_text(text)
    encoded = [
        {
            "confidence_basis_points": item.confidence_basis_points,
            "evidence": [
                {
                    "end_byte": evidence.end_byte,
                    "evidence_text_digest": evidence.evidence_text_digest,
                    "passage_id": str(evidence.passage_id),
                    "start_byte": evidence.start_byte,
                }
                for evidence in item.evidence
            ],
            "kind": item.kind.value,
            "local_id": item.local_id,
            "rationale_codes": list(item.rationale_codes),
            "subject_placeholder": item.subject_placeholder,
        }
        for item in proposals
    ]
    return {
        "auto_publish": False,
        "discord": False,
        "item_id": admitted["item_id"],
        "language": language,
        "outcome": "SUCCESS",
        "producer_kind": LIVE_OFFICIAL_PRODUCER_KIND,
        "proposal_count": len(encoded),
        "proposals": encoded,
        "public_adapter": False,
        "source_id": selected,
        "text_digest": text_digest,
        "title": clean_official_text(title),
        "url": url,
    }


def record_extraction_run(path: Path, *, source_id: str) -> dict[str, Any]:
    from newsroom.host_store import open_host_store

    selected = resolve_extract_source_id(source_id)
    payload = load_official_extract(path, selected)
    canonicalize_extract_payload(payload)
    system = open_host_store(path)
    try:
        system.commands.execute(
            SemanticCommand(
                command_type=COMMAND_TYPE,
                aggregate_id=EXTRACT_AGGREGATE_IDS[selected],
                expected_aggregate_version=0,
                payload=InlinePayload(payload),
                idempotency_key=extract_idempotency_key(selected),
            ),
            proof=AuthenticationProof(
                method="STATIC_TOKEN",
                credential=OWNER_CREDENTIAL,
            ),
        )
    finally:
        system.close()
    return payload


__all__ = [
    "COMMAND_TYPE",
    "EVENT_TYPE",
    "EXTRACT_SOURCE_IDS",
    "LIVE_OFFICIAL_PRODUCER_KIND",
    "LIVE_OFFICIAL_PROFILE",
    "ExtractSignalError",
    "extract_command_definition",
    "extract_payload_contract",
    "find_official_feed_item",
    "load_admitted_item",
    "load_official_extract",
    "record_extraction_run",
    "resolve_extract_source_id",
]
