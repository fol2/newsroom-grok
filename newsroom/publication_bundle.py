"""Mint one HK-01 Publication Bundle with a readable story body.

The authorised digest stays the identity of the story. This command appends a
new ledger event; it does not mutate the digest-only publication decision.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

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
from newsroom.discovery_ingest import (
    EVENT_TYPE as SIGNAL_EVENT_TYPE,
    OFFICIAL_RSS_ADAPTER,
)
from newsroom.envelope_grant import CONTROLLER_ID
from newsroom.increment9.proving import (
    ProvingError,
    assert_allowed_url,
    default_fetch,
)
from newsroom.publication_decision import EVENT_TYPE as DECISION_EVENT_TYPE


CONTROLLER_CREDENTIAL = "agent-turn-controller"
COMMAND_TYPE = "publication.bundle.mint_with_body"
EVENT_TYPE = "publication.bundle.with_body.minted"
AGGREGATE_TYPE = "publication.bundle"
IDEMPOTENCY_KEY = "hk-01-publication-bundle-with-body-v1"
BUNDLE_AGGREGATE_ID = AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa78")
SOURCE_ID = "HK-01"
FEED_URL = "https://www.news.gov.hk/tc/common/html/topstories.rss.xml"
ITEM_URL = (
    "https://www.news.gov.hk/chi/2026/08/20260817/20260817_150017_056.html"
)
BUNDLE_DIGEST = (
    "sha256:c487ece7149f0fcf7afa6808f717f2896d5e89c932d7c08519883b7ae09b1b94"
)
ALLOWED_HOST = "www.news.gov.hk"
MAX_SOURCE_BYTES = 1_048_576
MAX_TITLE_CHARS = 300
MAX_BODY_CHARS = 32_000
MIN_READABLE_BODY_CHARS = 40
RSS_SUMMARY_CHARS = MIN_READABLE_BODY_CHARS
PAYLOAD_KEYS = (
    "auto_publish",
    "bundle_digest",
    "discord",
    "item_url",
    "public_adapter",
    "source_id",
    "story_body",
    "story_title",
    "x_as_publisher",
)
SAMPLE_PAYLOAD: dict[str, Any] = {
    "auto_publish": False,
    "bundle_digest": BUNDLE_DIGEST,
    "discord": False,
    "item_url": ITEM_URL,
    "public_adapter": False,
    "source_id": SOURCE_ID,
    "story_body": (
        "政府公布措施詳情，交代申請安排、合資格人士及推行時間，"
        "並表示會密切監察執行情況。"
    ),
    "story_title": "政府公布措施詳情",
    "x_as_publisher": False,
}


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _exact_official_url(url: object, *, expected: str) -> str:
    if not isinstance(url, str) or url != expected:
        raise ValueError("source URL must be the exact allowed news.gov.hk URL")
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != ALLOWED_HOST
        or parsed.username
        or parsed.password
        or parsed.port
    ):
        raise ValueError("source URL must be the exact allowed news.gov.hk URL")
    lowered = url.lower()
    if "brave" in lowered or "gdelt" in lowered or "x.com" in lowered:
        raise ValueError("Brave, GDELT and user-X stay out of this extract")
    assert_allowed_url(url)
    return url


def _bounded_source(body: bytes, *, kind: str) -> bytes:
    if type(body) is not bytes or not body:
        raise ValueError(f"{kind} body is required")
    if len(body) > MAX_SOURCE_BYTES:
        raise ValueError(f"{kind} body exceeds bound")
    return body


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
        text = _clean("".join(child.itertext()))
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
        return _clean(" ".join(html.fromstring(f"<div>{fragment}</div>").itertext()))
    except (etree.ParserError, ValueError):
        return _clean(fragment)


def _rss_story(body: bytes) -> tuple[str, str] | None:
    from lxml import etree

    raw = _bounded_source(body, kind="RSS")
    lowered = raw.lower()
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        raise ValueError("XML DTD/entity is prohibited")
    parser = etree.XMLParser(
        resolve_entities=False,
        no_network=True,
        dtd_validation=False,
        huge_tree=False,
    )
    try:
        root = etree.fromstring(raw, parser=parser)
    except etree.XMLSyntaxError as exc:
        raise ValueError("RSS body is malformed") from exc
    for item in root.iter():
        if _local_name(item) not in {"item", "entry"}:
            continue
        guid = _child_text(item, {"guid", "id"})
        link = _child_text(item, {"link"})
        if ITEM_URL not in {guid, link}:
            continue
        title = _child_text(item, {"title"})
        summary = _html_text(_child_text(item, {"description", "summary", "content"}))
        return title, summary
    return None


def _page_story(body: bytes) -> tuple[str, str]:
    from lxml import etree, html

    raw = _bounded_source(body, kind="page")
    try:
        try:
            document = html.fromstring(raw.decode("utf-8"))
        except UnicodeDecodeError:
            document = html.fromstring(raw)
    except (etree.ParserError, ValueError) as exc:
        raise ValueError("official page body is malformed") from exc
    titles = document.xpath(
        "//article//h1//text() | //main//h1//text() | //h1//text() | //title//text()"
    )
    title = _clean(" ".join(str(value) for value in titles))
    paragraphs = document.xpath("//article//p | //main//p") or document.xpath(
        "//body//p"
    )
    body_text = "\n\n".join(
        _clean(" ".join(paragraph.itertext())) for paragraph in paragraphs
    )
    return title, body_text.strip()


def _validate_story(title: object, body: object) -> dict[str, str]:
    if not isinstance(title, str) or not isinstance(body, str):
        raise ValueError("story title and body must be text")
    clean_title = _clean(title)
    clean_body = "\n\n".join(
        paragraph for paragraph in (_clean(part) for part in body.split("\n\n")) if paragraph
    )
    if not (4 <= len(clean_title) <= MAX_TITLE_CHARS):
        raise ValueError("story title is missing or unreadable")
    if not (MIN_READABLE_BODY_CHARS <= len(clean_body) <= MAX_BODY_CHARS):
        raise ValueError("story body is missing or too thin")
    if clean_body == BUNDLE_DIGEST or "sha256:" in clean_body.lower():
        raise ValueError("story body must be readable news, not a digest")
    return {"title": clean_title, "body": clean_body}


def extract_hk01_story(
    *,
    rss_body: bytes,
    page_body: bytes | None = None,
) -> dict[str, str]:
    found = _rss_story(rss_body)
    if found is None:
        raise ValueError("admitted HK-01 item is missing from official RSS")
    title, summary = found
    if len(summary) >= RSS_SUMMARY_CHARS:
        return _validate_story(title, summary)
    if page_body is None:
        raise ValueError(
            "official RSS summary is too thin and official page extraction failed"
        )
    page_title, page_text = _page_story(page_body)
    return _validate_story(title or page_title, page_text)


def _load_override(name: str) -> bytes | None:
    value = os.environ.get(name, "").strip()
    if not value:
        return None
    path = Path(value)
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{name} path is unusable")
    return _bounded_source(path.read_bytes(), kind=name)


def _fetch_official(url: str, *, kind: str) -> bytes:
    _exact_official_url(url, expected=url)
    try:
        status, body = default_fetch(url)
    except ProvingError as exc:
        raise ValueError(f"official {kind} fetch failed: {exc}") from exc
    if status != 200:
        raise ValueError(f"official {kind} fetch failed: http-{status}")
    return _bounded_source(body, kind=kind)


def load_official_story() -> dict[str, str]:
    _exact_official_url(FEED_URL, expected=FEED_URL)
    _exact_official_url(ITEM_URL, expected=ITEM_URL)
    rss = _load_override("NEWSROOM_BUNDLE_RSS_BODY_PATH")
    if rss is None:
        rss = _fetch_official(FEED_URL, kind="RSS")
    found = _rss_story(rss)
    if found is None:
        raise ValueError("admitted HK-01 item is missing from official RSS")
    if len(found[1]) >= RSS_SUMMARY_CHARS:
        return _validate_story(*found)
    page = _load_override("NEWSROOM_BUNDLE_PAGE_BODY_PATH")
    if page is None:
        if os.environ.get("NEWSROOM_BUNDLE_RSS_BODY_PATH", "").strip():
            raise ValueError(
                "official RSS summary is too thin and official page extraction failed"
            )
        page = _fetch_official(ITEM_URL, kind="page")
    return extract_hk01_story(rss_body=rss, page_body=page)


def canonicalize_bundle_payload(value: object) -> bytes:
    if not isinstance(value, dict):
        raise ValueError("publication bundle payload must be an object")
    if set(value) != set(PAYLOAD_KEYS):
        raise ValueError("publication bundle payload keys are exact")
    if value["auto_publish"] is not False:
        raise ValueError("AUTO_PUBLISH stays off")
    if value["discord"] is not False:
        raise ValueError("Discord stays off")
    if value["public_adapter"] is not False:
        raise ValueError("public adapters stay off")
    if value["x_as_publisher"] is not False:
        raise ValueError("X-as-publisher stays off")
    if value["source_id"] != SOURCE_ID:
        raise ValueError("source stays HK-01")
    _exact_official_url(value["item_url"], expected=ITEM_URL)
    digest = value["bundle_digest"]
    if not isinstance(digest, str):
        raise ValueError("bundle digest must be canonical text")
    validate_sha256_digest(digest, field="bundle_digest")
    if digest != BUNDLE_DIGEST:
        raise ValueError("bundle digest stays the authorised HK-01 digest")
    story = _validate_story(value["story_title"], value["story_body"])
    return canonical_json_bytes(
        {
            "auto_publish": False,
            "bundle_digest": BUNDLE_DIGEST,
            "discord": False,
            "item_url": ITEM_URL,
            "public_adapter": False,
            "source_id": SOURCE_ID,
            "story_body": story["body"],
            "story_title": story["title"],
            "x_as_publisher": False,
        }
    )


def bundle_payload_contract() -> PayloadSchemaContract:
    expected = canonicalize_bundle_payload(SAMPLE_PAYLOAD)
    return PayloadSchemaContract(
        schema_version="publication_bundle_with_body_v1",
        payload_mode=PayloadMode.INLINE,
        contract_version="publication-bundle-with-body-contract-v1",
        canonicalizer_implementation_version="publication-bundle-with-body-v1",
        canonicalizer=canonicalize_bundle_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="hk01_readable_bundle",
                input_identity="hk01-authorised-digest-v1",
                value=SAMPLE_PAYLOAD,
                expected_bytes=expected,
            ),
        ),
    )


def bundle_command_definition(
    contract: PayloadSchemaContract | None = None,
) -> CommandDefinition:
    selected = contract or bundle_payload_contract()
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
        security_scope="authority.publication",
        retention_scope="authority.publication",
        required_scope="authority.publication.bundle",
        max_inline_bytes=65536,
    )


def load_authorised_hk01_binding(path: Path) -> None:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        rows = conn.execute(
            "SELECT e.event_type, p.payload_bytes FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type IN (?, ?) ORDER BY e.ledger_seq",
            (SIGNAL_EVENT_TYPE, DECISION_EVENT_TYPE),
        ).fetchall()
    finally:
        conn.close()
    signals: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    for event_type, payload_bytes in rows:
        payload = json.loads(bytes(payload_bytes))
        if not isinstance(payload, dict):
            continue
        if event_type == SIGNAL_EVENT_TYPE and payload.get("source_id") == SOURCE_ID:
            signals.append(payload)
        if event_type == DECISION_EVENT_TYPE:
            decisions.append(payload)
    matching = [
        payload
        for payload in signals
        if payload.get("adapter") == OFFICIAL_RSS_ADAPTER
        and payload.get("url") == FEED_URL
        and payload.get("item_id") == ITEM_URL
    ]
    if len(matching) != 1:
        raise ValueError("exact admitted HK-01 item is missing or ambiguous")
    exact = [
        payload for payload in decisions if payload.get("bundle_digest") == BUNDLE_DIGEST
    ]
    if len(exact) != 1:
        raise ValueError(
            "exact authorised HK-01 publication decision is missing or ambiguous"
        )
    decision = exact[0]
    if set(decision) != {
        "authorising",
        "auto_publish",
        "bundle_digest",
        "controller",
        "discord",
        "hermes_publication_admission",
        "public_adapter",
    }:
        raise ValueError("authorised HK-01 decision must remain digest-only")
    if decision.get("authorising") is not True:
        raise ValueError("authorised HK-01 decision must remain authorising")


def record_publication_bundle(path: Path, *, story: dict[str, str]) -> dict[str, Any]:
    from newsroom.host_store import open_host_store

    payload = {
        "auto_publish": False,
        "bundle_digest": BUNDLE_DIGEST,
        "discord": False,
        "item_url": ITEM_URL,
        "public_adapter": False,
        "source_id": SOURCE_ID,
        "story_body": story["body"],
        "story_title": story["title"],
        "x_as_publisher": False,
    }
    system = open_host_store(path)
    try:
        system.commands.execute(
            SemanticCommand(
                command_type=COMMAND_TYPE,
                aggregate_id=BUNDLE_AGGREGATE_ID,
                expected_aggregate_version=0,
                payload=InlinePayload(payload),
                idempotency_key=IDEMPOTENCY_KEY,
            ),
            proof=AuthenticationProof(
                method="STATIC_TOKEN",
                credential=CONTROLLER_CREDENTIAL,
            ),
        )
    finally:
        system.close()
    return payload
