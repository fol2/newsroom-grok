"""Admit one Discovery Signal from official Source Definition RSS into the host ledger."""

from __future__ import annotations

import os
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
    digest_bytes,
)
from newsroom.increment9.proving import (
    SOURCE_URLS,
    ProvingError,
    assert_allowed_url,
    default_fetch,
)


OWNER_CREDENTIAL = "owner-signed"
COMMAND_TYPE = "discovery.signal.ingest"
EVENT_TYPE = "discovery.signal.admitted"
SKIP_COMMAND_TYPE = "discovery.signal.skip"
SKIP_EVENT_TYPE = "discovery.signal.skipped"
AGGREGATE_TYPE = "discovery.signal"
SKIP_AGGREGATE_TYPE = "discovery.signal.skip"
IDEMPOTENCY_KEY = "first-production-discovery-signal-v1"
SIGNAL_AGGREGATE_ID = AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa29")
OFFICIAL_RSS_ADAPTER = "official_source_definition_rss"
DEFAULT_SOURCE_ID = "HK-01"
MAX_BODY_BYTES = 1_048_576
MAX_ITEM_ID_BYTES = 256
MAX_REASON_BYTES = 256
RSS_SOURCE_IDS = frozenset(
    {"UK-01", "UK-05", "UK-10", "HK-01", "HK-04", "RAD-01", "RAD-02"}
)
SIGNAL_AGGREGATE_IDS = {
    "HK-01": SIGNAL_AGGREGATE_ID,
    "HK-04": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa61"),
    "RAD-01": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa62"),
    "RAD-02": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa63"),
    "UK-01": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa64"),
    "UK-05": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa65"),
    "UK-10": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa66"),
}
SKIP_AGGREGATE_IDS = {
    "HK-01": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa71"),
    "HK-04": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa72"),
    "RAD-01": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa73"),
    "RAD-02": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa74"),
    "UK-01": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa75"),
    "UK-05": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa76"),
    "UK-10": AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa77"),
}
PAYLOAD_KEYS = (
    "adapter",
    "auto_publish",
    "discord",
    "item_id",
    "public_adapter",
    "source_id",
    "url",
)
SKIP_PAYLOAD_KEYS = (
    "adapter",
    "auto_publish",
    "discord",
    "public_adapter",
    "reason",
    "source_id",
    "url",
)

SAMPLE_PAYLOAD: dict[str, Any] = {
    "adapter": OFFICIAL_RSS_ADAPTER,
    "auto_publish": False,
    "discord": False,
    "item_id": "hk-01-first-item",
    "public_adapter": False,
    "source_id": DEFAULT_SOURCE_ID,
    "url": SOURCE_URLS[DEFAULT_SOURCE_ID],
}
SAMPLE_SKIP_PAYLOAD: dict[str, Any] = {
    "adapter": OFFICIAL_RSS_ADAPTER,
    "auto_publish": False,
    "discord": False,
    "public_adapter": False,
    "reason": "official RSS fetch failed: http-503",
    "source_id": "HK-04",
    "url": SOURCE_URLS["HK-04"],
}


class FeedFetchError(ValueError):
    """Official RSS/Atom fetch failed; record a skip instead of inventing a signal."""


def resolve_rss_source_id(source_id: str | None) -> str:
    selected = DEFAULT_SOURCE_ID if source_id is None else source_id
    if not isinstance(selected, str) or selected not in RSS_SOURCE_IDS:
        raise ValueError("source must be an official Source Definition RSS endpoint")
    return selected


def signal_idempotency_key(source_id: str) -> str:
    selected = resolve_rss_source_id(source_id)
    if selected == DEFAULT_SOURCE_ID:
        return IDEMPOTENCY_KEY
    return f"production-discovery-signal-{selected}-v1"


def skip_idempotency_key(source_id: str) -> str:
    selected = resolve_rss_source_id(source_id)
    return f"production-discovery-signal-skip-{selected}-v1"


def canonicalize_signal_payload(value: object) -> bytes:
    if not isinstance(value, dict):
        raise ValueError("discovery signal payload must be an object")
    if set(value) != set(PAYLOAD_KEYS):
        raise ValueError("discovery signal payload keys are exact")
    if value["adapter"] != OFFICIAL_RSS_ADAPTER:
        raise ValueError("adapter must be official Source Definition RSS")
    if value["auto_publish"] is not False:
        raise ValueError("AUTO_PUBLISH stays off")
    if value["discord"] is not False:
        raise ValueError("Discord stays off")
    if value["public_adapter"] is not False:
        raise ValueError("public adapters stay off")
    source_id = resolve_rss_source_id(value["source_id"])
    url = value["url"]
    if not isinstance(url, str) or url != SOURCE_URLS[source_id]:
        raise ValueError("url must match the official Source Definition")
    assert_allowed_url(url)
    _bound_item_id(value["item_id"])
    lowered = url.lower()
    if "brave" in lowered or "gdelt" in lowered:
        raise ValueError("Brave and GDELT stay retired")
    return canonical_json_bytes(
        {
            "adapter": OFFICIAL_RSS_ADAPTER,
            "auto_publish": False,
            "discord": False,
            "item_id": value["item_id"],
            "public_adapter": False,
            "source_id": source_id,
            "url": url,
        }
    )


def canonicalize_skip_payload(value: object) -> bytes:
    if not isinstance(value, dict):
        raise ValueError("discovery skip payload must be an object")
    if set(value) != set(SKIP_PAYLOAD_KEYS):
        raise ValueError("discovery skip payload keys are exact")
    if value["adapter"] != OFFICIAL_RSS_ADAPTER:
        raise ValueError("adapter must be official Source Definition RSS")
    if value["auto_publish"] is not False:
        raise ValueError("AUTO_PUBLISH stays off")
    if value["discord"] is not False:
        raise ValueError("Discord stays off")
    if value["public_adapter"] is not False:
        raise ValueError("public adapters stay off")
    source_id = resolve_rss_source_id(value["source_id"])
    url = value["url"]
    if not isinstance(url, str) or url != SOURCE_URLS[source_id]:
        raise ValueError("url must match the official Source Definition")
    assert_allowed_url(url)
    _bound_reason(value["reason"])
    lowered = url.lower()
    if "brave" in lowered or "gdelt" in lowered:
        raise ValueError("Brave and GDELT stay retired")
    return canonical_json_bytes(
        {
            "adapter": OFFICIAL_RSS_ADAPTER,
            "auto_publish": False,
            "discord": False,
            "public_adapter": False,
            "reason": value["reason"],
            "source_id": source_id,
            "url": url,
        }
    )


def signal_payload_contract() -> PayloadSchemaContract:
    expected = canonicalize_signal_payload(SAMPLE_PAYLOAD)
    return PayloadSchemaContract(
        schema_version="discovery_signal_ingest_v1",
        payload_mode=PayloadMode.INLINE,
        contract_version="discovery-signal-ingest-contract-v1",
        canonicalizer_implementation_version="discovery-signal-ingest-v1",
        canonicalizer=canonicalize_signal_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="official_rss_first_signal",
                input_identity="grok-bot-first-ingest-v1",
                value=SAMPLE_PAYLOAD,
                expected_bytes=expected,
            ),
        ),
    )


def skip_payload_contract() -> PayloadSchemaContract:
    expected = canonicalize_skip_payload(SAMPLE_SKIP_PAYLOAD)
    return PayloadSchemaContract(
        schema_version="discovery_signal_skip_v1",
        payload_mode=PayloadMode.INLINE,
        contract_version="discovery-signal-skip-contract-v1",
        canonicalizer_implementation_version="discovery-signal-skip-v1",
        canonicalizer=canonicalize_skip_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="official_rss_fetch_skip",
                input_identity="grok-bot-remaining-rss-skip-v1",
                value=SAMPLE_SKIP_PAYLOAD,
                expected_bytes=expected,
            ),
        ),
    )


def signal_command_definition(
    contract: PayloadSchemaContract | None = None,
) -> CommandDefinition:
    selected = contract or signal_payload_contract()
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
        security_scope="authority.discovery",
        retention_scope="authority.discovery",
        required_scope="authority.discovery.ingest",
        max_inline_bytes=4096,
    )


def skip_command_definition(
    contract: PayloadSchemaContract | None = None,
) -> CommandDefinition:
    selected = contract or skip_payload_contract()
    return CommandDefinition(
        command_type=SKIP_COMMAND_TYPE,
        definition_version="v1",
        aggregate_type=SKIP_AGGREGATE_TYPE,
        event_type=SKIP_EVENT_TYPE,
        event_schema_version=1,
        payload_mode=PayloadMode.INLINE,
        payload_schema_version=selected.schema_version,
        payload_schema_contract_version=selected.contract_version,
        payload_schema_contract_digest=selected.contract_digest,
        payload_canonicalizer_version=selected.canonicalizer_implementation_version,
        trust_scope=TrustScope.OBSERVED,
        security_scope="authority.discovery",
        retention_scope="authority.discovery",
        required_scope="authority.discovery.ingest",
        max_inline_bytes=4096,
    )


def all_feed_item_ids(body: bytes) -> tuple[str, ...]:
    from lxml import etree

    if type(body) is not bytes or not body:
        raise ValueError("RSS body is required")
    if len(body) > MAX_BODY_BYTES:
        raise ValueError("RSS body exceeds bound")
    lowered = body.lower()
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        raise ValueError("XML DTD/entity is prohibited")
    parser = etree.XMLParser(
        resolve_entities=False,
        no_network=True,
        dtd_validation=False,
        huge_tree=False,
    )
    try:
        root = etree.fromstring(body, parser=parser)
    except etree.XMLSyntaxError as exc:
        raise ValueError("RSS/Atom body is malformed") from exc
    found: list[str] = []
    for element in root.iter():
        name = _local_name(element)
        if name == "item":
            found.append(_rss_item_id(element))
        elif name == "entry":
            found.append(_atom_entry_id(element))
    return tuple(found)


def first_feed_item_id(body: bytes) -> str:
    items = all_feed_item_ids(body)
    if not items:
        raise ValueError("RSS/Atom feed has no item")
    return items[0]


def load_official_rss_body(source_id: str | None = None) -> tuple[str, str, bytes]:
    selected = resolve_rss_source_id(source_id)
    url = SOURCE_URLS[selected]
    assert_allowed_url(url)
    forced_status = os.environ.get("NEWSROOM_INGEST_HTTP_STATUS", "").strip()
    if forced_status:
        raise FeedFetchError(f"official RSS fetch failed: http-{forced_status}")
    override = os.environ.get("NEWSROOM_INGEST_BODY_PATH", "").strip()
    if override:
        path = Path(override)
        if path.is_symlink() or not path.is_file():
            raise ValueError("ingest body path is unusable")
        body = path.read_bytes()
        if len(body) > MAX_BODY_BYTES:
            raise ValueError("RSS body exceeds bound")
        return selected, url, body
    try:
        status, body = default_fetch(url)
    except ProvingError as exc:
        raise FeedFetchError(f"official RSS fetch failed: {exc}") from exc
    if status != 200:
        raise FeedFetchError(f"official RSS fetch failed: http-{status}")
    return selected, url, body


def record_discovery_signal(
    path: Path,
    *,
    source_id: str,
    url: str,
    item_id: str,
) -> None:
    from newsroom.host_store import open_host_store

    selected = resolve_rss_source_id(source_id)
    payload = {
        "adapter": OFFICIAL_RSS_ADAPTER,
        "auto_publish": False,
        "discord": False,
        "item_id": item_id,
        "public_adapter": False,
        "source_id": selected,
        "url": url,
    }
    system = open_host_store(path)
    try:
        system.commands.execute(
            SemanticCommand(
                command_type=COMMAND_TYPE,
                aggregate_id=SIGNAL_AGGREGATE_IDS[selected],
                expected_aggregate_version=0,
                payload=InlinePayload(payload),
                idempotency_key=signal_idempotency_key(selected),
            ),
            proof=AuthenticationProof(
                method="STATIC_TOKEN",
                credential=OWNER_CREDENTIAL,
            ),
        )
    finally:
        system.close()


def record_discovery_skip(
    path: Path,
    *,
    source_id: str,
    url: str,
    reason: str,
) -> None:
    from newsroom.host_store import open_host_store

    selected = resolve_rss_source_id(source_id)
    payload = {
        "adapter": OFFICIAL_RSS_ADAPTER,
        "auto_publish": False,
        "discord": False,
        "public_adapter": False,
        "reason": reason,
        "source_id": selected,
        "url": url,
    }
    system = open_host_store(path)
    try:
        system.commands.execute(
            SemanticCommand(
                command_type=SKIP_COMMAND_TYPE,
                aggregate_id=SKIP_AGGREGATE_IDS[selected],
                expected_aggregate_version=0,
                payload=InlinePayload(payload),
                idempotency_key=skip_idempotency_key(selected),
            ),
            proof=AuthenticationProof(
                method="STATIC_TOKEN",
                credential=OWNER_CREDENTIAL,
            ),
        )
    finally:
        system.close()


def _bound_item_id(value: object) -> str:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise ValueError("item identity must be a single-line token")
    if len(value.encode("utf-8")) > MAX_ITEM_ID_BYTES:
        raise ValueError("item identity exceeds bound")
    return value


def _bound_reason(value: object) -> str:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise ValueError("skip reason must be a single-line token")
    if len(value.encode("utf-8")) > MAX_REASON_BYTES:
        raise ValueError("skip reason exceeds bound")
    return value


def _local_name(element: Any) -> str:
    from lxml import etree

    tag = getattr(element, "tag", None)
    if not isinstance(tag, str):
        return ""
    return etree.QName(element).localname.lower()


def _child_text(element: Any, local: str) -> str:
    for child in element:
        if _local_name(child) != local:
            continue
        text = "".join(child.itertext()).strip()
        if text:
            return text
        href = child.get("href")
        if isinstance(href, str) and href.strip():
            return href.strip()
    return ""


def _rss_item_id(item: Any) -> str:
    for field in ("guid", "link", "title"):
        value = _child_text(item, field)
        if value:
            return _item_id_for_ledger(value)
    raise ValueError("RSS item has no identity")


def _atom_entry_id(entry: Any) -> str:
    for field in ("id", "link", "title"):
        value = _child_text(entry, field)
        if value:
            return _item_id_for_ledger(value)
    raise ValueError("Atom entry has no identity")


def _item_id_for_ledger(value: str) -> str:
    if len(value.encode("utf-8")) > MAX_ITEM_ID_BYTES:
        return digest_bytes(value.encode("utf-8"))
    return _bound_item_id(value)
