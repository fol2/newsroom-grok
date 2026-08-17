"""Admit one Discovery Signal from gated X search into the host ledger."""

from __future__ import annotations

import json
import os
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


OWNER_CREDENTIAL = "owner-signed"
COMMAND_TYPE = "discovery.x_search.ingest"
EVENT_TYPE = "discovery.signal.admitted"
AGGREGATE_TYPE = "discovery.signal"
IDEMPOTENCY_KEY = "first-x-search-discovery-signal-v1"
SIGNAL_AGGREGATE_ID = AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa63")
GATED_X_SEARCH_ADAPTER = "gated_x_search"
DEFAULT_SOURCE_ID = "X-SEARCH-NEWS"
MAX_BODY_BYTES = 1_048_576
MAX_ITEM_ID_BYTES = 256
X_SEARCH_SOURCE_IDS = frozenset({"X-SEARCH-POSTS", "X-SEARCH-NEWS"})
X_SEARCH_HOSTS = frozenset({"x.com", "www.x.com"})
PAYLOAD_KEYS = (
    "adapter",
    "auto_publish",
    "discord",
    "item_id",
    "public_adapter",
    "source_id",
    "url",
)

SAMPLE_PAYLOAD: dict[str, Any] = {
    "adapter": GATED_X_SEARCH_ADAPTER,
    "auto_publish": False,
    "discord": False,
    "item_id": "x-search-news-first-item",
    "public_adapter": False,
    "source_id": DEFAULT_SOURCE_ID,
    "url": "https://x.com/i/news/x-search-news-first-item",
}


def canonicalize_signal_payload(value: object) -> bytes:
    if not isinstance(value, dict):
        raise ValueError("discovery signal payload must be an object")
    if set(value) != set(PAYLOAD_KEYS):
        raise ValueError("discovery signal payload keys are exact")
    if value["adapter"] != GATED_X_SEARCH_ADAPTER:
        raise ValueError("adapter must be gated X search")
    if value["auto_publish"] is not False:
        raise ValueError("AUTO_PUBLISH stays off")
    if value["discord"] is not False:
        raise ValueError("Discord stays off")
    if value["public_adapter"] is not False:
        raise ValueError("public adapters stay off")
    source_id = value["source_id"]
    if source_id not in X_SEARCH_SOURCE_IDS:
        raise ValueError("source must be X search posts or news")
    url = value["url"]
    assert_x_search_url(url)
    _bound_item_id(value["item_id"])
    return canonical_json_bytes(
        {
            "adapter": GATED_X_SEARCH_ADAPTER,
            "auto_publish": False,
            "discord": False,
            "item_id": value["item_id"],
            "public_adapter": False,
            "source_id": source_id,
            "url": url,
        }
    )


def signal_payload_contract() -> PayloadSchemaContract:
    expected = canonicalize_signal_payload(SAMPLE_PAYLOAD)
    return PayloadSchemaContract(
        schema_version="discovery_x_search_ingest_v1",
        payload_mode=PayloadMode.INLINE,
        contract_version="discovery-x-search-ingest-contract-v1",
        canonicalizer_implementation_version="discovery-x-search-ingest-v1",
        canonicalizer=canonicalize_signal_payload,
        golden_vectors=(
            PayloadGoldenVector(
                name="gated_x_search_first_signal",
                input_identity="grok-bot-x-search-ingest-v1",
                value=SAMPLE_PAYLOAD,
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


def assert_x_search_url(url: object) -> str:
    if not isinstance(url, str) or not url:
        raise ValueError("url must be an X search deeplink")
    parsed = urlsplit(url)
    if parsed.scheme != "https":
        raise ValueError("X search url must be https")
    if parsed.username or parsed.password:
        raise ValueError("X search url must not carry credentials")
    host = (parsed.hostname or "").lower()
    if host not in X_SEARCH_HOSTS:
        raise ValueError("url must be an x.com deeplink")
    if not parsed.path or parsed.path == "/":
        raise ValueError("X search url must include a resource path")
    lowered = url.lower()
    if "brave" in lowered or "gdelt" in lowered:
        raise ValueError("Brave and GDELT stay retired")
    return url


def load_x_search_hit() -> tuple[str, str, str]:
    """Return source_id, url, item_id from a supplied X search hit.

    This process does not call X. Live user-X search is billed and must be
    supplied by the operator after a successful read.
    """
    override = os.environ.get("NEWSROOM_X_SEARCH_BODY_PATH", "").strip()
    if not override:
        raise ValueError(
            "X search hit is required; set NEWSROOM_X_SEARCH_BODY_PATH "
            "(live user-X search is not called from this process)"
        )
    path = Path(override)
    if path.is_symlink() or not path.is_file():
        raise ValueError("X search body path is unusable")
    body = path.read_bytes()
    if not body or len(body) > MAX_BODY_BYTES:
        raise ValueError("X search body exceeds bound")
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("X search body must be JSON") from exc
    return _hit_from_body(parsed)


def record_discovery_signal(
    path: Path,
    *,
    source_id: str,
    url: str,
    item_id: str,
) -> None:
    from newsroom.host_store import open_host_store

    payload = {
        "adapter": GATED_X_SEARCH_ADAPTER,
        "auto_publish": False,
        "discord": False,
        "item_id": item_id,
        "public_adapter": False,
        "source_id": source_id,
        "url": url,
    }
    system = open_host_store(path)
    try:
        system.commands.execute(
            SemanticCommand(
                command_type=COMMAND_TYPE,
                aggregate_id=SIGNAL_AGGREGATE_ID,
                expected_aggregate_version=0,
                payload=InlinePayload(payload),
                idempotency_key=IDEMPOTENCY_KEY,
            ),
            proof=AuthenticationProof(
                method="STATIC_TOKEN",
                credential=OWNER_CREDENTIAL,
            ),
        )
    finally:
        system.close()


def _hit_from_body(value: object) -> tuple[str, str, str]:
    if isinstance(value, list):
        if not value:
            raise ValueError("X search body has no hit")
        value = value[0]
    if not isinstance(value, dict):
        raise ValueError("X search hit must be an object")
    if "hits" in value:
        hits = value["hits"]
        if not isinstance(hits, list) or not hits:
            raise ValueError("X search body has no hit")
        first = hits[0]
        if not isinstance(first, dict):
            raise ValueError("X search hit must be an object")
        value = {**value, **first}
    source_id = value.get("source_id")
    if source_id is None:
        kind = value.get("kind")
        if kind == "posts":
            source_id = "X-SEARCH-POSTS"
        elif kind == "news":
            source_id = "X-SEARCH-NEWS"
    if source_id not in X_SEARCH_SOURCE_IDS:
        raise ValueError("source must be X search posts or news")
    item_id = value.get("item_id") or value.get("id")
    url = value.get("url")
    return str(source_id), assert_x_search_url(url), _bound_item_id(item_id)


def _bound_item_id(value: object) -> str:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise ValueError("item identity must be a single-line token")
    if len(value.encode("utf-8")) > MAX_ITEM_ID_BYTES:
        raise ValueError("item identity exceeds bound")
    return value
