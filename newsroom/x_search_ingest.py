"""Admit one Discovery Signal from Grok-only xAI x_search into the host ledger.

Live path: POST https://api.x.ai/v1/responses with model grok-4.6 and
tools [{"type": "x_search"}]. Auth is grok.com OIDC from Grok Build,
or XAI_API_KEY if present. Not MCP user-X, not a supplied hit.json,
not X-as-publisher.
"""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
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
SIGNAL_AGGREGATE_ID = AggregateId.parse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa80")
GATED_X_SEARCH_ADAPTER = "gated_x_search"
DEFAULT_SOURCE_ID = "X-SEARCH-POSTS"
MAX_BODY_BYTES = 1_048_576
MAX_ITEM_ID_BYTES = 256
X_SEARCH_SOURCE_IDS = frozenset({"X-SEARCH-POSTS", "X-SEARCH-NEWS"})
X_SEARCH_HOSTS = frozenset({"x.com", "www.x.com"})
XAI_RESPONSES_URL = "https://api.x.ai/v1/responses"
XAI_MODEL = "grok-4.6"
GROK_AUTH_PATH = Path("/home/box/.grok/auth.json")
HTTP_TIMEOUT_SECONDS = 120
USER_AGENT = "Newsroom-XSearch-Ingest/1.0"
X_SEARCH_PROMPT = (
    "Search X for one recent official Hong Kong government post. "
    "Return only the post URL and a one-line summary."
)
X_SEARCH_REQUEST: dict[str, Any] = {
    "model": XAI_MODEL,
    "input": [{"role": "user", "content": X_SEARCH_PROMPT}],
    "tools": [{"type": "x_search"}],
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
_STATUS_URL_RE = re.compile(
    r"https://(?:www\.)?x\.com/(?:[A-Za-z0-9_]+|i)/status/\d+(?:\?[^\s)\"'<>]*)?",
    re.IGNORECASE,
)
_NEWS_URL_RE = re.compile(
    r"https://(?:www\.)?x\.com/i/news/[A-Za-z0-9_-]+(?:\?[^\s)\"'<>]*)?",
    re.IGNORECASE,
)

SAMPLE_PAYLOAD: dict[str, Any] = {
    "adapter": GATED_X_SEARCH_ADAPTER,
    "auto_publish": False,
    "discord": False,
    "item_id": "https://x.com/newsgovhk/status/2089334709910491610",
    "public_adapter": False,
    "source_id": DEFAULT_SOURCE_ID,
    "url": "https://x.com/newsgovhk/status/2089334709910491610",
}


class XSearchFetchError(ValueError):
    """xAI x_search fetch failed; do not invent a Discovery Signal."""


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
    url = assert_x_search_url(value["url"])
    if source_id != source_id_for_url(url):
        raise ValueError("source must match the X search URL kind")
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
        raise ValueError("url must be an X status or news deeplink")
    parsed = urlsplit(url)
    if parsed.scheme != "https":
        raise ValueError("X search url must be https")
    if parsed.username or parsed.password:
        raise ValueError("X search url must not carry credentials")
    host = (parsed.hostname or "").lower()
    if host not in X_SEARCH_HOSTS:
        raise ValueError("url must be an x.com deeplink")
    path = parsed.path or ""
    status = "/status/" in path.lower()
    news = "/i/news/" in path.lower()
    if not status and not news:
        raise ValueError("url must be an X status or news deeplink")
    lowered = url.lower()
    if "brave" in lowered or "gdelt" in lowered:
        raise ValueError("Brave and GDELT stay retired")
    return url


def source_id_for_url(url: str) -> str:
    path = urlsplit(assert_x_search_url(url)).path.lower()
    if "/status/" in path:
        return "X-SEARCH-POSTS"
    if "/i/news/" in path:
        return "X-SEARCH-NEWS"
    raise ValueError("url must be an X status or news deeplink")


def resolve_responses_url() -> str:
    override = os.environ.get("NEWSROOM_XAI_RESPONSES_URL", "").strip()
    if not override:
        return XAI_RESPONSES_URL
    parsed = urlsplit(override)
    host = (parsed.hostname or "").lower()
    if host in {"127.0.0.1", "localhost", "::1"}:
        return override
    if parsed.scheme == "https" and host == "api.x.ai":
        return override
    raise XSearchFetchError("X search responses URL is not the xAI Responses API")


def resolve_xai_credential(*, auth_path: Path | None = None) -> str:
    env_key = os.environ.get("XAI_API_KEY", "").strip()
    if env_key:
        return env_key
    env_key = os.environ.get("NEWSROOM_XAI_OIDC_TOKEN", "").strip()
    if env_key:
        return env_key
    if auth_path is not None:
        path = Path(auth_path)
    else:
        override = os.environ.get("NEWSROOM_GROK_AUTH_PATH", "").strip()
        path = Path(override).expanduser() if override else GROK_AUTH_PATH
    if path.is_symlink() or not path.is_file():
        raise XSearchFetchError(
            "xAI credential missing: no XAI_API_KEY and no grok.com OIDC"
        )
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise XSearchFetchError(
            "xAI credential missing: grok.com OIDC unreadable"
        ) from exc
    if not isinstance(parsed, dict):
        raise XSearchFetchError(
            "xAI credential missing: grok.com OIDC unreadable"
        )
    for value in parsed.values():
        if not isinstance(value, dict):
            continue
        key = value.get("key")
        issuer = value.get("oidc_issuer")
        if isinstance(key, str) and key.strip() and isinstance(issuer, str) and issuer:
            return key.strip()
    raise XSearchFetchError(
        "xAI credential missing: grok.com OIDC unreadable"
    )


def post_x_search(credential: str, body: dict[str, Any]) -> dict[str, Any]:
    if type(credential) is not str or not credential:
        raise XSearchFetchError(
            "xAI credential missing: no XAI_API_KEY and no grok.com OIDC"
        )
    if body.get("tools") != [{"type": "x_search"}]:
        raise XSearchFetchError("xAI request must use tools x_search")
    payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        resolve_responses_url(),
        data=payload,
        method="POST",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {credential}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            status = int(getattr(response, "status", 200))
            raw = response.read(MAX_BODY_BYTES + 1)
    except urllib.error.HTTPError as exc:
        raise XSearchFetchError(f"xAI x_search failed: http-{int(exc.code)}") from None
    except (urllib.error.URLError, TimeoutError, OSError):
        raise XSearchFetchError("xAI x_search failed: network") from None
    if status != 200:
        raise XSearchFetchError(f"xAI x_search failed: http-{status}")
    if not raw or len(raw) > MAX_BODY_BYTES:
        raise XSearchFetchError("xAI x_search body exceeds bound")
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise XSearchFetchError("xAI x_search body must be JSON") from exc
    if not isinstance(parsed, dict):
        raise XSearchFetchError("xAI x_search body must be an object")
    return parsed


def parse_x_search_hit(response: object) -> tuple[str, str, str]:
    if not isinstance(response, dict):
        raise ValueError("xAI x_search response must be an object")
    candidates = _collect_x_urls(response)
    chosen = _first_status_or_news(candidates)
    if chosen is None:
        raise ValueError("xAI x_search response has no x.com status or news URL")
    url = assert_x_search_url(chosen)
    source_id = source_id_for_url(url)
    return source_id, url, _bound_item_id(url)


def load_x_search_hit() -> tuple[str, str, str]:
    """POST xAI x_search and parse one x.com status URL. Tests mock HTTP."""
    credential = resolve_xai_credential()
    return parse_x_search_hit(post_x_search(credential, X_SEARCH_REQUEST))


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


def _collect_x_urls(value: object) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        annotation_url = value.get("url")
        if value.get("type") == "url_citation" and isinstance(annotation_url, str):
            found.append(annotation_url)
        citations = value.get("citations")
        if isinstance(citations, list):
            for item in citations:
                if isinstance(item, str):
                    found.append(item)
                elif isinstance(item, dict) and isinstance(item.get("url"), str):
                    found.append(item["url"])
        for item in value.values():
            found.extend(_collect_x_urls(item))
    elif isinstance(value, list):
        for item in value:
            found.extend(_collect_x_urls(item))
    elif isinstance(value, str):
        found.extend(_STATUS_URL_RE.findall(value))
        found.extend(_NEWS_URL_RE.findall(value))
    return found


def _first_status_or_news(candidates: list[str]) -> str | None:
    status: str | None = None
    news: str | None = None
    for raw in candidates:
        try:
            url = assert_x_search_url(raw)
        except ValueError:
            continue
        kind = source_id_for_url(url)
        if kind == "X-SEARCH-POSTS" and status is None:
            status = url
        elif kind == "X-SEARCH-NEWS" and news is None:
            news = url
    return status or news


def _bound_item_id(value: object) -> str:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise ValueError("item identity must be a single-line token")
    if len(value.encode("utf-8")) > MAX_ITEM_ID_BYTES:
        raise ValueError("item identity exceeds bound")
    return value
