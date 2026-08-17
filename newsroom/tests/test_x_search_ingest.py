"""External X-search ingest seam: one Discovery Signal via xAI x_search."""

from __future__ import annotations

import io
import json
import os
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator

import pytest

from newsroom.discovery_ingest import RSS_SOURCE_IDS
from newsroom.first_boot import SHARED_HOME, ledger_path
from newsroom.increment9.proving import ALLOWED_HOSTS, assert_allowed_url
from newsroom.increment9.qualification import GATE_ID as NONMUTATION_GATE
from newsroom.increment9.shadow_contracts import ProhibitedEffect, _NoEffect
from newsroom.increment10.requalification import EXPECTED_RESIDUAL_GATES
from newsroom.x_search_ingest import (
    GATED_X_SEARCH_ADAPTER,
    PAYLOAD_KEYS,
    SAMPLE_PAYLOAD,
    XAI_MODEL,
    XAI_RESPONSES_URL,
    X_SEARCH_REQUEST,
    X_SEARCH_SOURCE_IDS,
    XSearchFetchError,
    canonicalize_signal_payload,
    load_x_search_hit,
    parse_x_search_hit,
    post_x_search,
    resolve_xai_credential,
    signal_payload_contract,
)


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "newsroom_first_boot.py"

OWNER_PRINCIPAL = "owner.newsroom"
EVENT_TYPE = "discovery.signal.admitted"
ADAPTER = GATED_X_SEARCH_ADAPTER
RETIRED_MARKERS = ("brave", "gdelt", "api.search.brave.com", "gdeltproject.org")
PAUSE_RESTORE_NAME = "restore.paused"
SAMPLE_URL = SAMPLE_PAYLOAD["url"]
SAMPLE_ITEM = SAMPLE_PAYLOAD["item_id"]
SAMPLE_SOURCE = SAMPLE_PAYLOAD["source_id"]
CI_TEST_KEY = "ci-test-oidc-token"
SAMPLE_XAI_RESPONSE = {
    "id": "resp_test_x_search",
    "model": XAI_MODEL,
    "output": [
        {
            "type": "message",
            "role": "assistant",
            "content": [
                {
                    "type": "output_text",
                    "text": (
                        f"{SAMPLE_URL} Official Hong Kong government post."
                    ),
                    "annotations": [
                        {
                            "type": "url_citation",
                            "url": SAMPLE_URL,
                            "start_index": 0,
                            "end_index": len(SAMPLE_URL),
                            "title": "1",
                        }
                    ],
                }
            ],
        }
    ],
    "citations": [SAMPLE_URL],
}


def _cli(
    *args: str,
    home: Path,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    merged["PATH"] = str(home.parent / "bin") + os.pathsep + merged.get("PATH", "")
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args, "--home", str(home)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=merged,
        check=False,
    )


def _install_uv_stub(tmp_path: Path) -> None:
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    record = tmp_path / "uv-record"
    record.mkdir()
    stub = stub_dir / "uv"
    stub.write_text(
        "#!/bin/sh\n"
        "set -e\n"
        f'RECORD="{record}"\n'
        'printf "%s" "$UV_PROJECT_ENVIRONMENT" > "$RECORD/uv_project_environment"\n'
        'printf "%s" "$*" > "$RECORD/uv_args"\n'
        'mkdir -p "$UV_PROJECT_ENVIRONMENT/bin"\n'
        f'ln -sfn "{sys.executable}" "$UV_PROJECT_ENVIRONMENT/bin/python"\n'
        f'ln -sfn "{sys.executable}" "$UV_PROJECT_ENVIRONMENT/bin/python3"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)


def _stop(home: Path, *, pause_restore: bool = False) -> None:
    args = ("stop", "--pause-restore") if pause_restore else ("stop",)
    _cli(*args, home=home)


class _MockXAIHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw.decode("utf-8")) if raw else None
        except json.JSONDecodeError:
            body = None
        self.server.requests.append(
            {
                "path": self.path,
                "authorization": self.headers.get("Authorization"),
                "content_type": self.headers.get("Content-Type"),
                "body": body,
            }
        )
        payload = json.dumps(self.server.response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format: str, *_args: object) -> None:
        return


@pytest.fixture
def mock_xai_http() -> Iterator[tuple[dict[str, str], Any]]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _MockXAIHandler)
    server.response = SAMPLE_XAI_RESPONSE
    server.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    env = {
        "XAI_API_KEY": CI_TEST_KEY,
        "NEWSROOM_XAI_OIDC_TOKEN": CI_TEST_KEY,
        "NEWSROOM_XAI_RESPONSES_URL": f"http://127.0.0.1:{port}/v1/responses",
    }
    try:
        yield env, server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _read_signals(db: Path) -> list[tuple[str, str, dict[str, object]]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            "SELECT e.event_type, e.principal_id, p.payload_bytes "
            "FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type=?",
            (EVENT_TYPE,),
        ).fetchall()
    signals: list[tuple[str, str, dict[str, object]]] = []
    for event_type, principal_id, payload_bytes in rows:
        payload = json.loads(bytes(payload_bytes))
        assert isinstance(payload, dict)
        signals.append((str(event_type), str(principal_id), payload))
    return signals


def test_shared_ledger_path_stays_on_the_box_home() -> None:
    assert SHARED_HOME == Path("/home/box/newsroom")
    assert ledger_path(SHARED_HOME) == Path("/home/box/newsroom/data/authority.sqlite3")


def test_payload_keys_match_rss_discovery_contract() -> None:
    from newsroom.discovery_ingest import PAYLOAD_KEYS as RSS_KEYS

    assert PAYLOAD_KEYS == RSS_KEYS
    assert set(SAMPLE_PAYLOAD) == set(PAYLOAD_KEYS)
    assert SAMPLE_PAYLOAD["adapter"] == ADAPTER
    assert SAMPLE_PAYLOAD["item_id"] == SAMPLE_PAYLOAD["url"]
    assert X_SEARCH_SOURCE_IDS.isdisjoint(RSS_SOURCE_IDS)
    canonicalize_signal_payload(SAMPLE_PAYLOAD)
    contract = signal_payload_contract()
    assert contract.golden_vectors[0].name == "gated_x_search_first_signal"


def test_contract_rejects_rss_ids_outlets_and_write_flags() -> None:
    with pytest.raises(ValueError, match="X search posts or news"):
        canonicalize_signal_payload({**SAMPLE_PAYLOAD, "source_id": "HK-01"})
    with pytest.raises(ValueError, match="X search posts or news"):
        canonicalize_signal_payload({**SAMPLE_PAYLOAD, "source_id": "BBC"})
    with pytest.raises(ValueError, match="gated X search"):
        canonicalize_signal_payload(
            {**SAMPLE_PAYLOAD, "adapter": "official_source_definition_rss"}
        )
    with pytest.raises(ValueError, match="AUTO_PUBLISH"):
        canonicalize_signal_payload({**SAMPLE_PAYLOAD, "auto_publish": True})
    with pytest.raises(ValueError, match="Discord"):
        canonicalize_signal_payload({**SAMPLE_PAYLOAD, "discord": True})
    with pytest.raises(ValueError, match="public adapters"):
        canonicalize_signal_payload({**SAMPLE_PAYLOAD, "public_adapter": True})
    with pytest.raises(ValueError, match="x.com"):
        canonicalize_signal_payload(
            {
                **SAMPLE_PAYLOAD,
                "url": "https://www.news.gov.hk/tc/common/html/topstories.rss.xml",
            }
        )
    with pytest.raises(ValueError, match="Brave"):
        canonicalize_signal_payload(
            {**SAMPLE_PAYLOAD, "url": "https://x.com/i/news/brave-search"}
        )
    with pytest.raises(ValueError, match="source must match"):
        canonicalize_signal_payload(
            {**SAMPLE_PAYLOAD, "source_id": "X-SEARCH-NEWS"}
        )


def test_parse_prefers_status_url_from_annotations_and_text() -> None:
    source_id, url, item_id = parse_x_search_hit(SAMPLE_XAI_RESPONSE)
    assert source_id == SAMPLE_SOURCE
    assert url == SAMPLE_URL
    assert item_id == SAMPLE_URL

    text_only = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": f"See {SAMPLE_URL}",
                    }
                ],
            }
        ]
    }
    assert parse_x_search_hit(text_only) == (SAMPLE_SOURCE, SAMPLE_URL, SAMPLE_URL)

    news_url = "https://x.com/i/news/hk-official-briefing"
    mixed = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": f"{news_url} and a user page",
                        "annotations": [
                            {"type": "url_citation", "url": "https://x.com/i/user/1"},
                            {"type": "url_citation", "url": news_url},
                            {"type": "url_citation", "url": SAMPLE_URL},
                        ],
                    }
                ],
            }
        ]
    }
    assert parse_x_search_hit(mixed) == (SAMPLE_SOURCE, SAMPLE_URL, SAMPLE_URL)

    news_only = {
        "citations": [news_url, "https://x.com/i/user/1"],
        "output": [{"type": "message", "content": [{"type": "output_text", "text": ""}]}],
    }
    assert parse_x_search_hit(news_only) == ("X-SEARCH-NEWS", news_url, news_url)

    with pytest.raises(ValueError, match="no x.com status or news URL"):
        parse_x_search_hit(
            {"output": [{"type": "message", "content": [{"type": "output_text", "text": "none"}]}]}
        )


def test_live_path_posts_xai_x_search_and_ignores_supplied_hit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hit = tmp_path / "hit.json"
    hit.write_text(
        json.dumps({"source_id": "X-SEARCH-NEWS", "item_id": "file-hit", "url": SAMPLE_URL}),
        encoding="utf-8",
    )
    monkeypatch.setenv("NEWSROOM_X_SEARCH_BODY_PATH", str(hit))
    monkeypatch.setenv("NEWSROOM_XAI_OIDC_TOKEN", CI_TEST_KEY)
    captured: dict[str, object] = {}

    def fake_post(credential: str, body: dict[str, object]) -> dict[str, object]:
        captured["credential"] = credential
        captured["body"] = body
        return SAMPLE_XAI_RESPONSE

    monkeypatch.setattr("newsroom.x_search_ingest.post_x_search", fake_post)
    source_id, url, item_id = load_x_search_hit()
    assert captured["credential"] == CI_TEST_KEY
    assert captured["body"] == X_SEARCH_REQUEST
    assert captured["body"]["model"] == "grok-4.6"
    assert captured["body"]["tools"] == [{"type": "x_search"}]
    assert source_id == SAMPLE_SOURCE
    assert url == SAMPLE_URL
    assert item_id == SAMPLE_URL


def test_post_x_search_uses_responses_url_and_does_not_leak_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    class _FakeResponse:
        status = 200

        def read(self, _n: int = -1) -> bytes:
            return json.dumps(SAMPLE_XAI_RESPONSE).encode("utf-8")

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, *_args: object) -> bool:
            return False

    def fake_urlopen(request: object, timeout: object = None) -> _FakeResponse:
        assert isinstance(request, urllib.request.Request)
        seen["url"] = request.full_url
        seen["method"] = request.get_method()
        seen["timeout"] = timeout
        seen["authorization"] = request.get_header("Authorization")
        seen["content_type"] = request.get_header("Content-type")
        return _FakeResponse()

    monkeypatch.delenv("NEWSROOM_XAI_RESPONSES_URL", raising=False)
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    parsed = post_x_search(CI_TEST_KEY, X_SEARCH_REQUEST)
    assert parsed["id"] == "resp_test_x_search"
    assert seen["url"] == XAI_RESPONSES_URL
    assert seen["method"] == "POST"
    assert seen["authorization"] == f"Bearer {CI_TEST_KEY}"
    assert seen["content_type"] == "application/json"
    dumped = json.dumps(seen)
    assert CI_TEST_KEY in dumped
    assert "eyJ" not in dumped


def test_post_x_search_maps_http_error_without_header_leak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(request: object, timeout: object = None) -> object:
        raise urllib.error.HTTPError(
            XAI_RESPONSES_URL,
            403,
            "Forbidden",
            {"Authorization": f"Bearer {CI_TEST_KEY}"},
            io.BytesIO(b'{"error":"no"}'),
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(XSearchFetchError, match="http-403") as excinfo:
        post_x_search(CI_TEST_KEY, X_SEARCH_REQUEST)
    text = str(excinfo.value)
    assert CI_TEST_KEY not in text
    assert "Bearer" not in text


def test_resolve_credential_prefers_env_and_reads_grok_oidc(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XAI_API_KEY", CI_TEST_KEY)
    monkeypatch.delenv("NEWSROOM_XAI_OIDC_TOKEN", raising=False)
    assert resolve_xai_credential(auth_path=tmp_path / "missing.json") == CI_TEST_KEY

    monkeypatch.delenv("XAI_API_KEY", raising=False)
    monkeypatch.setenv("NEWSROOM_XAI_OIDC_TOKEN", CI_TEST_KEY)
    assert resolve_xai_credential(auth_path=tmp_path / "missing.json") == CI_TEST_KEY

    monkeypatch.delenv("NEWSROOM_XAI_OIDC_TOKEN", raising=False)
    missing = tmp_path / "missing.json"
    with pytest.raises(XSearchFetchError, match="xAI credential missing"):
        resolve_xai_credential(auth_path=missing)

    auth = tmp_path / "auth.json"
    auth.write_text(
        json.dumps(
            {
                "https://auth.x.ai::test": {
                    "key": "ci-test-oidc-jwt",
                    "oidc_issuer": "https://auth.x.ai",
                    "email": "not-a-token@example.com",
                }
            }
        ),
        encoding="utf-8",
    )
    assert resolve_xai_credential(auth_path=auth) == "ci-test-oidc-jwt"

    empty = tmp_path / "empty.json"
    empty.write_text("{}", encoding="utf-8")
    with pytest.raises(XSearchFetchError, match="xAI credential missing"):
        resolve_xai_credential(auth_path=empty)

    no_issuer = tmp_path / "no-issuer.json"
    no_issuer.write_text(
        json.dumps({"https://auth.x.ai::test": {"key": "ci-test-oidc-jwt"}}),
        encoding="utf-8",
    )
    with pytest.raises(XSearchFetchError, match="xAI credential missing"):
        resolve_xai_credential(auth_path=no_issuer)


def test_fail_closed_gates_still_hold_on_this_seam() -> None:
    assert NONMUTATION_GATE == "PRODUCTION_NONMUTATION_BASELINE"
    assert "EGRESS_ALLOWLIST_ENFORCED" in EXPECTED_RESIDUAL_GATES
    rights = [gate for gate in EXPECTED_RESIDUAL_GATES if gate.startswith("RIGHTS_")]
    assert len(rights) == 10
    assert set(ProhibitedEffect) >= {
        ProhibitedEffect.PUBLICATION,
        ProhibitedEffect.DISCORD_OR_PUBLIC_DISPATCH,
        ProhibitedEffect.PRODUCTION_AUTHORITY_MUTATION,
    }
    assert _NoEffect.authorises_publication is False
    with pytest.raises(Exception, match="allowlist"):
        assert_allowed_url("https://discord.com/api")
    with pytest.raises(Exception, match="allowlist"):
        assert_allowed_url("https://api.search.brave.com/res/v1/news/search")
    with pytest.raises(Exception, match="allowlist"):
        assert_allowed_url(SAMPLE_URL)
    assert "discord.com" not in ALLOWED_HOSTS
    assert "x.com" not in ALLOWED_HOSTS
    assert "api.x.ai" not in ALLOWED_HOSTS


def test_x_search_signal_is_absent_after_envelope_grant_only(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        db = ledger_path(home)
        assert _read_signals(db) == []
        with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
            assert conn.execute("SELECT COUNT(*) FROM discovery_signals").fetchone()[0] == 0
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM ledger_events WHERE event_type=?",
                    (EVENT_TYPE,),
                ).fetchone()[0]
                == 0
            )
    finally:
        _stop(home)


def test_ingest_x_search_records_exactly_one_authorised_discovery_signal(
    tmp_path: Path,
    mock_xai_http: tuple[dict[str, str], Any],
) -> None:
    env, server = mock_xai_http
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr

        ingested = _cli("ingest-x-search", home=home, env=env)
        assert ingested.returncode == 0, ingested.stderr
        report = json.loads(ingested.stdout)
        assert report["ok"] is True
        assert report["adapter"] == ADAPTER
        assert report["auto_publish"] is False
        assert report["discord"] is False
        assert report["public_adapter"] is False
        assert report["x_as_publisher"] is False
        assert report["ledger_path"] == str(ledger_path(home))
        assert report["source_id"] == SAMPLE_SOURCE
        assert report["url"] == SAMPLE_URL
        assert report["item_id"] == SAMPLE_URL
        assert report["event_type"] == EVENT_TYPE
        assert CI_TEST_KEY not in ingested.stdout
        assert "Bearer" not in ingested.stdout

        assert len(server.requests) == 1
        posted = server.requests[0]
        assert posted["authorization"] == f"Bearer {CI_TEST_KEY}"
        assert posted["body"] == X_SEARCH_REQUEST
        assert posted["body"]["tools"] == [{"type": "x_search"}]
        assert posted["body"]["model"] == "grok-4.6"

        signals = _read_signals(ledger_path(home))
        assert len(signals) == 1
        event_type, principal_id, payload = signals[0]
        assert event_type == EVENT_TYPE
        assert principal_id == OWNER_PRINCIPAL
        assert payload["adapter"] == ADAPTER
        assert payload["source_id"] in X_SEARCH_SOURCE_IDS
        assert payload["source_id"] not in RSS_SOURCE_IDS
        assert payload["auto_publish"] is False
        assert payload["discord"] is False
        assert payload["public_adapter"] is False
        assert payload["url"] == SAMPLE_URL
        assert payload["item_id"] == SAMPLE_URL
        raw = json.dumps(payload).lower()
        assert all(marker not in raw for marker in RETIRED_MARKERS)
        assert str(payload["url"]).startswith("https://x.com/")
        assert "/status/" in str(payload["url"])
        assert CI_TEST_KEY not in raw

        with sqlite3.connect(f"file:{ledger_path(home)}?mode=ro", uri=True) as conn:
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM ledger_events WHERE event_type=?",
                    (EVENT_TYPE,),
                ).fetchone()[0]
                == 1
            )
            news_leads = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='news_leads'"
            ).fetchone()
            if news_leads is not None:
                assert conn.execute("SELECT COUNT(*) FROM news_leads").fetchone()[0] == 0

        again = _cli("ingest-x-search", home=home, env=env)
        assert again.returncode == 0, again.stderr
        assert len(_read_signals(ledger_path(home))) == 1
    finally:
        _stop(home)


def test_emergency_stop_vetoes_x_search_ingest(
    tmp_path: Path,
    mock_xai_http: tuple[dict[str, str], Any],
) -> None:
    env, _server = mock_xai_http
    home = tmp_path / "newsroom"
    pause = home / "logs" / PAUSE_RESTORE_NAME
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        stopped = _cli("stop", "--pause-restore", home=home)
        assert stopped.returncode == 0, stopped.stderr
        assert pause.is_file()

        refused = _cli("ingest-x-search", home=home, env=env)
        assert refused.returncode != 0
        report = json.loads(refused.stdout)
        assert report["ok"] is False
        error = report["error"].lower()
        assert "pause" in error or "emergency stop" in error
        assert "resume" in error
        assert _read_signals(ledger_path(home)) == []
        health = json.loads(_cli("health", home=home).stdout)
        assert health["process_up"] is False
        assert pause.is_file()
    finally:
        _stop(home, pause_restore=True)


def test_emergency_stop_still_holds_after_x_search_ingest(
    tmp_path: Path,
    mock_xai_http: tuple[dict[str, str], Any],
) -> None:
    env, _server = mock_xai_http
    home = tmp_path / "newsroom"
    pause = home / "logs" / PAUSE_RESTORE_NAME
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        ingested = _cli("ingest-x-search", home=home, env=env)
        assert ingested.returncode == 0, ingested.stderr
        assert len(_read_signals(ledger_path(home))) == 1

        stopped = _cli("stop", "--pause-restore", home=home)
        assert stopped.returncode == 0, stopped.stderr
        health = json.loads(_cli("health", home=home).stdout)
        assert health["process_up"] is False
        assert health["pid"] is None
        assert pause.is_file()

        time.sleep(1.2)
        still = json.loads(_cli("health", home=home).stdout)
        assert still["process_up"] is False
        assert pause.is_file()

        auto = _cli("start", home=home)
        assert auto.returncode != 0
        refused = json.loads(_cli("health", home=home).stdout)
        assert refused["process_up"] is False
        assert pause.is_file()
        assert len(_read_signals(ledger_path(home))) == 1
    finally:
        _stop(home, pause_restore=True)
