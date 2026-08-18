"""Live Increment 4A extract on admitted item_id-only sources."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from newsroom.discovery_ingest import (
    SOURCE_URLS,
    record_discovery_signal,
    record_discovery_skip,
)
from newsroom.envelope_grant import record_envelope_grant
from newsroom.extraction.fixtures import FIXTURE_EN_TEXT, FIXTURE_ZH_HK_TEXT
from newsroom.extraction.live_official import (
    LIVE_OFFICIAL_PRODUCER_KIND,
    LiveOfficialExtractor,
    official_passage_text,
)
from newsroom.extraction.producer import DeterministicFixtureExtractor
from newsroom.extraction.types import ExtractionContractError
from newsroom.extraction_signal import (
    EVENT_TYPE,
    EXTRACT_SOURCE_IDS,
    ExtractSignalError,
    canonicalize_extract_payload,
    extract_payload_contract,
    find_official_feed_item,
    load_official_extract,
    record_extraction_run,
    resolve_extract_source_id,
)
from newsroom.first_boot import SHARED_HOME, ledger_path
from newsroom.x_search_ingest import record_discovery_signal as record_x_search_signal


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "newsroom_first_boot.py"
HK04_ITEM = (
    "https://www.edb.gov.hk/tc/student-parents/parents-related/"
    "ebulletin-for-parents/2025-2026/20260814.html"
)
RAD01_ITEM = "https://news.rthk.hk/rthk/ch/component/k2/1866503-20260817.htm"
RAD02_ITEM = "https://www.bbc.co.uk/news/articles/c4g3re5ew8do#0"
UK01_ITEM = "tag:www.gov.uk,2005:/government/publications/form-bota-guidance"
UK05_ITEM = "tag:www.gov.uk,2005:/guidance/special-educational-needs-survey"
X_URL = "https://x.com/newsgovhk/status/2089334709910491610"
BUNDLE_EVENT = "publication.bundle.with_body.minted"

HK04_FEED = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    "<rss version=\"2.0\"><channel>"
    f"<item><guid>{HK04_ITEM}</guid>"
    "<title>家長電子通訊</title>"
    "<description>教育局公布家長電子通訊最新一期。</description>"
    f"<link>{HK04_ITEM}</link>"
    "</item></channel></rss>\n"
)
RAD02_FEED = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    "<rss version=\"2.0\"><channel>"
    "<item><guid>https://www.bbc.co.uk/news/articles/c4g3re5ew8do</guid>"
    "<title>UK official update</title>"
    "<description>A recorded BBC UK item used only as extract bytes.</description>"
    "<link>https://www.bbc.co.uk/news/articles/c4g3re5ew8do#0</link>"
    "</item></channel></rss>\n"
)
UK01_FEED = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<feed xmlns="http://www.w3.org/2005/Atom">'
    f"<entry><id>{UK01_ITEM}</id>"
    "<title>Form BOTA guidance</title>"
    "<summary>Recorded GOV.UK Atom entry for extract tests.</summary>"
    "</entry></feed>\n"
)
EMPTY_FEED = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    "<rss version=\"2.0\"><channel>"
    "<item><guid>https://www.edb.gov.hk/other</guid>"
    "<title>Other item</title>"
    "</item></channel></rss>\n"
)


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


def _write_feed(tmp_path: Path, body: str) -> dict[str, str]:
    feed = tmp_path / "official-rss.xml"
    feed.write_text(body, encoding="utf-8")
    return {"NEWSROOM_INGEST_BODY_PATH": str(feed)}


def _seed_ledger(db: Path) -> None:
    record_envelope_grant(db)
    record_discovery_signal(
        db,
        source_id="HK-04",
        url=SOURCE_URLS["HK-04"],
        item_id=HK04_ITEM,
    )


def _events(db: Path, event_type: str) -> list[dict[str, object]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            "SELECT p.payload_bytes FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type=? ORDER BY e.ledger_seq",
            (event_type,),
        ).fetchall()
    payloads: list[dict[str, object]] = []
    for (raw,) in rows:
        payload = json.loads(bytes(raw))
        assert isinstance(payload, dict)
        payloads.append(payload)
    return payloads


def _digests(db: Path) -> list[tuple[int, str, str]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        return [
            (int(seq), str(event_type), str(digest))
            for seq, event_type, digest in conn.execute(
                "SELECT ledger_seq, event_type, payload_digest "
                "FROM ledger_events ORDER BY ledger_seq"
            )
        ]


def test_shared_ledger_path_stays_on_the_box_home() -> None:
    assert SHARED_HOME == Path("/home/box/newsroom")
    assert ledger_path(SHARED_HOME) == Path(
        "/home/box/newsroom/data/authority.sqlite3"
    )


def test_cover_set_excludes_uk10_and_hk01() -> None:
    assert EXTRACT_SOURCE_IDS == {
        "HK-04",
        "RAD-01",
        "RAD-02",
        "UK-01",
        "UK-05",
        "X-SEARCH-POSTS",
    }
    with pytest.raises(ExtractSignalError, match="UK-10 stays skip"):
        resolve_extract_source_id("UK-10")
    with pytest.raises(ExtractSignalError, match="do not remint seq 15"):
        resolve_extract_source_id("HK-01")


def test_live_producer_is_not_the_fixture_extractor() -> None:
    producer = LiveOfficialExtractor()
    assert type(producer) is not DeterministicFixtureExtractor
    assert producer.producer_kind == LIVE_OFFICIAL_PRODUCER_KIND
    assert producer.producer_kind != DeterministicFixtureExtractor.producer_kind
    text, digest, language, proposals = producer.produce_from_official_bytes(
        source_id="HK-04",
        title="家長電子通訊",
        body="教育局公布家長電子通訊最新一期。",
    )
    assert FIXTURE_EN_TEXT not in text
    assert FIXTURE_ZH_HK_TEXT not in text
    assert language == "zh-HK"
    assert digest.startswith("sha256:")
    assert len(proposals) == 1
    assert proposals[0].subject_placeholder == "家長電子通訊"
    assert proposals[0].rationale_codes == ("OFFICIAL_TITLE_SPAN",)
    span = text.encode("utf-8")[
        proposals[0].evidence[0].start_byte : proposals[0].evidence[0].end_byte
    ]
    assert span.decode("utf-8") == "家長電子通訊"


def test_live_producer_rejects_fixture_bytes() -> None:
    from newsroom.extraction.live_official import assert_not_fixture_text

    with pytest.raises(ExtractionContractError, match="fixture bytes"):
        assert_not_fixture_text(FIXTURE_EN_TEXT)
    with pytest.raises(ExtractionContractError, match="not authorised"):
        LiveOfficialExtractor().produce_from_official_bytes(
            source_id="UK-10",
            title="Met Office warning",
        )


def test_find_official_item_matches_atom_tag_and_bbc_fragment() -> None:
    title, body = find_official_feed_item(UK01_FEED.encode("utf-8"), UK01_ITEM)
    assert title == "Form BOTA guidance"
    assert "GOV.UK" in body
    title, body = find_official_feed_item(RAD02_FEED.encode("utf-8"), RAD02_ITEM)
    assert title == "UK official update"
    with pytest.raises(ExtractSignalError, match="missing from the official feed"):
        find_official_feed_item(EMPTY_FEED.encode("utf-8"), HK04_ITEM)


def test_payload_contract_rejects_fixture_producer_kind() -> None:
    extract_payload_contract()
    bad = json.loads(json.dumps(extract_payload_contract().golden_vectors[0].value))
    bad["producer_kind"] = "DETERMINISTIC_FIXTURE"
    with pytest.raises(ExtractSignalError, match="fixture producer"):
        canonicalize_extract_payload(bad)


def test_record_extract_writes_one_run_and_no_leads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = tmp_path / "authority.sqlite3"
    _seed_ledger(db)
    before = _digests(db)
    monkeypatch.setenv("NEWSROOM_INGEST_BODY_PATH", str(tmp_path / "official-rss.xml"))
    (tmp_path / "official-rss.xml").write_text(HK04_FEED, encoding="utf-8")

    payload = record_extraction_run(db, source_id="HK-04")
    assert payload["source_id"] == "HK-04"
    assert payload["item_id"] == HK04_ITEM
    assert payload["producer_kind"] == LIVE_OFFICIAL_PRODUCER_KIND
    assert payload["auto_publish"] is False
    assert payload["title"] == "家長電子通訊"

    runs = _events(db, EVENT_TYPE)
    assert len(runs) == 1
    assert runs[0]["item_id"] == HK04_ITEM
    assert runs[0]["producer_kind"] == LIVE_OFFICIAL_PRODUCER_KIND

    after = _digests(db)
    assert after[: len(before)] == before
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        assert conn.execute("SELECT COUNT(*) FROM news_leads").fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM editorial_relation_assertions"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM ledger_events WHERE event_type=?",
            (BUNDLE_EVENT,),
        ).fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM extraction_runs").fetchone()[0] == 0

    again = record_extraction_run(db, source_id="HK-04")
    assert again["item_id"] == HK04_ITEM
    assert len(_events(db, EVENT_TYPE)) == 1


def test_missing_item_and_uk10_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = tmp_path / "authority.sqlite3"
    record_envelope_grant(db)
    record_discovery_skip(
        db,
        source_id="UK-10",
        url=SOURCE_URLS["UK-10"],
        reason="RSS/Atom feed has no item",
    )
    with pytest.raises(ExtractSignalError, match="UK-10 stays skip"):
        record_extraction_run(db, source_id="UK-10")

    record_discovery_signal(
        db,
        source_id="HK-04",
        url=SOURCE_URLS["HK-04"],
        item_id=HK04_ITEM,
    )
    monkeypatch.setenv("NEWSROOM_INGEST_BODY_PATH", str(tmp_path / "empty.xml"))
    (tmp_path / "empty.xml").write_text(EMPTY_FEED, encoding="utf-8")
    with pytest.raises(ExtractSignalError, match="missing from the official feed"):
        record_extraction_run(db, source_id="HK-04")
    assert _events(db, EVENT_TYPE) == []


def test_x_search_uses_admitted_url_only(tmp_path: Path) -> None:
    db = tmp_path / "authority.sqlite3"
    record_envelope_grant(db)
    record_x_search_signal(
        db, source_id="X-SEARCH-POSTS", item_id=X_URL, url=X_URL
    )
    payload = load_official_extract(db, "X-SEARCH-POSTS")
    assert payload["item_id"] == X_URL
    assert payload["url"] == X_URL
    assert payload["title"] == X_URL
    assert payload["producer_kind"] == LIVE_OFFICIAL_PRODUCER_KIND
    expected = official_passage_text(title=X_URL)
    assert payload["text_digest"].startswith("sha256:")
    assert payload["proposals"][0]["subject_placeholder"] == X_URL
    assert expected == X_URL


def test_extract_signal_cli_records_hk04(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        granted = _cli("grant-envelope", home=home)
        assert granted.returncode == 0, granted.stderr
        stopped = _cli("stop", home=home)
        assert stopped.returncode == 0, stopped.stderr
        db = ledger_path(home)
        record_discovery_signal(
            db,
            source_id="HK-04",
            url=SOURCE_URLS["HK-04"],
            item_id=HK04_ITEM,
        )
        extracted = _cli(
            "extract-signal",
            "--source-id",
            "HK-04",
            home=home,
            env=_write_feed(tmp_path, HK04_FEED),
        )
        assert extracted.returncode == 0, extracted.stderr
        report = json.loads(extracted.stdout)
        assert report["ok"] is True
        assert report["event_type"] == EVENT_TYPE
        assert report["source_id"] == "HK-04"
        assert report["producer_kind"] == LIVE_OFFICIAL_PRODUCER_KIND
        assert report["item_id"] == HK04_ITEM
        assert len(_events(db, EVENT_TYPE)) == 1
        refused = _cli("extract-signal", "--source-id", "UK-10", home=home)
        assert refused.returncode == 1
        assert "UK-10" in refused.stderr or "admitted extract source" in refused.stderr
    finally:
        _cli("stop", home=home)
