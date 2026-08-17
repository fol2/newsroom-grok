"""HK-01 extract -> written story -> Publication Bundle with body seam."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from newsroom.first_boot import SHARED_HOME, ledger_path
from newsroom.publication_bundle import (
    BUNDLE_DIGEST,
    EVENT_TYPE,
    ITEM_URL,
    SAMPLE_PAYLOAD,
    canonicalize_bundle_payload,
    extract_hk01_story,
)


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "newsroom_first_boot.py"
SIGNAL_EVENT = "discovery.signal.admitted"
DECISION_EVENT = "publication.decision.authorised"
TARGET_EVENT = "target.operation.dispatched"
PAUSE_RESTORE_NAME = "restore.paused"
TITLE = "政府公布新一輪支援措施詳情"
BODY = (
    "政府今日公布新一輪支援措施詳情，並交代申請安排、合資格人士及推行時間。"
    "當局表示會密切監察執行情況，適時公布進一步資料。"
)
OTHER_RSS = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<rss version="2.0"><channel>'
    "<item><guid>hk-01-first-item</guid>"
    "<title>Official RSS item</title>"
    "<link>https://www.news.gov.hk/item-1</link>"
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


def _stop(home: Path, *, pause_restore: bool = False) -> None:
    args = ("stop", "--pause-restore") if pause_restore else ("stop",)
    _cli(*args, home=home)


def _fixture(
    path: Path,
    *,
    summary: str = BODY,
    item_url: str = ITEM_URL,
) -> Path:
    path.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel><item>'
        f"<guid>{item_url}</guid><link>{item_url}</link>"
        f"<title>{TITLE}</title>"
        f"<description><![CDATA[<p>{summary}</p>]]></description>"
        "</item></channel></rss>",
        encoding="utf-8",
    )
    return path


def _events(db: Path) -> list[tuple[int, str, bytes]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        return [
            (int(seq), str(kind), bytes(payload))
            for seq, kind, payload in conn.execute(
                "SELECT e.ledger_seq, e.event_type, p.payload_bytes "
                "FROM ledger_events e "
                "JOIN authority_payloads p ON p.payload_id=e.payload_id "
                "ORDER BY e.ledger_seq"
            )
        ]


def _prepare(home: Path, tmp_path: Path, *, rss: Path | None = None) -> Path:
    _install_uv_stub(tmp_path)
    feed = rss or _fixture(tmp_path / "admitted.xml")
    start = _cli("start", home=home)
    assert start.returncode == 0, start.stderr
    granted = _cli("grant-envelope", home=home)
    assert granted.returncode == 0, granted.stderr
    ingested = _cli(
        "ingest-signal",
        home=home,
        env={"NEWSROOM_INGEST_BODY_PATH": str(feed)},
    )
    assert ingested.returncode == 0, ingested.stderr
    minted = _cli("mint-decision", home=home)
    assert minted.returncode == 0, minted.stderr
    return feed


def test_shared_ledger_path_stays_on_the_box_home() -> None:
    assert SHARED_HOME == Path("/home/box/newsroom")
    assert ledger_path(SHARED_HOME) == Path("/home/box/newsroom/data/authority.sqlite3")


def test_canonicalize_fails_closed_on_wrong_digest_source_or_flags() -> None:
    canonicalize_bundle_payload(SAMPLE_PAYLOAD)
    wrong_digest = dict(SAMPLE_PAYLOAD)
    wrong_digest["bundle_digest"] = (
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )
    with pytest.raises(ValueError, match="authorised HK-01 digest"):
        canonicalize_bundle_payload(wrong_digest)
    wrong_source = dict(SAMPLE_PAYLOAD)
    wrong_source["source_id"] = "HK-04"
    with pytest.raises(ValueError, match="HK-01"):
        canonicalize_bundle_payload(wrong_source)
    for flag in ("auto_publish", "discord", "public_adapter", "x_as_publisher"):
        flipped = dict(SAMPLE_PAYLOAD)
        flipped[flag] = True
        with pytest.raises(ValueError):
            canonicalize_bundle_payload(flipped)


def test_extract_uses_official_page_when_matching_rss_summary_is_thin(
    tmp_path: Path,
) -> None:
    feed = _fixture(tmp_path / "feed.xml", summary="簡訊。")
    page = tmp_path / "page.html"
    page.write_text(
        f"<html><head><title>{TITLE}</title></head><body><article>"
        f"<h1>{TITLE}</h1><p>{BODY}</p>"
        "<p>市民可按官方公布的安排提交申請，詳情載於政府網站。</p>"
        "</article></body></html>",
        encoding="utf-8",
    )
    story = extract_hk01_story(rss_body=feed.read_bytes(), page_body=page.read_bytes())
    assert story["title"] == TITLE
    assert BODY in story["body"]
    assert "sha256:" not in story["body"]


def test_extract_fails_closed_when_item_is_missing_or_wrong() -> None:
    wrong = (
        b'<rss version="2.0"><channel><item>'
        b"<guid>https://www.news.gov.hk/wrong</guid>"
        b"<title>Wrong item</title>"
        b"<description>Wrong body that is long enough but untrusted.</description>"
        b"</item></channel></rss>"
    )
    with pytest.raises(ValueError, match="admitted HK-01 item is missing"):
        extract_hk01_story(rss_body=wrong)


def test_mint_bundle_body_appends_once_without_rewriting_live_rows(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    try:
        feed = _prepare(home, tmp_path)
        before = _events(ledger_path(home))
        decision_before = [row for row in before if row[1] == DECISION_EVENT]
        assert len(decision_before) == 1
        assert json.loads(_cli("mint-decision", home=home).stdout)["bundle_digest"] == (
            BUNDLE_DIGEST
        )
        assert set(json.loads(decision_before[0][2])) == {
            "authorising",
            "auto_publish",
            "bundle_digest",
            "controller",
            "discord",
            "hermes_publication_admission",
            "public_adapter",
        }

        env = {"NEWSROOM_BUNDLE_RSS_BODY_PATH": str(feed)}
        minted = _cli("mint-bundle-body", home=home, env=env)
        assert minted.returncode == 0, minted.stderr
        report = json.loads(minted.stdout)
        assert report["ok"] is True
        assert report["event_type"] == EVENT_TYPE
        assert report["bundle_digest"] == BUNDLE_DIGEST
        assert report["auto_publish"] is False
        assert report["discord"] is False
        assert report["public_adapter"] is False
        assert report["x_as_publisher"] is False

        after = _events(ledger_path(home))
        assert after[: len(before)] == before
        assert len(after) == len(before) + 1
        assert len([row for row in after if row[1] == SIGNAL_EVENT]) == 1
        assert [row for row in after if row[1] == DECISION_EVENT] == decision_before
        assert not [row for row in after if row[1] == TARGET_EVENT]

        bundles = [json.loads(row[2]) for row in after if row[1] == EVENT_TYPE]
        assert len(bundles) == 1
        assert bundles[0] == {
            "auto_publish": False,
            "bundle_digest": BUNDLE_DIGEST,
            "discord": False,
            "item_url": ITEM_URL,
            "public_adapter": False,
            "source_id": "HK-01",
            "story_body": BODY,
            "story_title": TITLE,
            "x_as_publisher": False,
        }
        assert len(bundles[0]["story_title"]) > 4
        assert len(bundles[0]["story_body"]) > 40
        assert "sha256:" not in bundles[0]["story_body"]

        again = _cli("mint-bundle-body", home=home, env=env)
        assert again.returncode == 0, again.stderr
        assert len([row for row in _events(ledger_path(home)) if row[1] == EVENT_TYPE]) == 1
    finally:
        _stop(home)


def test_mint_bundle_body_fails_closed_on_wrong_digest_or_missing_item(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    other = tmp_path / "other.xml"
    other.write_text(OTHER_RSS, encoding="utf-8")
    try:
        _prepare(home, tmp_path, rss=other)
        digest = json.loads(_cli("mint-decision", home=home).stdout)["bundle_digest"]
        assert digest != BUNDLE_DIGEST
        refused = _cli(
            "mint-bundle-body",
            home=home,
            env={"NEWSROOM_BUNDLE_RSS_BODY_PATH": str(_fixture(tmp_path / "hk01.xml"))},
        )
        assert refused.returncode != 0
        assert any(token in refused.stderr.lower() for token in ("decision", "digest", "item", "missing"))
        assert not [row for row in _events(ledger_path(home)) if row[1] == EVENT_TYPE]
    finally:
        _stop(home)


def test_mint_bundle_body_fails_closed_without_decision_and_when_restore_paused(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    feed = _fixture(tmp_path / "feed.xml")
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        absent = _cli(
            "mint-bundle-body",
            home=home,
            env={"NEWSROOM_BUNDLE_RSS_BODY_PATH": str(feed)},
        )
        assert absent.returncode != 0
        assert any(token in absent.stderr.lower() for token in ("decision", "signal", "item", "missing"))
        assert not [row for row in _events(ledger_path(home)) if row[1] == EVENT_TYPE]

        stopped = _cli("stop", "--pause-restore", home=home)
        assert stopped.returncode == 0, stopped.stderr
        assert (home / "logs" / PAUSE_RESTORE_NAME).is_file()
        paused = _cli(
            "mint-bundle-body",
            home=home,
            env={"NEWSROOM_BUNDLE_RSS_BODY_PATH": str(feed)},
        )
        assert paused.returncode != 0
        assert "paused" in paused.stderr.lower()
        assert not [row for row in _events(ledger_path(home)) if row[1] == EVENT_TYPE]
    finally:
        _stop(home, pause_restore=True)
