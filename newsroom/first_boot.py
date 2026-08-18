"""Grok Bot first-boot bring-up, grant, ingest, X-search ingest, mint, dispatch, internal-beta publish, AUTO-010 commit, Neo4j project, News Lead admission, live-official extract, accepted-material entity resolution, editorial relation ACCEPT, Story Candidate triage, Evidence Package, and emergency stop."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

SHARED_HOME = Path("/home/box/newsroom")
_HOLD_WAIT_SECONDS = 60.0
_HOLD_POLL_SECONDS = 0.2


class FirstBootError(RuntimeError):
    """Bring-up or health assessment failed."""


def ledger_path(home: Path) -> Path:
    return Path(home) / "data" / "authority.sqlite3"


def venv_path(home: Path) -> Path:
    return Path(home) / ".venv"


def pid_path(home: Path) -> Path:
    return Path(home) / "logs" / "store.pid"


def restore_pause_path(home: Path) -> Path:
    return Path(home) / "logs" / "restore.paused"


def restore_paused(home: Path) -> bool:
    path = restore_pause_path(home)
    return path.is_file() and not path.is_symlink()


def resolve_project_root(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return Path(explicit).expanduser().resolve()
    for candidate in (Path(__file__).resolve().parents[1], Path.cwd()):
        if (candidate / "pyproject.toml").is_file() and (candidate / "uv.lock").is_file():
            return candidate
    raise FirstBootError("cannot find newsroom checkout (pyproject.toml + uv.lock)")


def process_is_up(home: Path) -> bool:
    pid = _read_pid(home)
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def assess_health(home: Path) -> dict[str, Any]:
    db = ledger_path(home)
    exists = db.is_file() and not db.is_symlink()
    up = process_is_up(home)
    empty = False
    signal_count: int | None = None
    event_count: int | None = None
    if exists:
        try:
            empty, signal_count, event_count = _read_ledger_emptiness(db)
        except sqlite3.Error:
            empty = False
    return {
        "ok": bool(up and exists and empty),
        "process_up": up,
        "ledger_exists": exists,
        "ledger_empty": empty,
        "ledger_path": str(db),
        "home": str(home),
        "pid": _read_pid(home) if up else None,
        "discovery_signal_count": signal_count,
        "ledger_event_count": event_count,
    }


def start(
    home: Path,
    *,
    project_root: Path | None = None,
    uv_bin: str = "uv",
    resume: bool = False,
) -> dict[str, Any]:
    if restore_paused(home) and not resume:
        raise FirstBootError(
            "Newsroom first-boot restore is paused; pass --resume to return"
        )
    if resume:
        _clear_restore_pause(home)
    root = resolve_project_root(project_root)
    _prepare_home(home)
    _sync_venv(home, root, uv_bin)
    if process_is_up(home) and _ledger_present(home):
        return _start_report(home, pid=_read_pid(home))
    if process_is_up(home):
        stop(home)
    _spawn_hold(home, root)
    report = _wait_ready(home)
    if not (report["process_up"] and report["ledger_exists"]):
        stop(home)
        raise FirstBootError(
            "host process did not come up with a ledger after bring-up: "
            + json.dumps(report, sort_keys=True)
        )
    return _start_report(home, pid=report.get("pid"))


def stop(home: Path, *, pause_restore: bool = False) -> dict[str, Any]:
    pid = _read_pid(home)
    if pid is not None and process_is_up(home):
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and process_is_up(home):
            time.sleep(0.05)
        if process_is_up(home):
            os.kill(pid, signal.SIGKILL)
    _clear_pid(home)
    if pause_restore:
        _write_restore_pause(home)
    return {
        "ok": True,
        "home": str(home),
        "restore_paused": restore_paused(home),
    }


def hold(home: Path) -> int:
    _prepare_home(home)
    stopped = threading.Event()

    def _request_stop(_signum: int, _frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    system = _open_host_store(ledger_path(home))
    try:
        _write_pid(home, os.getpid())
        while not stopped.is_set():
            stopped.wait(timeout=3600)
    finally:
        system.close()
        _clear_pid(home)
    return 0


def grant_envelope(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.envelope_grant import (
        CONTROLLER_ID,
        EVENT_TYPE,
        record_envelope_grant,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        record_envelope_grant(db)
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"envelope grant failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    return {
        "ok": True,
        "controller": CONTROLLER_ID,
        "event_type": EVENT_TYPE,
        "home": str(home),
        "ledger_path": str(db),
    }



def grant_auto_publish(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.auto_publish_grant import (
        EVENT_TYPE,
        FIRST_TARGET,
        SEMANTIC,
        record_auto_publish_grant,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        record_auto_publish_grant(db)
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"AUTO_PUBLISH grant failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    return {
        "ok": True,
        "event_type": EVENT_TYPE,
        "first_target": FIRST_TARGET,
        "home": str(home),
        "ledger_path": str(db),
        "semantic": SEMANTIC,
    }


def grant_internal_beta(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.internal_beta_grant import (
        BUNDLE_DIGEST,
        EVENT_TYPE,
        OPERATION,
        SOURCE_ID,
        TARGET,
        record_internal_beta_grant,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before internal beta grant"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        record_internal_beta_grant(db)
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"internal beta grant failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    return {
        "ok": True,
        "auto_publish": False,
        "bundle_digest": BUNDLE_DIGEST,
        "discord": False,
        "event_type": EVENT_TYPE,
        "home": str(home),
        "ledger_path": str(db),
        "operation": OPERATION,
        "public_adapter": False,
        "source_id": SOURCE_ID,
        "target": TARGET,
        "x_as_publisher": False,
    }


def ingest_signal(
    home: Path,
    *,
    project_root: Path | None = None,
    source_id: str | None = None,
) -> dict[str, Any]:
    from newsroom.discovery_ingest import (
        EVENT_TYPE,
        OFFICIAL_RSS_ADAPTER,
        SKIP_EVENT_TYPE,
        SOURCE_URLS,
        FeedFetchError,
        first_feed_item_id,
        load_official_rss_body,
        record_discovery_signal,
        record_discovery_skip,
        resolve_rss_source_id,
    )
    from newsroom.envelope_grant import EVENT_TYPE as GRANT_EVENT_TYPE

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before discovery ingest"
        )
    try:
        selected = resolve_rss_source_id(source_id)
    except ValueError as exc:
        raise FirstBootError(str(exc)) from exc
    url = SOURCE_URLS[selected]
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        _require_envelope_grant(db, GRANT_EVENT_TYPE)
        try:
            loaded_id, url, body = load_official_rss_body(selected)
            item_id = first_feed_item_id(body)
            record_discovery_signal(
                db, source_id=loaded_id, url=url, item_id=item_id
            )
        except FeedFetchError as exc:
            reason = str(exc)
            record_discovery_skip(
                db, source_id=selected, url=url, reason=reason
            )
            if was_up and not paused:
                start(home, project_root=project_root)
            return {
                "ok": True,
                "adapter": OFFICIAL_RSS_ADAPTER,
                "auto_publish": False,
                "discord": False,
                "event_type": SKIP_EVENT_TYPE,
                "home": str(home),
                "ledger_path": str(db),
                "public_adapter": False,
                "reason": reason,
                "skipped": True,
                "source_id": selected,
                "url": url,
            }
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"discovery ingest failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    return {
        "ok": True,
        "adapter": OFFICIAL_RSS_ADAPTER,
        "auto_publish": False,
        "discord": False,
        "event_type": EVENT_TYPE,
        "home": str(home),
        "item_id": item_id,
        "ledger_path": str(db),
        "public_adapter": False,
        "source_id": loaded_id,
        "url": url,
    }


def ingest_x_search(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.envelope_grant import EVENT_TYPE as GRANT_EVENT_TYPE
    from newsroom.x_search_ingest import (
        EVENT_TYPE,
        GATED_X_SEARCH_ADAPTER,
        load_x_search_hit,
        record_discovery_signal,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before X search ingest"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        _require_envelope_grant(db, GRANT_EVENT_TYPE)
        source_id, url, item_id = load_x_search_hit()
        record_discovery_signal(
            db, source_id=source_id, url=url, item_id=item_id
        )
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"X search ingest failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    return {
        "ok": True,
        "adapter": GATED_X_SEARCH_ADAPTER,
        "auto_publish": False,
        "discord": False,
        "event_type": EVENT_TYPE,
        "home": str(home),
        "item_id": item_id,
        "ledger_path": str(db),
        "public_adapter": False,
        "source_id": source_id,
        "url": url,
        "x_as_publisher": False,
    }


def mint_decision(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.envelope_grant import CONTROLLER_ID, EVENT_TYPE as GRANT_EVENT_TYPE
    from newsroom.publication_decision import (
        EVENT_TYPE,
        load_first_discovery_signal,
        record_publication_decision,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        _require_envelope_grant(db, GRANT_EVENT_TYPE)
        signal = load_first_discovery_signal(db)
        recorded = record_publication_decision(db, signal_payload=signal)
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"publication decision failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    return {
        "ok": True,
        "bundle_digest": recorded["bundle_digest"],
        "controller": CONTROLLER_ID,
        "event_type": EVENT_TYPE,
        "home": str(home),
        "ledger_path": str(db),
    }


def admit_leads(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.discovery_lead_admission import (
        LeadAdmissionError,
        record_first_boot_leads,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before lead admission"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        recorded = record_first_boot_leads(db)
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except LeadAdmissionError as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(str(exc)) from exc
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"news lead admission failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    recorded["home"] = str(home)
    recorded["ledger_path"] = str(db)
    return recorded


def extract_leads(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.discovery_lead_extraction import (
        LeadExtractionError,
        record_first_boot_extraction,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before live-official extract"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        recorded = record_first_boot_extraction(db)
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except LeadExtractionError as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(str(exc)) from exc
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"live-official extract failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    recorded["home"] = str(home)
    recorded["ledger_path"] = str(db)
    return recorded


def resolve_leads(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.discovery_lead_entity_resolution import (
        LeadEntityResolutionError,
        record_first_boot_entity_resolution,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before live-official entity resolution"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        recorded = record_first_boot_entity_resolution(db)
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except LeadEntityResolutionError as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(str(exc)) from exc
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(
            f"live-official entity resolution failed: {exc}"
        ) from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    recorded["home"] = str(home)
    recorded["ledger_path"] = str(db)
    return recorded


def relate_leads(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.discovery_lead_editorial_relations import (
        LeadEditorialRelationError,
        record_first_boot_editorial_relations,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before live-official editorial relations"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        recorded = record_first_boot_editorial_relations(db)
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except LeadEditorialRelationError as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(str(exc)) from exc
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(
            f"live-official editorial relation failed: {exc}"
        ) from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    recorded["home"] = str(home)
    recorded["ledger_path"] = str(db)
    return recorded


def triage_leads(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.discovery_lead_story_candidates import (
        LeadStoryCandidateError,
        record_first_boot_story_candidates,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before live-official triage"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        recorded = record_first_boot_story_candidates(db)
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except LeadStoryCandidateError as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(str(exc)) from exc
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(
            f"live-official Story Candidate triage failed: {exc}"
        ) from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    recorded["home"] = str(home)
    recorded["ledger_path"] = str(db)
    return recorded


def mint_bundle_body(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.publication_bundle import (
        BUNDLE_DIGEST,
        EVENT_TYPE,
        ITEM_URL,
        SOURCE_ID,
        load_authorised_hk01_binding,
        load_official_story,
        record_publication_bundle,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before bundle mint"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        load_authorised_hk01_binding(db)
        story = load_official_story()
        recorded = record_publication_bundle(db, story=story)
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"publication bundle mint failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    return {
        "ok": True,
        "auto_publish": False,
        "bundle_digest": BUNDLE_DIGEST,
        "discord": False,
        "event_type": EVENT_TYPE,
        "home": str(home),
        "item_url": ITEM_URL,
        "ledger_path": str(db),
        "public_adapter": False,
        "source_id": SOURCE_ID,
        "story_body": recorded["story_body"],
        "story_title": recorded["story_title"],
        "x_as_publisher": False,
    }



def evidence_leads(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.discovery_lead_evidence_packages import (
        LeadEvidencePackageError,
        record_first_boot_evidence_packages,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before live-official evidence"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        recorded = record_first_boot_evidence_packages(db)
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except LeadEvidencePackageError as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(str(exc)) from exc
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(
            f"live-official Evidence Package failed: {exc}"
        ) from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    recorded["home"] = str(home)
    recorded["ledger_path"] = str(db)
    return recorded


def dispatch_target(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.envelope_grant import EVENT_TYPE as GRANT_EVENT_TYPE
    from newsroom.publication_decision import (
        bundle_digest_for_signal,
        load_first_discovery_signal,
    )
    from newsroom.target_operation import (
        DISPATCHER_ID,
        EVENT_TYPE,
        LEDGER_TARGET,
        load_first_authorising_decision,
        record_target_operation,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        _require_envelope_grant(db, GRANT_EVENT_TYPE)
        signal = load_first_discovery_signal(db)
        decision = load_first_authorising_decision(db)
        expected = bundle_digest_for_signal(signal)
        if decision["bundle_digest"] != expected:
            raise ValueError(
                "publication decision digest does not match first Discovery Signal"
            )
        recorded = record_target_operation(
            db, bundle_digest=decision["bundle_digest"]
        )
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"target operation dispatch failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    return {
        "ok": True,
        "auto_publish": False,
        "bundle_digest": recorded["bundle_digest"],
        "discord": False,
        "dispatcher": DISPATCHER_ID,
        "event_type": EVENT_TYPE,
        "home": str(home),
        "ledger_path": str(db),
        "operation": recorded["operation"],
        "public_adapter": False,
        "target": LEDGER_TARGET,
    }



def dispatch_internal_beta(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.internal_beta_grant import EVENT_TYPE as GRANT_EVENT_TYPE
    from newsroom.internal_beta_publish import (
        DISPATCHER_ID,
        EVENT_TYPE,
        TARGET,
        record_internal_beta_publish,
    )

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before internal beta publish"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        _require_internal_beta_grant(db, GRANT_EVENT_TYPE)
        recorded = record_internal_beta_publish(db)
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"internal beta publish dispatch failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    return {
        "ok": True,
        "auto_publish": False,
        "bundle_digest": recorded["bundle_digest"],
        "discord": False,
        "dispatcher": DISPATCHER_ID,
        "event_type": EVENT_TYPE,
        "home": str(home),
        "ledger_path": str(db),
        "operation": recorded["operation"],
        "public_adapter": False,
        "target": TARGET,
        "x_as_publisher": False,
    }


def auto_010_commit(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.auto_010_commit import hold_applies, record_auto_010_commit
    from newsroom.auto_publish_grant import EVENT_TYPE as AUTO_PUBLISH_EVENT_TYPE
    from newsroom.envelope_grant import EVENT_TYPE as GRANT_EVENT_TYPE
    from newsroom.publication_decision import EVENT_TYPE as DECISION_EVENT_TYPE
    from newsroom.target_operation import EVENT_TYPE as OPERATION_EVENT_TYPE

    db = ledger_path(home)
    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if hold_applies(home):
        raise FirstBootError(
            "hold applies; AUTO-010 does not age into publish (AUTO-054)"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before AUTO-010 commit"
        )
    paused = restore_paused(home)
    was_up = process_is_up(home)
    if was_up:
        stop(home)
    try:
        _require_envelope_grant(db, GRANT_EVENT_TYPE)
        _require_auto_publish_grant(db, AUTO_PUBLISH_EVENT_TYPE)
        recorded = record_auto_010_commit(db)
    except FirstBootError:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise
    except Exception as exc:
        if was_up and not paused:
            start(home, project_root=project_root)
        raise FirstBootError(f"AUTO-010 commit failed: {exc}") from exc
    if was_up and not paused:
        start(home, project_root=project_root)
    return {
        "ok": True,
        "auto_publish": False,
        "bundle_digest": recorded["bundle_digest"],
        "discord": False,
        "every_applicable_gate_passed": recorded["every_applicable_gate_passed"],
        "first_target": recorded["first_target"],
        "hold": False,
        "home": str(home),
        "ledger_path": str(db),
        "operation": recorded["operation"],
        "public_adapter": False,
        "publication_decision": DECISION_EVENT_TYPE,
        "semantic": recorded["semantic"],
        "target": recorded["target"],
        "target_operation": OPERATION_EVENT_TYPE,
    }


def project_neo4j(
    home: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    from newsroom.neo4j_host_projection import project_host_neo4j

    if not _ledger_present(home):
        raise FirstBootError(
            "ledger missing; run newsroom-first-boot start first"
        )
    if restore_paused(home):
        raise FirstBootError(
            "emergency stop is paused; resume is required before Neo4j projection"
        )
    try:
        return project_host_neo4j(home)
    except FirstBootError:
        raise
    except Exception as exc:
        raise FirstBootError(f"Neo4j host projection failed: {exc}") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="First-boot bring-up, grant, ingest, X-search ingest, mint, dispatch, internal-beta publish, AUTO-010 commit, Neo4j project, News Lead admission, live-official extract, accepted-material entity resolution, editorial relation ACCEPT, Story Candidate triage, Evidence Package, and emergency stop."
    )
    parser.add_argument(
        "command",
        choices=(
            "start",
            "health",
            "stop",
            "hold",
            "grant-envelope",
            "grant-auto-publish",
            "grant-internal-beta",
            "ingest-signal",
            "ingest-x-search",
            "mint-decision",
            "mint-bundle-body",
            "admit-leads",
            "extract-leads",
            "resolve-leads",
            "relate-leads",
            "triage-leads",
            "evidence-leads",
            "dispatch-target",
            "dispatch-internal-beta",
            "auto-010-commit",
            "project-neo4j",
        ),
    )
    parser.add_argument("--home", default=str(SHARED_HOME))
    parser.add_argument("--project-root")
    parser.add_argument(
        "--pause-restore",
        action="store_true",
        help="Pause Newsroom first-boot restore so the process does not auto-return.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Clear a restore pause and bring the host process up.",
    )
    parser.add_argument(
        "--source-id",
        default=None,
        help="Official Source Definition RSS/Atom id (default HK-01).",
    )
    args = parser.parse_args(argv)
    home = Path(args.home).expanduser().resolve()
    project_root = Path(args.project_root).expanduser() if args.project_root else None
    try:
        if args.command == "health":
            report = assess_health(home)
            _print_json(report)
            return 0 if report["ok"] else 1
        if args.command == "stop":
            _print_json(stop(home, pause_restore=args.pause_restore))
            return 0
        if args.command == "hold":
            return hold(home)
        if args.command == "grant-envelope":
            _print_json(grant_envelope(home, project_root=project_root))
            return 0
        if args.command == "grant-auto-publish":
            _print_json(grant_auto_publish(home, project_root=project_root))
            return 0
        if args.command == "grant-internal-beta":
            _print_json(grant_internal_beta(home, project_root=project_root))
            return 0
        if args.command == "ingest-signal":
            _print_json(
                ingest_signal(
                    home,
                    project_root=project_root,
                    source_id=args.source_id,
                )
            )
            return 0
        if args.command == "ingest-x-search":
            _print_json(ingest_x_search(home, project_root=project_root))
            return 0
        if args.command == "mint-decision":
            _print_json(mint_decision(home, project_root=project_root))
            return 0
        if args.command == "mint-bundle-body":
            _print_json(mint_bundle_body(home, project_root=project_root))
            return 0
        if args.command == "admit-leads":
            _print_json(admit_leads(home, project_root=project_root))
            return 0
        if args.command == "extract-leads":
            _print_json(extract_leads(home, project_root=project_root))
            return 0
        if args.command == "resolve-leads":
            _print_json(resolve_leads(home, project_root=project_root))
            return 0
        if args.command == "relate-leads":
            _print_json(relate_leads(home, project_root=project_root))
            return 0
        if args.command == "triage-leads":
            _print_json(triage_leads(home, project_root=project_root))
            return 0
        if args.command == "evidence-leads":
            _print_json(evidence_leads(home, project_root=project_root))
            return 0
        if args.command == "dispatch-target":
            _print_json(dispatch_target(home, project_root=project_root))
            return 0
        if args.command == "dispatch-internal-beta":
            _print_json(dispatch_internal_beta(home, project_root=project_root))
            return 0
        if args.command == "auto-010-commit":
            _print_json(auto_010_commit(home, project_root=project_root))
            return 0
        if args.command == "project-neo4j":
            _print_json(project_neo4j(home, project_root=project_root))
            return 0
        _print_json(start(home, project_root=project_root, resume=args.resume))
        return 0
    except FirstBootError as exc:
        print(str(exc), file=sys.stderr)
        _print_json({"ok": False, "error": str(exc), "home": str(home)})
        return 1


def _start_report(home: Path, *, pid: int | None) -> dict[str, Any]:
    return {
        "ok": True,
        "uv_sync": True,
        "home": str(home),
        "venv": str(venv_path(home)),
        "ledger_path": str(ledger_path(home)),
        "pid": pid,
    }


def _prepare_home(home: Path) -> None:
    for relative in (".", "data", "logs"):
        path = home if relative == "." else home / relative
        path.mkdir(parents=True, exist_ok=True)
        os.chmod(path, 0o700)


def _sync_venv(home: Path, project_root: Path, uv_bin: str) -> None:
    env = os.environ.copy()
    env["UV_PROJECT_ENVIRONMENT"] = str(venv_path(home))
    completed = subprocess.run(
        [uv_bin, "sync", "--locked"],
        cwd=str(project_root),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "uv sync failed").strip()
        raise FirstBootError(f"uv sync into {venv_path(home)} failed: {detail}")


def _spawn_hold(home: Path, project_root: Path) -> None:
    python = venv_path(home) / "bin" / "python"
    if not python.is_file():
        raise FirstBootError(f"shared-home venv python missing: {python}")
    logs = home / "logs"
    with (logs / "store.out.log").open("a", encoding="utf-8") as out_f, (
        logs / "store.err.log"
    ).open("a", encoding="utf-8") as err_f:
        subprocess.Popen(
            [str(python), "-m", "newsroom.first_boot", "hold", "--home", str(home)],
            cwd=str(project_root),
            stdout=out_f,
            stderr=err_f,
            start_new_session=True,
        )


def _wait_ready(home: Path) -> dict[str, Any]:
    deadline = time.monotonic() + _HOLD_WAIT_SECONDS
    report = assess_health(home)
    while time.monotonic() < deadline:
        report = assess_health(home)
        if report["process_up"] and report["ledger_exists"]:
            return report
        time.sleep(_HOLD_POLL_SECONDS)
    return report


def _read_pid(home: Path) -> int | None:
    path = pid_path(home)
    if not path.is_file() or path.is_symlink():
        return None
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except ValueError:
        return None


def _write_pid(home: Path, pid: int) -> None:
    path = pid_path(home)
    path.write_text(f"{pid}\n", encoding="utf-8")
    os.chmod(path, 0o600)


def _clear_pid(home: Path) -> None:
    path = pid_path(home)
    if path.is_file() and not path.is_symlink():
        path.unlink()


def _ledger_present(home: Path) -> bool:
    db = ledger_path(home)
    return db.is_file() and not db.is_symlink()


def _require_envelope_grant(path: Path, event_type: str) -> None:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        count = int(
            conn.execute(
                "SELECT COUNT(*) FROM ledger_events WHERE event_type=?",
                (event_type,),
            ).fetchone()[0]
        )
    finally:
        conn.close()
    if count < 1:
        raise FirstBootError(
            "envelope grant missing; run newsroom-first-boot grant-envelope first"
        )



def _require_internal_beta_grant(path: Path, event_type: str) -> None:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        count = int(
            conn.execute(
                "SELECT COUNT(*) FROM ledger_events WHERE event_type=?",
                (event_type,),
            ).fetchone()[0]
        )
    finally:
        conn.close()
    if count < 1:
        raise FirstBootError(
            "internal_beta grant missing; run newsroom-first-boot grant-internal-beta first"
        )


def _require_auto_publish_grant(path: Path, event_type: str) -> None:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        count = int(
            conn.execute(
                "SELECT COUNT(*) FROM ledger_events WHERE event_type=?",
                (event_type,),
            ).fetchone()[0]
        )
    finally:
        conn.close()
    if count < 1:
        raise FirstBootError(
            "AUTO_PUBLISH grant missing; run newsroom-first-boot grant-auto-publish first"
        )


def _write_restore_pause(home: Path) -> None:
    logs = Path(home) / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    os.chmod(logs, 0o700)
    path = restore_pause_path(home)
    path.write_text("paused\n", encoding="utf-8")
    os.chmod(path, 0o600)


def _clear_restore_pause(home: Path) -> None:
    path = restore_pause_path(home)
    if path.is_file() and not path.is_symlink():
        path.unlink()


def _read_ledger_emptiness(path: Path) -> tuple[bool, int, int]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "discovery_signals" not in tables or "ledger_events" not in tables:
            return False, 0, 0
        signals = int(
            conn.execute("SELECT COUNT(*) FROM discovery_signals").fetchone()[0]
        )
        events = int(conn.execute("SELECT COUNT(*) FROM ledger_events").fetchone()[0])
        return signals == 0 and events == 0, signals, events
    finally:
        conn.close()


def _open_host_store(path: Path) -> Any:
    from newsroom.host_store import open_host_store

    return open_host_store(path)


def _print_json(value: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(value, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
