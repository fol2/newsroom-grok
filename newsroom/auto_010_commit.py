"""AUTO-010 commit: PD + TO to host.authority.ledger when gates pass and no hold."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from newsroom.auto_publish_grant import (
    EVENT_TYPE as GRANT_EVENT_TYPE,
    FIRST_TARGET,
    OPERATION,
    SEMANTIC,
)
from newsroom.publication_decision import (
    load_first_discovery_signal,
    record_publication_decision,
)
from newsroom.target_operation import record_target_operation


HOLD_NAME = "publication.hold"
HOLD_ENV = "NEWSROOM_AUTO_010_HOLD"


def publication_hold_path(home: Path) -> Path:
    return Path(home) / "logs" / HOLD_NAME


def hold_applies(home: Path) -> bool:
    """Durable publication hold. Time passing does not convert it to AUTO_PUBLISH."""
    marker = os.environ.get(HOLD_ENV, "").strip().lower()
    if marker in {"1", "true", "yes"}:
        return True
    path = publication_hold_path(home)
    return path.is_file() and not path.is_symlink()


def load_auto_publish_grant(path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        row = conn.execute(
            "SELECT p.payload_bytes FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id=e.payload_id "
            "WHERE e.event_type=? "
            "ORDER BY e.ledger_seq ASC LIMIT 1",
            (GRANT_EVENT_TYPE,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise ValueError(
            "AUTO_PUBLISH grant missing; run newsroom-first-boot grant-auto-publish first"
        )
    payload = json.loads(bytes(row[0]))
    if not isinstance(payload, dict):
        raise ValueError("AUTO_PUBLISH grant payload must be an object")
    if payload.get("semantic") != SEMANTIC:
        raise ValueError("AUTO_PUBLISH grant must be AUTO-010")
    if payload.get("every_applicable_gate_passed") is not True:
        raise ValueError("AUTO-010 requires every applicable gate passed")
    if payload.get("hold") is not False:
        raise ValueError("hold applies; AUTO-010 does not age into publish (AUTO-054)")
    if payload.get("first_target") != FIRST_TARGET:
        raise ValueError("first target stays host.authority.ledger")
    if payload.get("operation") != OPERATION:
        raise ValueError("operation stays record")
    if payload.get("auto_publish") is not False:
        raise ValueError("public AUTO_PUBLISH stays off")
    if payload.get("public_adapter") is not False:
        raise ValueError("public adapters stay off")
    if payload.get("discord") is not False:
        raise ValueError("Discord stays off")
    if payload.get("controller_may_arm") is not False:
        raise ValueError("agent-turn controller may not arm")
    return payload


def record_auto_010_commit(path: Path) -> dict[str, Any]:
    grant = load_auto_publish_grant(path)
    signal = load_first_discovery_signal(path)
    decision = record_publication_decision(path, signal_payload=signal)
    operation = record_target_operation(
        path, bundle_digest=str(decision["bundle_digest"])
    )
    return {
        "auto_publish": False,
        "bundle_digest": decision["bundle_digest"],
        "discord": False,
        "every_applicable_gate_passed": True,
        "first_target": FIRST_TARGET,
        "hold": False,
        "operation": operation["operation"],
        "public_adapter": False,
        "semantic": grant["semantic"],
        "target": operation["target"],
    }
