"""Host Neo4j wipe+rebuild from the SQLite ledger onto existing B2 types."""

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
from newsroom.projection.mapping import native_structural_mapping_v1
from newsroom.projection.neo4j.models import (
    NEO4J_B2_DRIVER_VERSION,
    NEO4J_B2_SERVER_VERSION,
    Neo4jCompatibilityError,
    Neo4jConfigurationError,
)
from newsroom.projection.ontology import native_ontology_v1


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "newsroom_first_boot.py"
LIVE_EVENT_TYPES = (
    "autonomy.envelope.granted",
    "discovery.signal.admitted",
    "publication.decision.authorised",
    "target.operation.dispatched",
    "auto_publish.granted",
    "internal_beta.granted",
    "discovery.signal.skipped",
    "publication.bundle.with_body.minted",
)
B2_NODE_LABELS = frozenset(
    {
        "AUTHORITY_AGGREGATE",
        "AUTHORITY_VERSION",
        "SOURCE_ITEM",
        "SOURCE_REVISION",
        "SOURCE_REPRESENTATION",
        "SIGNAL",
        "LEAD",
        "PAYLOAD",
        "LEDGER_EVENT",
    }
)
B2_RELATION_TYPES = frozenset(
    {
        "HAS_VERSION",
        "HAS_REVISION",
        "HAS_REPRESENTATION",
        "PRODUCED_SIGNAL",
        "PROMOTED_TO_LEAD",
        "DERIVED_FROM",
        "CONTAINS_PAYLOAD",
        "PROJECTED_FROM_EVENT",
    }
)
FORBIDDEN_LABELS = frozenset(
    {
        "GRANT",
        "TARGET_OPERATION",
        "TARGETOPERATION",
        "PUBLICATION_BUNDLE",
        "ENVELOPE",
    }
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


def _write_auth(path: Path, *, user: str = "newsroom_projector", mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "NEO4J_USER=neo4j\n"
        "NEO4J_PASSWORD=bootstrap-secret-value\n"
        f"PROJECTOR_USER={user}\n"
        "PROJECTOR_PASSWORD=projector-secret-value\n",
        encoding="utf-8",
    )
    os.chmod(path, mode)


def _seed_ledger(db: Path) -> list[str]:
    db.parent.mkdir(parents=True, exist_ok=True)
    events = (
        (1, "autonomy.envelope.granted", "autonomy.envelope", "agg-envelope", {"controller": "grok_bot"}),
        (
            2,
            "discovery.signal.admitted",
            "discovery.signal",
            "agg-signal-hk01",
            {
                "source_id": "HK-01",
                "item_id": "https://www.news.gov.hk/item",
                "url": "https://www.news.gov.hk/feed.xml",
                "adapter": "official_source_definition_rss",
            },
        ),
        (
            3,
            "publication.decision.authorised",
            "publication.decision",
            "agg-decision",
            {"bundle_digest": "sha256:" + ("ab" * 32), "authorising": True},
        ),
        (
            4,
            "target.operation.dispatched",
            "target.operation",
            "agg-target",
            {"operation": "record", "target": "host.authority.ledger"},
        ),
        (5, "auto_publish.granted", "auto_publish.grant", "agg-auto", {"semantic": "AUTO-010"}),
        (6, "internal_beta.granted", "internal_beta.grant", "agg-beta", {"target": "internal.beta.origin"}),
        (
            7,
            "target.operation.dispatched",
            "internal_beta.publish",
            "agg-beta-pub",
            {"operation": "publish", "target": "internal.beta.origin"},
        ),
        (
            8,
            "discovery.signal.skipped",
            "discovery.signal.skip",
            "agg-skip",
            {"source_id": "UK-10", "reason": "RSS/Atom feed has no item"},
        ),
        (
            9,
            "publication.bundle.with_body.minted",
            "publication.bundle",
            "agg-bundle",
            {
                "source_id": "HK-01",
                "story_title": "Readable title",
                "story_body": "Readable body that is long enough.",
            },
        ),
    )
    conn = sqlite3.connect(db)
    try:
        conn.execute(
            "CREATE TABLE ledger_events ("
            "ledger_seq INTEGER PRIMARY KEY,"
            "event_id TEXT,"
            "event_type TEXT,"
            "aggregate_type TEXT,"
            "aggregate_id TEXT,"
            "aggregate_version INTEGER,"
            "payload_id TEXT,"
            "payload_digest TEXT,"
            "principal_id TEXT,"
            "recorded_at TEXT"
            ")"
        )
        conn.execute(
            "CREATE TABLE authority_payloads ("
            "payload_id TEXT PRIMARY KEY,"
            "payload_digest TEXT,"
            "payload_bytes BLOB"
            ")"
        )
        for seq, event_type, aggregate_type, aggregate_id, payload in events:
            payload_id = f"payload-{seq}"
            payload_digest = f"sha256:{seq:064x}"
            event_id = f"event-{seq:08d}-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
            conn.execute(
                "INSERT INTO authority_payloads VALUES (?,?,?)",
                (payload_id, payload_digest, json.dumps(payload, sort_keys=True).encode("utf-8")),
            )
            conn.execute(
                "INSERT INTO ledger_events VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    seq,
                    event_id,
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    1,
                    payload_id,
                    payload_digest,
                    "owner.newsroom",
                    "2026-08-18T00:00:00Z",
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return [item[1] for item in events]


class _FakeRecord(dict):
    pass


class _FakeResult:
    def __init__(self, record: dict[str, object] | None = None) -> None:
        self._record = None if record is None else _FakeRecord(record)

    def single(self) -> _FakeRecord | None:
        return self._record

    def consume(self) -> None:
        return None


class _FakeSession:
    def __init__(self, store: dict[str, object]) -> None:
        self.store = store

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def run(self, query: str, parameters: dict[str, object] | None = None, **kwargs: object) -> _FakeResult:
        params = dict(parameters or {})
        params.update(kwargs)
        self.store.setdefault("queries", []).append(query)
        if "dbms.components()" in query:
            return _FakeResult(
                {
                    "version": self.store.get("version", NEO4J_B2_SERVER_VERSION),
                    "edition": self.store.get("edition", "community"),
                }
            )
        if "DETACH DELETE" in query:
            self.store["wipes"] = int(self.store.get("wipes", 0)) + 1
            self.store["nodes"] = {}
            self.store["rels"] = []
            return _FakeResult({"deleted": 1})
        if "MERGE (n:" in query or "MERGE (n {" in query:
            nodes = self.store.setdefault("nodes", {})
            key = str(params.get("canonical_id"))
            nodes[key] = {
                "label": params.get("label") or params.get("entity_type"),
                "properties": dict(params.get("properties") or {}),
            }
            return _FakeResult({"canonical_id": key})
        if "MERGE (source)-[r:" in query or "MERGE (source)-[" in query:
            rels = self.store.setdefault("rels", [])
            rels.append(
                {
                    "type": params.get("rel_type"),
                    "source": params.get("source_canonical_id"),
                    "target": params.get("target_canonical_id"),
                }
            )
            return _FakeResult({"ok": True})
        if "count(n) AS count" in query or "count(n) as count" in query.lower():
            label = str(params.get("label") or "")
            nodes = self.store.get("nodes") or {}
            count = sum(1 for item in nodes.values() if item.get("label") == label)
            return _FakeResult({"count": count, "label": label})
        if "type(r) AS rel_type" in query or "count(r) AS count" in query:
            rels = self.store.get("rels") or []
            return _FakeResult({"count": len(rels)})
        return _FakeResult({})


class _FakeDriver:
    def __init__(self, store: dict[str, object]) -> None:
        self.store = store
        self.closed = False

    def verify_connectivity(self) -> None:
        self.store["connected"] = True

    def session(self, database: str | None = None) -> _FakeSession:
        self.store["database"] = database
        return _FakeSession(self.store)

    def close(self) -> None:
        self.closed = True


def test_shared_ledger_path_stays_on_the_box_home() -> None:
    assert SHARED_HOME == Path("/home/box/newsroom")
    assert ledger_path(SHARED_HOME) == Path("/home/box/newsroom/data/authority.sqlite3")


def test_stock_structural_mapping_ignores_live_first_boot_types() -> None:
    contract = native_structural_mapping_v1(native_ontology_v1())
    mapped = {item.event_type for item in contract.mappings}
    assert not set(LIVE_EVENT_TYPES) & mapped


def test_plan_projects_every_live_event_type_onto_b2_only(tmp_path: Path) -> None:
    from newsroom.neo4j_host_projection import plan_host_projection

    events = _events_from_seeded_db(tmp_path)
    plan = plan_host_projection(events)
    assert plan.through_ledger_seq == 9
    assert plan.event_count == 9
    assert {event.event_type for event in events} >= set(LIVE_EVENT_TYPES)
    labels = {node.label for node in plan.nodes}
    rels = {rel.rel_type for rel in plan.relations}
    assert labels <= B2_NODE_LABELS
    assert rels <= B2_RELATION_TYPES
    assert not (labels & FORBIDDEN_LABELS)
    assert "LEDGER_EVENT" in labels
    assert "SIGNAL" in labels
    assert "PAYLOAD" in labels
    assert "AUTHORITY_AGGREGATE" in labels
    event_ids = {node.properties["event_id"] for node in plan.nodes if node.label == "LEDGER_EVENT"}
    assert len(event_ids) == 9
    signal_events = [
        event.event_type
        for event in events
        if any(
            node.label == "SIGNAL" and node.properties.get("aggregate_id") == event.aggregate_id
            for node in plan.nodes
        )
    ]
    assert "discovery.signal.admitted" in signal_events
    assert "discovery.signal.skipped" in signal_events


def _events_from_seeded_db(tmp_path: Path) -> list[object]:
    from newsroom.neo4j_host_projection import load_host_ledger_events

    db = tmp_path / "authority.sqlite3"
    _seed_ledger(db)
    return load_host_ledger_events(db)


def test_plan_is_deterministic_for_an_identical_ledger(tmp_path: Path) -> None:
    from newsroom.neo4j_host_projection import plan_host_projection

    events = _events_from_seeded_db(tmp_path)
    first = plan_host_projection(events)
    second = plan_host_projection(events)
    assert first.nodes == second.nodes
    assert first.relations == second.relations


def test_projector_config_rejects_bootstrap_username() -> None:
    from newsroom.neo4j_host_projection import projector_config_from_values

    with pytest.raises(Neo4jConfigurationError):
        projector_config_from_values(
            {
                "NEWSROOM_NEO4J_URI": "bolt://127.0.0.1:7687",
                "NEWSROOM_NEO4J_DATABASE": "neo4j",
                "NEWSROOM_NEO4J_PROJECTOR_USERNAME": "neo4j",
                "NEWSROOM_NEO4J_PROJECTOR_PASSWORD": "x",
            }
        )


def test_auth_loader_uses_projector_user_and_requires_mode_600(tmp_path: Path) -> None:
    from newsroom.neo4j_host_projection import load_projector_environment

    home = tmp_path / "newsroom"
    auth = home / "neo4j" / "auth"
    _write_auth(auth, mode=0o644)
    with pytest.raises(ValueError, match="mode 600"):
        load_projector_environment(home)
    _write_auth(auth, user="newsroom_projector", mode=0o600)
    values = load_projector_environment(home)
    assert values["NEWSROOM_NEO4J_PROJECTOR_USERNAME"] == "newsroom_projector"
    assert values["NEWSROOM_NEO4J_PROJECTOR_USERNAME"] != "neo4j"
    assert values["NEWSROOM_NEO4J_URI"] == "bolt://127.0.0.1:7687"
    assert "projector-secret-value" not in json.dumps(
        {key: values[key] for key in values if "PASSWORD" not in key}
    )


def test_wipe_rebuild_is_idempotent_and_does_not_write_sqlite(tmp_path: Path) -> None:
    from newsroom.neo4j_host_projection import project_host_neo4j

    home = tmp_path / "newsroom"
    db = home / "data" / "authority.sqlite3"
    _seed_ledger(db)
    _write_auth(home / "neo4j" / "auth")
    before = db.read_bytes()
    store: dict[str, object] = {}

    def factory(_config: object) -> tuple[_FakeDriver, str]:
        return _FakeDriver(store), NEO4J_B2_DRIVER_VERSION

    first = project_host_neo4j(home, driver_factory=factory)
    second = project_host_neo4j(home, driver_factory=factory)
    assert first["ok"] is True
    assert first["through_ledger_seq"] == 9
    assert first["event_count"] == 9
    assert first["node_counts"]["LEDGER_EVENT"] == 9
    assert first["node_counts"]["SIGNAL"] >= 2
    assert second["node_counts"] == first["node_counts"]
    assert second["relation_counts"] == first["relation_counts"]
    assert int(store["wipes"]) == 2
    assert db.read_bytes() == before
    assert first["server_version"] == NEO4J_B2_SERVER_VERSION
    assert first["edition"] == "community"
    assert "password" not in json.dumps(first).lower()
    assert "projector-secret-value" not in json.dumps(first)


def test_wrong_server_identity_fails_closed(tmp_path: Path) -> None:
    from newsroom.neo4j_host_projection import project_host_neo4j

    home = tmp_path / "newsroom"
    _seed_ledger(home / "data" / "authority.sqlite3")
    _write_auth(home / "neo4j" / "auth")
    store = {"version": "5.26.2", "edition": "community"}

    def factory(_config: object) -> tuple[_FakeDriver, str]:
        return _FakeDriver(store), NEO4J_B2_DRIVER_VERSION

    with pytest.raises(Neo4jCompatibilityError):
        project_host_neo4j(home, driver_factory=factory)


def test_project_neo4j_cli_reports_counts_and_honors_restore_pause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from newsroom import neo4j_host_projection as module

    home = tmp_path / "newsroom"
    _seed_ledger(home / "data" / "authority.sqlite3")
    _write_auth(home / "neo4j" / "auth")
    store: dict[str, object] = {}

    def factory(_config: object) -> tuple[_FakeDriver, str]:
        return _FakeDriver(store), NEO4J_B2_DRIVER_VERSION

    monkeypatch.setattr(module, "open_host_driver", factory)
    # first_boot imports the function by name inside the handler; patch the module.
    monkeypatch.setenv("NEWSROOM_NEO4J_URI", "bolt://127.0.0.1:7687")
    monkeypatch.setenv("NEWSROOM_NEO4J_DATABASE", "neo4j")
    monkeypatch.setenv("NEWSROOM_NEO4J_PROJECTOR_USERNAME", "newsroom_projector")
    monkeypatch.setenv("NEWSROOM_NEO4J_PROJECTOR_PASSWORD", "projector-secret-value")

    # Direct module call proves the CLI wiring target before subprocess PATH tricks.
    from newsroom.first_boot import project_neo4j

    report = project_neo4j(home)
    assert report["ok"] is True
    assert report["through_ledger_seq"] == 9
    assert report["event_count"] == 9

    paused = home / "logs" / "restore.paused"
    paused.parent.mkdir(parents=True, exist_ok=True)
    paused.write_text("paused\n", encoding="utf-8")
    os.chmod(paused, 0o600)
    with pytest.raises(Exception, match="paused"):
        project_neo4j(home)


def test_cli_accepts_project_neo4j_choice() -> None:
    help_text = _cli("project-neo4j", "--help", home=Path("/tmp/newsroom-help-home")).stdout
    if not help_text:
        help_text = subprocess.run(
            [sys.executable, str(SCRIPT), "--help"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        ).stdout
    assert "project-neo4j" in help_text
