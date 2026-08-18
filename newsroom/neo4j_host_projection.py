"""Wipe+rebuild the host Neo4j projection from the SQLite ledger onto B2 types.

Stock `native_structural_mapping_v1` ignores live first-boot event types.
This projector maps every current ledger row onto existing B2 labels/rels
only. Neo4j stays a disposable projection. SQLite remains authority.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from newsroom.authority.canonical import digest_canonical
from newsroom.projection.mapping import (
    ProjectionIdentitySource,
    StructuralIdentityContext,
    StructuralNodeBinding,
    canonical_node_id,
)
from newsroom.projection.neo4j.models import (
    NEO4J_B2_DRIVER_VERSION,
    NEO4J_B2_SERVER_VERSION,
    Neo4jCompatibilityError,
    Neo4jConfigurationError,
    Neo4jConnectionError,
    Neo4jProjectorConfig,
    Neo4jWriteError,
)
from newsroom.projection.ontology import ProjectionNodeType, ProjectionRelationType


COMMAND = "project-neo4j"
DEFAULT_URI = "bolt://127.0.0.1:7687"
DEFAULT_DATABASE = "neo4j"
AUTH_RELATIVE = Path("neo4j") / "auth"

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
_SIGNAL_EVENT_TYPES = frozenset(
    {"discovery.signal.admitted", "discovery.signal.skipped"}
)
_COMPONENT_QUERY = """
CALL dbms.components() YIELD name, versions, edition
WHERE name = 'Neo4j Kernel'
RETURN versions[0] AS version, toLower(edition) AS edition
"""
_WIPE_QUERY = """
MATCH (n)
WHERE n:AUTHORITY_AGGREGATE OR n:AUTHORITY_VERSION OR n:SOURCE_ITEM
   OR n:SOURCE_REVISION OR n:SOURCE_REPRESENTATION OR n:SIGNAL
   OR n:LEAD OR n:PAYLOAD OR n:LEDGER_EVENT
DETACH DELETE n
RETURN count(*) AS deleted
"""


@dataclass(frozen=True, slots=True)
class HostLedgerEvent:
    ledger_seq: int
    event_id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    payload_id: str
    payload_digest: str
    payload: Mapping[str, object]
    principal_id: str
    recorded_at: str


@dataclass(frozen=True, slots=True)
class PlannedNode:
    label: str
    canonical_id: str
    properties: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class PlannedRelation:
    rel_type: str
    source_canonical_id: str
    target_canonical_id: str
    relation_key: str
    properties: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class ProjectionPlan:
    through_ledger_seq: int
    event_count: int
    nodes: tuple[PlannedNode, ...]
    relations: tuple[PlannedRelation, ...]


def load_auth_file(path: Path) -> dict[str, str]:
    auth = Path(path)
    if auth.is_symlink():
        raise ValueError("Neo4j auth file must not be a symlink")
    if not auth.is_file():
        raise ValueError("Neo4j auth file is missing")
    mode = auth.stat().st_mode & 0o777
    if mode != 0o600:
        raise ValueError("Neo4j auth file must be mode 600")
    values: dict[str, str] = {}
    for line in auth.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key] = value
    if "PROJECTOR_USER" not in values or "PROJECTOR_PASSWORD" not in values:
        raise ValueError("Neo4j auth file is missing projector credentials")
    return values


def load_projector_environment(
    home: Path,
    environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    values = dict(os.environ if environment is None else environment)
    file_values = load_auth_file(Path(home) / AUTH_RELATIVE)
    if not values.get("NEWSROOM_NEO4J_PROJECTOR_USERNAME"):
        values["NEWSROOM_NEO4J_PROJECTOR_USERNAME"] = file_values["PROJECTOR_USER"]
    if not values.get("NEWSROOM_NEO4J_PROJECTOR_PASSWORD"):
        values["NEWSROOM_NEO4J_PROJECTOR_PASSWORD"] = file_values["PROJECTOR_PASSWORD"]
    values.setdefault("NEWSROOM_NEO4J_URI", DEFAULT_URI)
    values.setdefault("NEWSROOM_NEO4J_DATABASE", DEFAULT_DATABASE)
    return values


def projector_config_from_values(values: Mapping[str, str]) -> Neo4jProjectorConfig:
    config = Neo4jProjectorConfig.from_environment(values)
    host = urlsplit(config.uri).hostname
    if host != "127.0.0.1":
        raise Neo4jConfigurationError("Neo4j URI must use loopback 127.0.0.1")
    return config


def load_host_ledger_events(db: Path) -> list[HostLedgerEvent]:
    path = Path(db)
    if not path.is_file() or path.is_symlink():
        raise ValueError("ledger missing; run newsroom-first-boot start first")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        rows = conn.execute(
            "SELECT e.ledger_seq, e.event_id, e.event_type, e.aggregate_type, "
            "e.aggregate_id, e.aggregate_version, e.payload_id, e.payload_digest, "
            "e.principal_id, e.recorded_at, p.payload_bytes "
            "FROM ledger_events e "
            "JOIN authority_payloads p ON p.payload_id = e.payload_id "
            "ORDER BY e.ledger_seq"
        ).fetchall()
    finally:
        conn.close()
    events: list[HostLedgerEvent] = []
    for row in rows:
        raw = row[10]
        if isinstance(raw, bytes):
            payload_obj: object = json.loads(raw.decode("utf-8"))
        elif isinstance(raw, str):
            payload_obj = json.loads(raw)
        else:
            payload_obj = {}
        payload = payload_obj if isinstance(payload_obj, dict) else {}
        events.append(
            HostLedgerEvent(
                ledger_seq=int(row[0]),
                event_id=str(row[1]),
                event_type=str(row[2]),
                aggregate_type=str(row[3]),
                aggregate_id=str(row[4]),
                aggregate_version=int(row[5]),
                payload_id=str(row[6]),
                payload_digest=str(row[7]),
                payload=payload,
                principal_id=str(row[8]),
                recorded_at=str(row[9]),
            )
        )
    return events


def plan_host_projection(events: Sequence[HostLedgerEvent]) -> ProjectionPlan:
    nodes: dict[tuple[str, str], PlannedNode] = {}
    relations: dict[tuple[str, str, str, str], PlannedRelation] = {}
    for event in events:
        for node in _nodes_for_event(event):
            key = (node.label, node.canonical_id)
            existing = nodes.get(key)
            if existing is None:
                nodes[key] = node
                continue
            if int(node.properties["first_ledger_seq"]) < int(
                existing.properties["first_ledger_seq"]
            ):
                nodes[key] = node
        for relation in _relations_for_event(event):
            key = (
                relation.rel_type,
                relation.source_canonical_id,
                relation.target_canonical_id,
                relation.relation_key,
            )
            relations[key] = relation
    ordered_nodes = tuple(sorted(nodes.values(), key=lambda item: (item.label, item.canonical_id)))
    ordered_rels = tuple(
        sorted(
            relations.values(),
            key=lambda item: (
                item.rel_type,
                item.source_canonical_id,
                item.target_canonical_id,
                item.relation_key,
            ),
        )
    )
    through = max((event.ledger_seq for event in events), default=0)
    return ProjectionPlan(
        through_ledger_seq=through,
        event_count=len(events),
        nodes=ordered_nodes,
        relations=ordered_rels,
    )


def open_host_driver(config: Neo4jProjectorConfig) -> tuple[Any, str]:
    try:
        import neo4j
        from neo4j import GraphDatabase
    except Exception as exc:
        raise Neo4jConnectionError("Neo4j projector driver creation failed") from exc
    version = str(neo4j.__version__)
    if version != NEO4J_B2_DRIVER_VERSION:
        raise Neo4jCompatibilityError(
            "Neo4j driver is not the exact B2 qualification target"
        )
    try:
        driver = GraphDatabase.driver(config.uri, auth=(config.username, config.password))
    except Exception as exc:
        raise Neo4jConnectionError("Neo4j projector driver creation failed") from exc
    return driver, version


def project_host_neo4j(
    home: Path,
    *,
    environment: Mapping[str, str] | None = None,
    driver_factory: Callable[[Neo4jProjectorConfig], tuple[Any, str]] | None = None,
) -> dict[str, Any]:
    db = Path(home) / "data" / "authority.sqlite3"
    events = load_host_ledger_events(db)
    plan = plan_host_projection(events)
    values = load_projector_environment(home, environment)
    config = projector_config_from_values(values)
    factory = driver_factory or open_host_driver
    driver, driver_version = factory(config)
    try:
        driver.verify_connectivity()
        with driver.session(database=config.database) as session:
            compatibility = _require_components(session)
            if driver_version != NEO4J_B2_DRIVER_VERSION:
                raise Neo4jCompatibilityError(
                    "Neo4j driver is not the exact B2 qualification target"
                )
            session.run(_WIPE_QUERY).consume()
            for node in plan.nodes:
                _merge_node(session, node)
            for relation in plan.relations:
                _merge_relation(session, relation)
            node_counts = _count_nodes(session)
            relation_counts = _count_relations(session)
    except (Neo4jCompatibilityError, Neo4jConfigurationError, Neo4jConnectionError):
        raise
    except Exception as exc:
        raise Neo4jWriteError("Neo4j host projection write failed") from exc
    finally:
        close = getattr(driver, "close", None)
        if callable(close):
            close()
    return {
        "ok": True,
        "through_ledger_seq": plan.through_ledger_seq,
        "event_count": plan.event_count,
        "node_counts": node_counts,
        "relation_counts": relation_counts,
        "wiped": True,
        "server_version": compatibility.server_version,
        "edition": compatibility.edition,
        "home": str(home),
        "ledger_path": str(db),
    }


def _require_components(session: Any) -> Any:
    from newsroom.projection.neo4j.models import Neo4jCompatibility

    try:
        record = session.run(_COMPONENT_QUERY).single()
    except Exception as exc:
        raise Neo4jConnectionError(
            "Neo4j authenticated compatibility check failed"
        ) from exc
    if record is None:
        raise Neo4jCompatibilityError("Neo4j service did not identify its component")
    try:
        server_version = str(record["version"])
        edition = str(record["edition"]).lower()
    except Exception as exc:
        raise Neo4jCompatibilityError(
            "Neo4j service returned malformed compatibility metadata"
        ) from exc
    compatibility = Neo4jCompatibility(
        server_version=server_version,
        edition=edition,
        driver_version=NEO4J_B2_DRIVER_VERSION,
    )
    if compatibility.server_version != NEO4J_B2_SERVER_VERSION:
        raise Neo4jCompatibilityError(
            "Neo4j server is not the exact B2 qualification target"
        )
    if compatibility.edition != "community":
        raise Neo4jCompatibilityError(
            "Neo4j edition is not the exact Community qualification target"
        )
    return compatibility


def _merge_node(session: Any, node: PlannedNode) -> None:
    label = _require_label(node.label)
    properties = dict(node.properties)
    properties["canonical_id"] = node.canonical_id
    properties["entity_type"] = label
    session.run(
        f"MERGE (n:{label} {{canonical_id: $canonical_id}})\n"
        "SET n = $properties\n"
        "RETURN n.canonical_id AS canonical_id",
        {
            "canonical_id": node.canonical_id,
            "label": label,
            "entity_type": label,
            "properties": properties,
        },
    ).consume()


def _merge_relation(session: Any, relation: PlannedRelation) -> None:
    rel_type = _require_rel(relation.rel_type)
    properties = dict(relation.properties)
    properties["relation_key"] = relation.relation_key
    session.run(
        "MATCH (source {canonical_id: $source_canonical_id})\n"
        "MATCH (target {canonical_id: $target_canonical_id})\n"
        f"MERGE (source)-[r:{rel_type} {{relation_key: $relation_key}}]->(target)\n"
        "SET r += $properties\n"
        "RETURN type(r) AS rel_type",
        {
            "rel_type": rel_type,
            "source_canonical_id": relation.source_canonical_id,
            "target_canonical_id": relation.target_canonical_id,
            "relation_key": relation.relation_key,
            "properties": properties,
        },
    ).consume()


def _count_nodes(session: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for label in sorted(B2_NODE_LABELS):
        record = session.run(
            f"MATCH (n:{label}) RETURN count(n) AS count",
            {"label": label},
        ).single()
        counts[label] = 0 if record is None else int(record["count"])
    return counts


def _count_relations(session: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for rel_type in sorted(B2_RELATION_TYPES):
        record = session.run(
            f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS count",
            {"rel_type": rel_type},
        ).single()
        counts[rel_type] = 0 if record is None else int(record["count"])
    return counts


def _nodes_for_event(event: HostLedgerEvent) -> list[PlannedNode]:
    nodes = [
        _node(
            event,
            ProjectionNodeType.AUTHORITY_AGGREGATE,
            ProjectionIdentitySource.AGGREGATE,
        ),
        _node(
            event,
            ProjectionNodeType.AUTHORITY_VERSION,
            ProjectionIdentitySource.AGGREGATE_VERSION,
        ),
        _node(
            event,
            ProjectionNodeType.LEDGER_EVENT,
            ProjectionIdentitySource.EVENT,
            extra={"event_id": event.event_id, "event_type": event.event_type},
        ),
        _node(
            event,
            ProjectionNodeType.PAYLOAD,
            ProjectionIdentitySource.PAYLOAD,
            extra={"payload_id": event.payload_id, "payload_digest": event.payload_digest},
        ),
    ]
    if event.event_type in _SIGNAL_EVENT_TYPES:
        extra = {}
        source_id = event.payload.get("source_id")
        if isinstance(source_id, str) and source_id:
            extra["source_id"] = source_id
        nodes.append(
            _node(
                event,
                ProjectionNodeType.SIGNAL,
                ProjectionIdentitySource.AGGREGATE,
                extra=extra,
            )
        )
        if isinstance(source_id, str) and source_id:
            nodes.append(
                _node(
                    event,
                    ProjectionNodeType.SOURCE_ITEM,
                    ProjectionIdentitySource.PAYLOAD_FIELD,
                    payload_field="source_id",
                    extra={"source_id": source_id},
                )
            )
        item_id = event.payload.get("item_id")
        if isinstance(item_id, str) and item_id:
            nodes.append(
                _node(
                    event,
                    ProjectionNodeType.SOURCE_REVISION,
                    ProjectionIdentitySource.PAYLOAD_FIELD,
                    payload_field="item_id",
                    extra={"item_id": item_id},
                )
            )
        url = event.payload.get("url")
        if isinstance(url, str) and url and isinstance(item_id, str) and item_id:
            nodes.append(
                _node(
                    event,
                    ProjectionNodeType.SOURCE_REPRESENTATION,
                    ProjectionIdentitySource.PAYLOAD_FIELD,
                    payload_field="url",
                    extra={},
                )
            )
    return nodes


def _relations_for_event(event: HostLedgerEvent) -> list[PlannedRelation]:
    context = _context(event)
    aggregate = canonical_node_id(
        _binding(ProjectionNodeType.AUTHORITY_AGGREGATE, ProjectionIdentitySource.AGGREGATE),
        context,
    )
    version = canonical_node_id(
        _binding(
            ProjectionNodeType.AUTHORITY_VERSION,
            ProjectionIdentitySource.AGGREGATE_VERSION,
        ),
        context,
    )
    ledger_event = canonical_node_id(
        _binding(ProjectionNodeType.LEDGER_EVENT, ProjectionIdentitySource.EVENT),
        context,
    )
    payload = canonical_node_id(
        _binding(ProjectionNodeType.PAYLOAD, ProjectionIdentitySource.PAYLOAD),
        context,
    )
    rels = [
        _rel(ProjectionRelationType.HAS_VERSION.value, aggregate, version, event),
        _rel(ProjectionRelationType.CONTAINS_PAYLOAD.value, version, payload, event),
        _rel(ProjectionRelationType.CONTAINS_PAYLOAD.value, ledger_event, payload, event),
        _rel(ProjectionRelationType.PROJECTED_FROM_EVENT.value, aggregate, ledger_event, event),
        _rel(ProjectionRelationType.PROJECTED_FROM_EVENT.value, version, ledger_event, event),
        _rel(ProjectionRelationType.PROJECTED_FROM_EVENT.value, payload, ledger_event, event),
    ]
    if event.event_type in _SIGNAL_EVENT_TYPES:
        signal = canonical_node_id(
            _binding(ProjectionNodeType.SIGNAL, ProjectionIdentitySource.AGGREGATE),
            context,
        )
        rels.append(
            _rel(ProjectionRelationType.PROJECTED_FROM_EVENT.value, signal, ledger_event, event)
        )
        source_id = event.payload.get("source_id")
        item_id = event.payload.get("item_id")
        url = event.payload.get("url")
        item = None
        revision = None
        representation = None
        if isinstance(source_id, str) and source_id:
            item = canonical_node_id(
                _binding(
                    ProjectionNodeType.SOURCE_ITEM,
                    ProjectionIdentitySource.PAYLOAD_FIELD,
                    "source_id",
                ),
                context,
            )
            rels.append(
                _rel(ProjectionRelationType.PROJECTED_FROM_EVENT.value, item, ledger_event, event)
            )
        if isinstance(item_id, str) and item_id:
            revision = canonical_node_id(
                _binding(
                    ProjectionNodeType.SOURCE_REVISION,
                    ProjectionIdentitySource.PAYLOAD_FIELD,
                    "item_id",
                ),
                context,
            )
            rels.append(
                _rel(ProjectionRelationType.PROJECTED_FROM_EVENT.value, revision, ledger_event, event)
            )
            if item is not None:
                rels.append(_rel(ProjectionRelationType.HAS_REVISION.value, item, revision, event))
            rels.append(_rel(ProjectionRelationType.PRODUCED_SIGNAL.value, revision, signal, event))
        if (
            isinstance(url, str)
            and url
            and isinstance(item_id, str)
            and item_id
        ):
            representation = canonical_node_id(
                _binding(
                    ProjectionNodeType.SOURCE_REPRESENTATION,
                    ProjectionIdentitySource.PAYLOAD_FIELD,
                    "url",
                ),
                context,
            )
            rels.append(
                _rel(
                    ProjectionRelationType.PROJECTED_FROM_EVENT.value,
                    representation,
                    ledger_event,
                    event,
                )
            )
            if revision is not None:
                rels.append(
                    _rel(
                        ProjectionRelationType.HAS_REPRESENTATION.value,
                        revision,
                        representation,
                        event,
                    )
                )
            rels.append(
                _rel(
                    ProjectionRelationType.PRODUCED_SIGNAL.value,
                    representation,
                    signal,
                    event,
                )
            )
    return rels


def _context(event: HostLedgerEvent) -> StructuralIdentityContext:
    return StructuralIdentityContext(
        aggregate_type=event.aggregate_type,
        aggregate_id=event.aggregate_id,
        aggregate_version=event.aggregate_version,
        event_id=event.event_id,
        payload_id=event.payload_id,
        payload=event.payload,
    )


def _binding(
    node_type: ProjectionNodeType,
    identity_source: ProjectionIdentitySource,
    payload_field: str | None = None,
) -> StructuralNodeBinding:
    return StructuralNodeBinding(
        alias=node_type.value.lower(),
        node_type=node_type,
        identity_source=identity_source,
        payload_field=payload_field,
    )


def _node(
    event: HostLedgerEvent,
    node_type: ProjectionNodeType,
    identity_source: ProjectionIdentitySource,
    *,
    payload_field: str | None = None,
    extra: Mapping[str, object] | None = None,
) -> PlannedNode:
    binding = _binding(node_type, identity_source, payload_field)
    canonical = canonical_node_id(binding, _context(event))
    properties: dict[str, object] = {
        "canonical_id": canonical,
        "entity_type": node_type.value,
        "first_ledger_seq": event.ledger_seq,
        "aggregate_type": event.aggregate_type,
        "aggregate_id": event.aggregate_id,
    }
    if extra:
        properties.update(extra)
    return PlannedNode(label=node_type.value, canonical_id=canonical, properties=properties)


def _rel(
    rel_type: str,
    source: str,
    target: str,
    event: HostLedgerEvent,
) -> PlannedRelation:
    key = digest_canonical(
        {
            "rel_type": rel_type,
            "source": source,
            "target": target,
            "ledger_seq": event.ledger_seq,
            "event_id": event.event_id,
        }
    )
    return PlannedRelation(
        rel_type=rel_type,
        source_canonical_id=source,
        target_canonical_id=target,
        relation_key=key,
        properties={
            "ledger_seq": event.ledger_seq,
            "event_id": event.event_id,
            "event_type": event.event_type,
        },
    )


def _require_label(label: str) -> str:
    if label not in B2_NODE_LABELS:
        raise Neo4jWriteError("projection refused a non-B2 node label")
    return label


def _require_rel(rel_type: str) -> str:
    if rel_type not in B2_RELATION_TYPES:
        raise Neo4jWriteError("projection refused a non-B2 relation type")
    return rel_type
