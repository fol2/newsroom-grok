"""Checked v34 live-official entity-mention lineage migration and v33 backup gate."""

# fmt: off
# ruff: noqa: I001
from __future__ import annotations
import sqlite3
from pathlib import Path
from . import live_official_extraction_migrations as predecessor
from .canonical import digest_canonical

LIVE_OFFICIAL_ENTITY_MENTION_SCHEMA_VERSION = 34
LIVE_OFFICIAL_ENTITY_MENTION_MIGRATION_NAME = "live_official_entity_mention_authority_v34"
LIVE_OFFICIAL_ENTITY_MENTION_PREDECESSOR_MIGRATION_CHECKSUM = (
    "sha256:d808df71d9b5d4f9368e92fca8baacbc965994a61c9bbf24d92acba389028580"
)
LIVE_OFFICIAL_ENTITY_MENTION_PREDECESSOR_FINGERPRINT = (
    "sha256:2b297e1c4755590f5877a6afa735297e6788447b2e55937d45122d6df2094104"
)
LiveOfficialEntityMentionBackupError = predecessor.LiveOfficialExtractionBackupError
LiveOfficialEntityMentionBackupReceipt = predecessor.LiveOfficialExtractionBackupReceipt
LiveOfficialEntityMentionMigrationRecord = predecessor.LiveOfficialExtractionMigrationRecord
_helpers = predecessor._helpers


def live_official_entity_mention_backup_paths(database: str | Path) -> tuple[Path, Path]:
    source = Path(database)
    backup = source.with_name(source.name + ".pre-v34.sqlite3")
    return backup, backup.with_name(backup.name + ".sha256")


def _checked_backup(path: Path, logical: str) -> LiveOfficialEntityMentionBackupReceipt:
    digest_path = path.with_name(path.name + ".sha256")
    digest = _helpers._file_digest(path)
    if digest_path.read_text(encoding="ascii") != digest + "\n":
        raise LiveOfficialEntityMentionBackupError("backup digest differs")
    target = sqlite3.connect(f"file:{path}?mode=ro", uri=True, isolation_level=None)
    try:
        if (
            target.execute("PRAGMA user_version").fetchone()[0] != 33
            or _helpers._schema_fingerprint(target) != LIVE_OFFICIAL_ENTITY_MENTION_PREDECESSOR_FINGERPRINT
            or _helpers._logical_database_digest(target) != logical
        ):
            raise LiveOfficialEntityMentionBackupError("backup differs from source")
    finally:
        target.close()
    return LiveOfficialEntityMentionBackupReceipt(path, digest_path, digest, logical)


def prepare_live_official_entity_mention_backup(
    connection: sqlite3.Connection, backup_path: Path
) -> LiveOfficialEntityMentionBackupReceipt:
    fingerprint = _helpers._schema_fingerprint
    logical_digest = _helpers._logical_database_digest
    file_digest = _helpers._file_digest
    if (
        connection.in_transaction
        or connection.execute("PRAGMA user_version").fetchone()[0] != 33
        or fingerprint(connection) != LIVE_OFFICIAL_ENTITY_MENTION_PREDECESSOR_FINGERPRINT
        or not isinstance(backup_path, Path)
        or not backup_path.is_absolute()
    ):
        raise LiveOfficialEntityMentionBackupError("backup requires checked schema v33")
    source = Path(next(row[2] for row in connection.execute("PRAGMA database_list") if row[1] == "main"))
    digest_path = backup_path.with_name(backup_path.name + ".sha256")
    logical = logical_digest(connection)
    if not source or source.resolve() == backup_path.resolve() or backup_path.exists() != digest_path.exists():
        raise LiveOfficialEntityMentionBackupError("backup boundary differs")
    if not backup_path.exists():
        backup_path.open("xb").close()
        target = sqlite3.connect(backup_path, isolation_level=None)
        try:
            connection.backup(target)
        finally:
            target.close()
        digest_path.write_text(file_digest(backup_path) + "\n", encoding="ascii")
    receipt = _checked_backup(backup_path, logical)
    connection.execute(
        "CREATE TEMP TABLE IF NOT EXISTS live_official_entity_mention_backup_gate("
        "backup_path TEXT PRIMARY KEY,backup_digest TEXT NOT NULL,logical_digest TEXT NOT NULL) STRICT"
    )
    connection.execute("DELETE FROM live_official_entity_mention_backup_gate")
    connection.execute(
        "INSERT INTO live_official_entity_mention_backup_gate VALUES(?,?,?)",
        (str(backup_path), receipt.backup_digest, logical),
    )
    if connection.in_transaction:
        connection.commit()
    return receipt


def require_live_official_entity_mention_backup(
    connection: sqlite3.Connection,
    *,
    expected_history: tuple[tuple[int, str, str], ...],
) -> LiveOfficialEntityMentionBackupReceipt:
    logical_digest = _helpers._logical_database_digest
    try:
        row = connection.execute(
            "SELECT backup_path,backup_digest,logical_digest FROM temp.live_official_entity_mention_backup_gate"
        ).fetchone()
    except sqlite3.OperationalError as exc:
        raise LiveOfficialEntityMentionBackupError("v33 to v34 requires prepared backup") from exc
    if row is None or logical_digest(connection) != row[2]:
        raise LiveOfficialEntityMentionBackupError("v33 to v34 requires prepared backup")
    receipt = _checked_backup(Path(row[0]), str(row[2]))
    target = sqlite3.connect(f"file:{receipt.backup_path}?mode=ro", uri=True, isolation_level=None)
    try:
        history = target.execute(
            "SELECT version,name,checksum FROM authority_migrations ORDER BY version"
        ).fetchall()
    finally:
        target.close()
    if receipt.backup_digest != row[1] or history != list(expected_history):
        raise LiveOfficialEntityMentionBackupError("prepared backup is not exact v33")
    return receipt


LIVE_OFFICIAL_ENTITY_MENTION_MIGRATION_STATEMENTS: tuple[str, ...] = (
    "DROP TRIGGER entity_mention_lineage_guard",
    """CREATE TRIGGER entity_mention_lineage_guard
        BEFORE INSERT ON entity_mentions
        WHEN NOT EXISTS(
            SELECT 1
            FROM extraction_proposals p
            JOIN extraction_proposal_sets s
              ON s.proposal_set_id=p.proposal_set_id
            JOIN extraction_outputs o ON o.output_id=p.output_id
            JOIN extraction_runs r ON r.run_id=p.run_id
            JOIN extraction_proposal_evidence e ON e.proposal_id=p.proposal_id
            WHERE p.proposal_id=NEW.source_proposal_id
              AND p.proposal_kind='ENTITY_MENTION'
              AND p.proposal_set_id=NEW.proposal_set_id
              AND p.output_id=NEW.output_id
              AND p.run_id=NEW.run_id
              AND p.run_version_id=NEW.run_version_id
              AND p.canonical_digest=NEW.source_proposal_digest
              AND r.definition_id=NEW.definition_id
              AND r.definition_version_id=NEW.definition_version_id
              AND r.item_id=NEW.item_id
              AND r.revision_id=NEW.revision_id
              AND r.representation_id=NEW.representation_id
              AND e.passage_id=NEW.passage_id
              AND e.start_byte=NEW.start_byte
              AND e.end_byte=NEW.end_byte
              AND e.evidence_text_digest=NEW.evidence_text_digest
              AND (p.subject_placeholder=NEW.mention_text
                   OR json_quote(p.subject_placeholder)=NEW.mention_text)
              AND (p.confidence_basis_points IS NEW.confidence_basis_points)
              AND NOT EXISTS(
                  SELECT 1 FROM extraction_proposal_evidence other
                  WHERE other.proposal_id=p.proposal_id
                    AND other.evidence_ordinal!=e.evidence_ordinal
              )
        )
        BEGIN SELECT RAISE(ABORT,'entity mention extraction lineage mismatch'); END""",
)
LIVE_OFFICIAL_ENTITY_MENTION_MIGRATION_CHECKSUM = digest_canonical({
    "version": LIVE_OFFICIAL_ENTITY_MENTION_SCHEMA_VERSION,
    "name": LIVE_OFFICIAL_ENTITY_MENTION_MIGRATION_NAME,
    "statements": list(LIVE_OFFICIAL_ENTITY_MENTION_MIGRATION_STATEMENTS),
})
LIVE_OFFICIAL_ENTITY_MENTION_MIGRATION = LiveOfficialEntityMentionMigrationRecord(
    LIVE_OFFICIAL_ENTITY_MENTION_SCHEMA_VERSION,
    LIVE_OFFICIAL_ENTITY_MENTION_MIGRATION_NAME,
    LIVE_OFFICIAL_ENTITY_MENTION_MIGRATION_CHECKSUM,
)
__all__ = [name for name in globals() if name.startswith(("LIVE_OFFICIAL_ENTITY_MENTION_", "LiveOfficialEntityMention", "live_official_entity_mention_", "prepare_", "require_"))]
# fmt: on
