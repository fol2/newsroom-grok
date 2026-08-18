"""Checked v33 live-official extraction-profile migration and v32 backup gate."""

# fmt: off
# ruff: noqa: I001
from __future__ import annotations
import sqlite3
from pathlib import Path
from . import increment8_recovery_migrations as predecessor
from .canonical import digest_canonical
from .graphiti_adapter_migrations import GRAPHITI_ADAPTER_MIGRATION_STATEMENTS

LIVE_OFFICIAL_EXTRACTION_SCHEMA_VERSION = 33
LIVE_OFFICIAL_EXTRACTION_MIGRATION_NAME = "live_official_extraction_authority_v33"
LIVE_OFFICIAL_EXTRACTION_PREDECESSOR_MIGRATION_CHECKSUM = (
    "sha256:513d983ce8f21f576c08b6a99337f3164025b73e588867d8dde4d500805f79ee"
)
LIVE_OFFICIAL_EXTRACTION_PREDECESSOR_FINGERPRINT = (
    "sha256:3439b82ec6d212116e54765d50cace4d7f147b6ecc3e6ff84146b523c6fd5676"
)
LiveOfficialExtractionBackupError = predecessor.Increment8RecoveryBackupError
LiveOfficialExtractionBackupReceipt = predecessor.Increment8RecoveryBackupReceipt
LiveOfficialExtractionMigrationRecord = predecessor.Increment8RecoveryMigrationRecord
_helpers = predecessor._helpers


def live_official_extraction_backup_paths(database: str | Path) -> tuple[Path, Path]:
    source = Path(database)
    backup = source.with_name(source.name + ".pre-v33.sqlite3")
    return backup, backup.with_name(backup.name + ".sha256")


def _checked_backup(path: Path, logical: str) -> LiveOfficialExtractionBackupReceipt:
    digest_path = path.with_name(path.name + ".sha256")
    digest = _helpers._file_digest(path)
    if digest_path.read_text(encoding="ascii") != digest + "\n":
        raise LiveOfficialExtractionBackupError("backup digest differs")
    target = sqlite3.connect(f"file:{path}?mode=ro", uri=True, isolation_level=None)
    try:
        if (
            target.execute("PRAGMA user_version").fetchone()[0] != 32
            or _helpers._schema_fingerprint(target) != LIVE_OFFICIAL_EXTRACTION_PREDECESSOR_FINGERPRINT
            or _helpers._logical_database_digest(target) != logical
        ):
            raise LiveOfficialExtractionBackupError("backup differs from source")
    finally:
        target.close()
    return LiveOfficialExtractionBackupReceipt(path, digest_path, digest, logical)


def prepare_live_official_extraction_backup(
    connection: sqlite3.Connection, backup_path: Path
) -> LiveOfficialExtractionBackupReceipt:
    fingerprint = _helpers._schema_fingerprint
    logical_digest = _helpers._logical_database_digest
    file_digest = _helpers._file_digest
    if (
        connection.in_transaction
        or connection.execute("PRAGMA user_version").fetchone()[0] != 32
        or fingerprint(connection) != LIVE_OFFICIAL_EXTRACTION_PREDECESSOR_FINGERPRINT
        or not isinstance(backup_path, Path)
        or not backup_path.is_absolute()
    ):
        raise LiveOfficialExtractionBackupError("backup requires checked schema v32")
    source = Path(next(row[2] for row in connection.execute("PRAGMA database_list") if row[1] == "main"))
    digest_path = backup_path.with_name(backup_path.name + ".sha256")
    logical = logical_digest(connection)
    if not source or source.resolve() == backup_path.resolve() or backup_path.exists() != digest_path.exists():
        raise LiveOfficialExtractionBackupError("backup boundary differs")
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
        "CREATE TEMP TABLE IF NOT EXISTS live_official_extraction_backup_gate("
        "backup_path TEXT PRIMARY KEY,backup_digest TEXT NOT NULL,logical_digest TEXT NOT NULL) STRICT"
    )
    connection.execute("DELETE FROM live_official_extraction_backup_gate")
    connection.execute(
        "INSERT INTO live_official_extraction_backup_gate VALUES(?,?,?)",
        (str(backup_path), receipt.backup_digest, logical),
    )
    if connection.in_transaction:
        connection.commit()
    return receipt


def require_live_official_extraction_backup(
    connection: sqlite3.Connection,
    *,
    expected_history: tuple[tuple[int, str, str], ...],
) -> LiveOfficialExtractionBackupReceipt:
    logical_digest = _helpers._logical_database_digest
    try:
        row = connection.execute(
            "SELECT backup_path,backup_digest,logical_digest FROM temp.live_official_extraction_backup_gate"
        ).fetchone()
    except sqlite3.OperationalError as exc:
        raise LiveOfficialExtractionBackupError("v32 to v33 requires prepared backup") from exc
    if row is None or logical_digest(connection) != row[2]:
        raise LiveOfficialExtractionBackupError("v32 to v33 requires prepared backup")
    receipt = _checked_backup(Path(row[0]), str(row[2]))
    target = sqlite3.connect(f"file:{receipt.backup_path}?mode=ro", uri=True, isolation_level=None)
    try:
        history = target.execute(
            "SELECT version,name,checksum FROM authority_migrations ORDER BY version"
        ).fetchall()
    finally:
        target.close()
    if receipt.backup_digest != row[1] or history != list(expected_history):
        raise LiveOfficialExtractionBackupError("prepared backup is not exact v32")
    return receipt


_GRAPHITI_CONFIGURATION_CONTRACT_GUARD = next(
    statement
    for statement in GRAPHITI_ADAPTER_MIGRATION_STATEMENTS
    if "CREATE TRIGGER graphiti_configuration_contract_guard" in statement
)
LIVE_OFFICIAL_EXTRACTION_MIGRATION_STATEMENTS: tuple[str, ...] = (
    "PRAGMA defer_foreign_keys=ON",
    "DROP TRIGGER graphiti_configuration_contract_guard",
    """CREATE TABLE extractor_contracts_v33(
        contract_id TEXT PRIMARY KEY,
        framework_id TEXT NOT NULL,
        framework_version TEXT NOT NULL,
        framework_digest TEXT NOT NULL,
        model_id TEXT NOT NULL,
        model_version TEXT NOT NULL,
        model_digest TEXT NOT NULL,
        prompt_id TEXT NOT NULL,
        prompt_version TEXT NOT NULL,
        prompt_digest TEXT NOT NULL,
        output_schema_id TEXT NOT NULL,
        output_schema_version TEXT NOT NULL,
        output_schema_digest TEXT NOT NULL,
        code_id TEXT NOT NULL,
        code_version TEXT NOT NULL,
        code_digest TEXT NOT NULL,
        normalisation_id TEXT NOT NULL,
        normalisation_version TEXT NOT NULL,
        normalisation_digest TEXT NOT NULL,
        policy_id TEXT NOT NULL,
        policy_version TEXT NOT NULL,
        policy_digest TEXT NOT NULL,
        execution_profile TEXT NOT NULL
            CHECK(execution_profile IN('FIXTURE_REPLAY_ONLY','LIVE_OFFICIAL')),
        producer_kind TEXT NOT NULL
            CHECK(producer_kind IN('DETERMINISTIC_FIXTURE','DETERMINISTIC_LIVE_OFFICIAL')),
        semantic_digest TEXT NOT NULL UNIQUE,
        authority_event_id TEXT NOT NULL UNIQUE REFERENCES ledger_events(event_id),
        authority_aggregate_version INTEGER NOT NULL
            CHECK(authority_aggregate_version=1),
        canonical_bytes BLOB NOT NULL,
        canonical_digest TEXT NOT NULL,
        recorded_at TEXT NOT NULL,
        CHECK(length(canonical_bytes)>0)
    ) STRICT""",
    "INSERT INTO extractor_contracts_v33 SELECT * FROM extractor_contracts",
    "DROP TABLE extractor_contracts",
    "ALTER TABLE extractor_contracts_v33 RENAME TO extractor_contracts",
    """CREATE TRIGGER immutable_extractor_contract_update
        BEFORE UPDATE ON extractor_contracts BEGIN
        SELECT RAISE(ABORT,'immutable extractor contract'); END""",
    """CREATE TRIGGER immutable_extractor_contract_delete
        BEFORE DELETE ON extractor_contracts BEGIN
        SELECT RAISE(ABORT,'extractor contracts are retained'); END""",
    _GRAPHITI_CONFIGURATION_CONTRACT_GUARD,
)
LIVE_OFFICIAL_EXTRACTION_MIGRATION_CHECKSUM = digest_canonical({
    "version": LIVE_OFFICIAL_EXTRACTION_SCHEMA_VERSION,
    "name": LIVE_OFFICIAL_EXTRACTION_MIGRATION_NAME,
    "statements": list(LIVE_OFFICIAL_EXTRACTION_MIGRATION_STATEMENTS),
})
LIVE_OFFICIAL_EXTRACTION_MIGRATION = LiveOfficialExtractionMigrationRecord(
    LIVE_OFFICIAL_EXTRACTION_SCHEMA_VERSION,
    LIVE_OFFICIAL_EXTRACTION_MIGRATION_NAME,
    LIVE_OFFICIAL_EXTRACTION_MIGRATION_CHECKSUM,
)
__all__ = [name for name in globals() if name.startswith(("LIVE_OFFICIAL_EXTRACTION_", "LiveOfficialExtraction", "live_official_extraction_", "prepare_", "require_"))]
# fmt: on
