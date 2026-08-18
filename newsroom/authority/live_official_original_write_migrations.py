"""Checked v36 live-official original 粵語 write migration and v35 backup gate."""

# fmt: off
# ruff: noqa: I001
from __future__ import annotations
import sqlite3
from pathlib import Path
from . import live_official_evidence_package_migrations as predecessor
from .canonical import digest_canonical

LIVE_OFFICIAL_ORIGINAL_WRITE_SCHEMA_VERSION = 36
LIVE_OFFICIAL_ORIGINAL_WRITE_MIGRATION_NAME = "live_official_original_write_authority_v36"
LIVE_OFFICIAL_ORIGINAL_WRITE_PREDECESSOR_MIGRATION_CHECKSUM = (
    "sha256:7ad509f00db8fd86f53d97dce7986014488971d23810174d10cb81be63bef238"
)
LIVE_OFFICIAL_ORIGINAL_WRITE_PREDECESSOR_FINGERPRINT = (
    "sha256:45fe0333359ff4792d20a6fe68713c38a135aaaa2132bb47cb9a1d8d550cefdb"
)
LiveOfficialOriginalWriteBackupError = predecessor.LiveOfficialEvidencePackageBackupError
LiveOfficialOriginalWriteBackupReceipt = predecessor.LiveOfficialEvidencePackageBackupReceipt
LiveOfficialOriginalWriteMigrationRecord = predecessor.LiveOfficialEvidencePackageMigrationRecord
_helpers = predecessor._helpers


def live_official_original_write_backup_paths(database: str | Path) -> tuple[Path, Path]:
    source = Path(database)
    backup = source.with_name(source.name + ".pre-v36.sqlite3")
    return backup, backup.with_name(backup.name + ".sha256")


def _checked_backup(path: Path, logical: str) -> LiveOfficialOriginalWriteBackupReceipt:
    digest_path = path.with_name(path.name + ".sha256")
    digest = _helpers._file_digest(path)
    if digest_path.read_text(encoding="ascii") != digest + "\n":
        raise LiveOfficialOriginalWriteBackupError("backup digest differs")
    target = sqlite3.connect(f"file:{path}?mode=ro", uri=True, isolation_level=None)
    try:
        if (
            target.execute("PRAGMA user_version").fetchone()[0] != 35
            or _helpers._schema_fingerprint(target) != LIVE_OFFICIAL_ORIGINAL_WRITE_PREDECESSOR_FINGERPRINT
            or _helpers._logical_database_digest(target) != logical
        ):
            raise LiveOfficialOriginalWriteBackupError("backup differs from source")
    finally:
        target.close()
    return LiveOfficialOriginalWriteBackupReceipt(path, digest_path, digest, logical)


def prepare_live_official_original_write_backup(
    connection: sqlite3.Connection, backup_path: Path
) -> LiveOfficialOriginalWriteBackupReceipt:
    fingerprint = _helpers._schema_fingerprint
    logical_digest = _helpers._logical_database_digest
    file_digest = _helpers._file_digest
    if (
        connection.in_transaction
        or connection.execute("PRAGMA user_version").fetchone()[0] != 35
        or fingerprint(connection) != LIVE_OFFICIAL_ORIGINAL_WRITE_PREDECESSOR_FINGERPRINT
        or not isinstance(backup_path, Path)
        or not backup_path.is_absolute()
    ):
        raise LiveOfficialOriginalWriteBackupError("backup requires checked schema v35")
    source = Path(next(row[2] for row in connection.execute("PRAGMA database_list") if row[1] == "main"))
    digest_path = backup_path.with_name(backup_path.name + ".sha256")
    logical = logical_digest(connection)
    if not source or source.resolve() == backup_path.resolve() or backup_path.exists() != digest_path.exists():
        raise LiveOfficialOriginalWriteBackupError("backup boundary differs")
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
        "CREATE TEMP TABLE IF NOT EXISTS live_official_original_write_backup_gate("
        "backup_path TEXT PRIMARY KEY,backup_digest TEXT NOT NULL,logical_digest TEXT NOT NULL) STRICT"
    )
    connection.execute("DELETE FROM live_official_original_write_backup_gate")
    connection.execute(
        "INSERT INTO live_official_original_write_backup_gate VALUES(?,?,?)",
        (str(backup_path), receipt.backup_digest, logical),
    )
    if connection.in_transaction:
        connection.commit()
    return receipt


def require_live_official_original_write_backup(
    connection: sqlite3.Connection,
    *,
    expected_history: tuple[tuple[int, str, str], ...],
) -> LiveOfficialOriginalWriteBackupReceipt:
    logical_digest = _helpers._logical_database_digest
    try:
        row = connection.execute(
            "SELECT backup_path,backup_digest,logical_digest FROM temp.live_official_original_write_backup_gate"
        ).fetchone()
    except sqlite3.OperationalError as exc:
        raise LiveOfficialOriginalWriteBackupError("v35 to v36 requires prepared backup") from exc
    if row is None or logical_digest(connection) != row[2]:
        raise LiveOfficialOriginalWriteBackupError("v35 to v36 requires prepared backup")
    receipt = _checked_backup(Path(row[0]), str(row[2]))
    target = sqlite3.connect(f"file:{receipt.backup_path}?mode=ro", uri=True, isolation_level=None)
    try:
        history = target.execute(
            "SELECT version,name,checksum FROM authority_migrations ORDER BY version"
        ).fetchall()
    finally:
        target.close()
    if receipt.backup_digest != row[1] or history != list(expected_history):
        raise LiveOfficialOriginalWriteBackupError("prepared backup is not exact v35")
    return receipt


_D = "substr({0},1,7)='sha256:' AND length({0})=71 AND substr({0},8) NOT GLOB '*[^0-9a-f]*'"
_U = "length({0})=36 AND substr({0},15,1)='4' AND lower(substr({0},20,1)) IN ('8','9','a','b')"
LIVE_OFFICIAL_ORIGINAL_WRITE_MIGRATION_STATEMENTS: tuple[str, ...] = (
    f"""CREATE TABLE original_writes(
write_id TEXT PRIMARY KEY CHECK({_U.format("write_id")}),
package_id TEXT NOT NULL UNIQUE CHECK({_U.format("package_id")}),
semantic_digest TEXT NOT NULL UNIQUE CHECK({_D.format("semantic_digest")}),
created_at TEXT NOT NULL) STRICT""",
    f"""CREATE TABLE original_write_receipts(
receipt_digest TEXT PRIMARY KEY CHECK({_D.format("receipt_digest")}),
write_id TEXT NOT NULL UNIQUE REFERENCES original_writes(write_id),
package_id TEXT NOT NULL UNIQUE CHECK({_U.format("package_id")}),
write_bytes BLOB NOT NULL,
recorded_at TEXT NOT NULL) STRICT""",
    f"""CREATE TABLE original_write_heads(
write_id TEXT PRIMARY KEY REFERENCES original_writes(write_id),
package_id TEXT NOT NULL UNIQUE CHECK({_U.format("package_id")}),
write_bytes BLOB NOT NULL,
semantic_digest TEXT NOT NULL UNIQUE CHECK({_D.format("semantic_digest")}),
updated_at TEXT NOT NULL) STRICT""",
    """CREATE TRIGGER immutable_original_write
BEFORE UPDATE ON original_writes
BEGIN SELECT RAISE(ABORT,'immutable original write'); END""",
    """CREATE TRIGGER retained_original_write
BEFORE DELETE ON original_writes
BEGIN SELECT RAISE(ABORT,'retained original write'); END""",
    """CREATE TRIGGER immutable_original_write_receipt
BEFORE UPDATE ON original_write_receipts
BEGIN SELECT RAISE(ABORT,'immutable original write receipt'); END""",
    """CREATE TRIGGER retained_original_write_receipt
BEFORE DELETE ON original_write_receipts
BEGIN SELECT RAISE(ABORT,'retained original write receipt'); END""",
    """CREATE TRIGGER retained_original_write_head
BEFORE DELETE ON original_write_heads
BEGIN SELECT RAISE(ABORT,'retained original write head'); END""",
)
LIVE_OFFICIAL_ORIGINAL_WRITE_MIGRATION_CHECKSUM = digest_canonical({
    "version": LIVE_OFFICIAL_ORIGINAL_WRITE_SCHEMA_VERSION,
    "name": LIVE_OFFICIAL_ORIGINAL_WRITE_MIGRATION_NAME,
    "statements": list(LIVE_OFFICIAL_ORIGINAL_WRITE_MIGRATION_STATEMENTS),
})
LIVE_OFFICIAL_ORIGINAL_WRITE_MIGRATION = LiveOfficialOriginalWriteMigrationRecord(
    LIVE_OFFICIAL_ORIGINAL_WRITE_SCHEMA_VERSION,
    LIVE_OFFICIAL_ORIGINAL_WRITE_MIGRATION_NAME,
    LIVE_OFFICIAL_ORIGINAL_WRITE_MIGRATION_CHECKSUM,
)
__all__ = [name for name in globals() if name.startswith(("LIVE_OFFICIAL_ORIGINAL_WRITE_", "LiveOfficialOriginalWrite", "live_official_original_write_", "prepare_", "require_"))]
# fmt: on
