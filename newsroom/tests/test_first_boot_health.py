"""External first-boot health seam: process up, ledger present, ledger empty."""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

from newsroom.first_boot import SHARED_HOME, ledger_path, pid_path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "newsroom_first_boot.py"


def _cli(*args: str, home: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
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


def _install_uv_stub(tmp_path: Path) -> Path:
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
    return record


def _stop(home: Path) -> None:
    _cli("stop", home=home)


def _kill_hold_unclean(home: Path) -> None:
    report = json.loads(_cli("health", home=home).stdout)
    pid = report["pid"]
    assert isinstance(pid, int)
    os.kill(pid, signal.SIGKILL)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except OSError:
            return
        time.sleep(0.05)
    raise AssertionError(f"hold pid {pid} still alive after SIGKILL")


def _cmdline_parts(pid: int) -> list[str]:
    raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    return [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]


def test_shared_home_is_the_box_newsroom_path() -> None:
    assert SHARED_HOME == Path("/home/box/newsroom")
    assert ledger_path(SHARED_HOME) == Path("/home/box/newsroom/data/authority.sqlite3")


def test_health_is_red_before_bring_up(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    _install_uv_stub(tmp_path)
    result = _cli("health", home=home)
    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["ok"] is False
    assert report["process_up"] is False
    assert report["ledger_exists"] is False
    assert report["ledger_empty"] is False
    assert report["ledger_path"] == str(home / "data" / "authority.sqlite3")


def test_bring_up_leaves_process_up_and_empty_ledger(tmp_path: Path) -> None:
    home = tmp_path / "newsroom"
    record = _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        started = json.loads(start.stdout)
        assert started["ok"] is True
        assert started["uv_sync"] is True

        assert (record / "uv_args").read_text(encoding="utf-8") == "sync --locked"
        assert (record / "uv_project_environment").read_text(encoding="utf-8") == str(
            home / ".venv"
        )

        health = _cli("health", home=home)
        assert health.returncode == 0, health.stderr
        report = json.loads(health.stdout)
        assert report["ok"] is True
        assert report["process_up"] is True
        assert report["ledger_exists"] is True
        assert report["ledger_empty"] is True
        assert report["ledger_path"] == str(home / "data" / "authority.sqlite3")
        assert report["discovery_signal_count"] == 0
        assert report["ledger_event_count"] == 0

        db = Path(report["ledger_path"])
        assert db.is_file()
        with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            assert "discovery_signals" in tables
            assert "ledger_events" in tables
            assert conn.execute("SELECT COUNT(*) FROM discovery_signals").fetchone()[0] == 0
            assert conn.execute("SELECT COUNT(*) FROM ledger_events").fetchone()[0] == 0

        assert not (home / ".env").exists()
        assert not list(home.glob(".env*"))
    finally:
        _stop(home)


def test_health_reports_ledger_not_empty_when_a_discovery_signal_exists(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    data = home / "data"
    data.mkdir(parents=True)
    db = data / "authority.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE discovery_signals (signal_id TEXT)")
        conn.execute("CREATE TABLE ledger_events (event_id TEXT)")
        conn.execute("INSERT INTO discovery_signals(signal_id) VALUES ('signal-1')")
        conn.commit()
    _install_uv_stub(tmp_path)
    result = _cli("health", home=home)
    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["ledger_exists"] is True
    assert report["ledger_empty"] is False
    assert report["discovery_signal_count"] == 1
    assert report["ok"] is False


def test_health_is_red_when_pid_file_names_a_live_unrelated_process(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    data = home / "data"
    logs = home / "logs"
    data.mkdir(parents=True)
    logs.mkdir(parents=True)
    db = data / "authority.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE discovery_signals (signal_id TEXT)")
        conn.execute("CREATE TABLE ledger_events (event_id TEXT)")
        conn.commit()
    pid_path(home).write_text(f"{os.getpid()}\n", encoding="utf-8")
    _install_uv_stub(tmp_path)
    result = _cli("health", home=home)
    assert result.returncode != 0
    report = json.loads(result.stdout)
    assert report["process_up"] is False
    assert report["ledger_exists"] is True
    assert report["ledger_empty"] is True
    assert report["ok"] is False
    assert report["pid"] is None
    assert report["ledger_path"] == str(db)


def test_bring_up_restores_health_on_the_same_shared_ledger_after_process_death(
    tmp_path: Path,
) -> None:
    home = tmp_path / "newsroom"
    db = home / "data" / "authority.sqlite3"
    workspace_db = ROOT / "data" / "authority.sqlite3"
    workspace_db_existed = workspace_db.exists()
    _install_uv_stub(tmp_path)
    try:
        start = _cli("start", home=home)
        assert start.returncode == 0, start.stderr
        first = json.loads(_cli("health", home=home).stdout)
        assert first["ok"] is True
        identity = db.stat()

        _kill_hold_unclean(home)
        dead = json.loads(_cli("health", home=home).stdout)
        assert dead["ok"] is False
        assert dead["process_up"] is False
        assert dead["ledger_exists"] is True
        assert dead["ledger_path"] == str(db)
        assert db.is_file()

        pid_path(home).write_text(f"{os.getpid()}\n", encoding="utf-8")
        restore = _cli("start", home=home)
        assert restore.returncode == 0, restore.stderr
        restored = json.loads(restore.stdout)
        health = _cli("health", home=home)
        assert health.returncode == 0, health.stderr
        report = json.loads(health.stdout)
        assert report["ok"] is True
        assert report["process_up"] is True
        assert report["ledger_exists"] is True
        assert report["ledger_empty"] is True
        assert report["ledger_path"] == str(db)
        assert report["pid"] != os.getpid()
        assert restored["ledger_path"] == str(db)
        assert restored["pid"] == report["pid"]

        after = db.stat()
        assert (after.st_dev, after.st_ino) == (identity.st_dev, identity.st_ino)
        parts = _cmdline_parts(int(report["pid"]))
        assert "newsroom.first_boot" in parts
        assert "hold" in parts
        assert str(home) in parts
        if not workspace_db_existed:
            assert not workspace_db.exists()
        assert list(home.glob(".env*")) == []
    finally:
        _stop(home)
