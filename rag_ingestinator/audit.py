"""Audit logging – structured per-session log files for compliance and review."""

from __future__ import annotations

import getpass
import json
import logging
import os
import platform
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rag_ingestinator.config import CONFIG_DIR
from rag_ingestinator.utils import format_size, format_duration

LOGS_DIR = CONFIG_DIR / "logs"

_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _ensure_logs_dir() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_local() -> str:
    return datetime.now().strftime(_DATE_FMT)


class AuditLogger:
    """Writes a structured log file for a single upload session.

    Log location: ``~/.rag_ingestinator/logs/<timestamp>_<session_id>.log``

    Each log entry is a single JSON line (JSONL format) for easy parsing,
    preceded by a human-readable header block.
    """

    def __init__(self, session_id: str, bucket: str, endpoint: str = "") -> None:
        _ensure_logs_dir()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_path = LOGS_DIR / f"{ts}_{session_id}.log"
        self.session_id = session_id
        self.bucket = bucket
        self.endpoint = endpoint
        self._logger = logging.getLogger(f"rag_ingestinator.audit.{session_id}")
        self._logger.setLevel(logging.DEBUG)
        self._logger.propagate = False

        handler = logging.FileHandler(self.log_path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(message)s"))
        self._logger.addHandler(handler)
        self._handler = handler

    def _write(self, entry: dict[str, Any]) -> None:
        entry.setdefault("timestamp", _now_iso())
        entry.setdefault("session_id", self.session_id)
        self._logger.info(json.dumps(entry, default=str))

    # ── Session lifecycle ────────────────────────────────────────────────

    def log_session_start(
        self,
        *,
        total_files: int,
        total_bytes: int,
        source_paths: list[str],
        prefix: str,
        concurrency: int,
        chunk_size_mb: int,
        dry_run: bool = False,
        resume: bool = False,
    ) -> None:
        self._write({
            "event": "session_start",
            "user": getpass.getuser(),
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "bucket": self.bucket,
            "endpoint": self.endpoint or "AWS (default)",
            "prefix": prefix,
            "source_paths": source_paths,
            "total_files": total_files,
            "total_bytes": total_bytes,
            "total_size_human": format_size(total_bytes),
            "concurrency": concurrency,
            "chunk_size_mb": chunk_size_mb,
            "dry_run": dry_run,
            "resume": resume,
        })

    def log_session_end(
        self,
        *,
        completed: int,
        failed: int,
        skipped: int,
        transferred_bytes: int,
        elapsed_seconds: float,
    ) -> None:
        avg_speed = transferred_bytes / elapsed_seconds if elapsed_seconds > 0 else 0
        self._write({
            "event": "session_end",
            "completed_files": completed,
            "failed_files": failed,
            "skipped_files": skipped,
            "transferred_bytes": transferred_bytes,
            "transferred_human": format_size(transferred_bytes),
            "elapsed_seconds": round(elapsed_seconds, 2),
            "elapsed_human": format_duration(elapsed_seconds),
            "avg_speed_bytes_sec": round(avg_speed, 0),
            "avg_speed_human": format_size(avg_speed) + "/s",
        })
        self._handler.close()

    # ── Per-file events ──────────────────────────────────────────────────

    def log_file_start(self, local_path: str, s3_key: str, size: int) -> None:
        self._write({
            "event": "file_start",
            "local_path": local_path,
            "s3_key": s3_key,
            "size": size,
            "size_human": format_size(size),
        })

    def log_file_done(
        self,
        local_path: str,
        s3_key: str,
        size: int,
        elapsed_seconds: float,
    ) -> None:
        speed = size / elapsed_seconds if elapsed_seconds > 0 else 0
        self._write({
            "event": "file_done",
            "status": "success",
            "local_path": local_path,
            "s3_key": s3_key,
            "size": size,
            "size_human": format_size(size),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "speed_bytes_sec": round(speed, 0),
            "speed_human": format_size(speed) + "/s",
        })

    def log_file_skipped(self, local_path: str, s3_key: str, reason: str) -> None:
        self._write({
            "event": "file_skipped",
            "local_path": local_path,
            "s3_key": s3_key,
            "reason": reason,
        })

    def log_file_error(self, local_path: str, s3_key: str, error: str) -> None:
        self._write({
            "event": "file_error",
            "status": "failed",
            "local_path": local_path,
            "s3_key": s3_key,
            "error": error,
        })

    # ── Other events ─────────────────────────────────────────────────────

    def log_benchmark(
        self,
        latency_ms: float,
        upload_mbps: float,
        est_seconds_opt: float,
        est_seconds_cons: float,
    ) -> None:
        self._write({
            "event": "benchmark",
            "latency_ms": round(latency_ms, 1),
            "upload_mbps": round(upload_mbps, 1),
            "est_time_optimistic": format_duration(est_seconds_opt),
            "est_time_conservative": format_duration(est_seconds_cons),
        })


def list_log_files() -> list[Path]:
    """Return all log files sorted newest first."""
    _ensure_logs_dir()
    return sorted(LOGS_DIR.glob("*.log"), reverse=True)


def read_log_entries(log_path: Path) -> list[dict[str, Any]]:
    """Parse a JSONL log file into a list of dicts."""
    entries: list[dict[str, Any]] = []
    for line in log_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries
