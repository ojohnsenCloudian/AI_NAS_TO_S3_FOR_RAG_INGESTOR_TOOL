"""Checkpoint / resume support – persist upload session state to JSON files."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

from rag_ingestinator.config import CHECKPOINTS_DIR, S3Config, get_s3_client


@dataclass
class FileCheckpoint:
    local_path: str
    s3_key: str
    size: int
    upload_id: str | None = None
    completed_parts: list[dict[str, Any]] = field(default_factory=list)
    done: bool = False


@dataclass
class SessionCheckpoint:
    session_id: str
    bucket: str
    prefix: str
    created_at: float
    files: dict[str, FileCheckpoint] = field(default_factory=dict)  # keyed by s3_key

    @property
    def total_files(self) -> int:
        return len(self.files)

    @property
    def completed_files(self) -> int:
        return sum(1 for f in self.files.values() if f.done)

    @property
    def total_bytes(self) -> int:
        return sum(f.size for f in self.files.values())

    @property
    def completed_bytes(self) -> int:
        total = 0
        for f in self.files.values():
            if f.done:
                total += f.size
            elif f.completed_parts:
                total += sum(p.get("Size", 0) for p in f.completed_parts)
        return total


class CheckpointManager:
    """Manages on-disk checkpoint files for upload sessions."""

    def __init__(self) -> None:
        CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)

    def _path(self, session_id: str) -> Path:
        return CHECKPOINTS_DIR / f"{session_id}.json"

    # ── Lifecycle ────────────────────────────────────────────────────────

    def create_session(self, bucket: str, prefix: str) -> SessionCheckpoint:
        session = SessionCheckpoint(
            session_id=uuid.uuid4().hex[:12],
            bucket=bucket,
            prefix=prefix,
            created_at=time.time(),
        )
        self._save(session)
        return session

    def _save(self, session: SessionCheckpoint) -> None:
        data = {
            "session_id": session.session_id,
            "bucket": session.bucket,
            "prefix": session.prefix,
            "created_at": session.created_at,
            "files": {
                k: {
                    "local_path": v.local_path,
                    "s3_key": v.s3_key,
                    "size": v.size,
                    "upload_id": v.upload_id,
                    "completed_parts": v.completed_parts,
                    "done": v.done,
                }
                for k, v in session.files.items()
            },
        }
        self._path(session.session_id).write_text(json.dumps(data, indent=2))

    def load_session(self, session_id: str) -> SessionCheckpoint | None:
        path = self._path(session_id)
        if not path.exists():
            return None
        data = json.loads(path.read_text())
        session = SessionCheckpoint(
            session_id=data["session_id"],
            bucket=data["bucket"],
            prefix=data["prefix"],
            created_at=data["created_at"],
        )
        for k, v in data.get("files", {}).items():
            session.files[k] = FileCheckpoint(**v)
        return session

    def delete_session(self, session_id: str) -> None:
        path = self._path(session_id)
        if path.exists():
            path.unlink()

    def list_sessions(self) -> list[SessionCheckpoint]:
        sessions = []
        for p in sorted(CHECKPOINTS_DIR.glob("*.json")):
            try:
                sessions.append(self.load_session(p.stem))  # type: ignore[arg-type]
            except Exception:
                continue
        return [s for s in sessions if s is not None]

    # ── Per-file updates ─────────────────────────────────────────────────

    def register_file(
        self,
        session: SessionCheckpoint,
        local_path: str,
        s3_key: str,
        size: int,
    ) -> None:
        session.files[s3_key] = FileCheckpoint(
            local_path=local_path,
            s3_key=s3_key,
            size=size,
        )
        self._save(session)

    def mark_file_done(
        self,
        session: SessionCheckpoint,
        s3_key: str,
        upload_id: str | None = None,
        parts: list[dict[str, Any]] | None = None,
    ) -> None:
        if s3_key in session.files:
            fc = session.files[s3_key]
            fc.done = True
            fc.upload_id = upload_id
            fc.completed_parts = parts or []
        self._save(session)

    def update_file_parts(
        self,
        session: SessionCheckpoint,
        s3_key: str,
        upload_id: str,
        parts: list[dict[str, Any]],
    ) -> None:
        if s3_key in session.files:
            fc = session.files[s3_key]
            fc.upload_id = upload_id
            fc.completed_parts = parts
        self._save(session)

    # ── Resume helpers ───────────────────────────────────────────────────

    def build_resume_info(
        self, session: SessionCheckpoint, cfg: S3Config
    ) -> dict[str, Any]:
        """Build the resume_info dict expected by S3Uploader.upload_batch.

        For each incomplete file with an upload_id, query S3 to get the
        actual completed parts (in case checkpoint is stale).
        """
        client = get_s3_client(cfg)
        info: dict[str, Any] = {}
        for key, fc in session.files.items():
            if fc.done:
                continue
            if not fc.upload_id:
                continue
            try:
                resp = client.list_parts(
                    Bucket=session.bucket,
                    Key=fc.s3_key,
                    UploadId=fc.upload_id,
                )
                parts = [
                    {"PartNumber": p["PartNumber"], "ETag": p["ETag"], "Size": p["Size"]}
                    for p in resp.get("Parts", [])
                ]
                info[key] = {"upload_id": fc.upload_id, "parts": parts}
            except Exception:
                pass
        return info

    def incomplete_files(self, session: SessionCheckpoint) -> list[FileCheckpoint]:
        return [fc for fc in session.files.values() if not fc.done]

    # ── Cleanup ──────────────────────────────────────────────────────────

    def abort_stale_uploads(self, session: SessionCheckpoint, cfg: S3Config) -> int:
        """Abort any in-progress multipart uploads for a session. Returns count aborted."""
        client = get_s3_client(cfg)
        aborted = 0
        for fc in session.files.values():
            if fc.done or not fc.upload_id:
                continue
            try:
                client.abort_multipart_upload(
                    Bucket=session.bucket,
                    Key=fc.s3_key,
                    UploadId=fc.upload_id,
                )
                aborted += 1
            except Exception:
                pass
        return aborted

    def find_latest_session(self) -> SessionCheckpoint | None:
        """Return the most recent session, if any."""
        sessions = self.list_sessions()
        if not sessions:
            return None
        return max(sessions, key=lambda s: s.created_at)
