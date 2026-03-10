"""Multipart S3 uploader with concurrent thread pool and progress tracking."""

from __future__ import annotations

import signal
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from pathlib import Path
from typing import Any, Callable

from rich.console import Console
from rich.progress import Progress, TaskID

from rag_ingestinator.config import S3Config, get_s3_client
from rag_ingestinator.scanner import FileEntry
from rag_ingestinator.progress import (
    UploadStats,
    SpeedTracker,
    create_upload_progress,
    render_summary,
)
from rag_ingestinator.audit import AuditLogger

ProgressCallback = Callable[[int], None]  # bytes_delta


class S3Uploader:
    """Handles uploading files to S3 with multipart support."""

    def __init__(
        self,
        cfg: S3Config,
        console: Console,
        *,
        bucket: str | None = None,
        concurrency: int | None = None,
        chunk_size: int | None = None,
        audit: AuditLogger | None = None,
    ) -> None:
        self.cfg = cfg
        self.console = console
        self.bucket = bucket or cfg.bucket
        self.concurrency = concurrency or cfg.concurrency
        self.chunk_size = chunk_size or cfg.chunk_size_bytes
        self.client = get_s3_client(cfg)
        self.audit = audit
        self._lock = threading.Lock()
        self._cancelled = threading.Event()

    # ── Single-file upload ───────────────────────────────────────────────

    def _upload_small(
        self,
        entry: FileEntry,
        s3_key: str,
        *,
        on_progress: ProgressCallback | None = None,
    ) -> None:
        """Upload a file that fits in a single PUT."""
        if self._cancelled.is_set():
            raise InterruptedError("Upload cancelled")
        data = entry.local_path.read_bytes()
        self.client.put_object(Bucket=self.bucket, Key=s3_key, Body=data)
        if on_progress:
            on_progress(len(data))

    def _upload_multipart(
        self,
        entry: FileEntry,
        s3_key: str,
        *,
        on_progress: ProgressCallback | None = None,
        completed_parts_hint: list[dict[str, Any]] | None = None,
        upload_id: str | None = None,
    ) -> tuple[str, list[dict[str, Any]]]:
        """Upload a file using multipart upload.

        If ``upload_id`` and ``completed_parts_hint`` are provided we resume
        from the last completed part.

        Returns (upload_id, parts_list) for checkpoint tracking.
        """
        chunk = self.chunk_size
        file_size = entry.size
        total_parts = (file_size + chunk - 1) // chunk

        if upload_id is None:
            resp = self.client.create_multipart_upload(Bucket=self.bucket, Key=s3_key)
            upload_id = resp["UploadId"]

        completed: dict[int, dict[str, Any]] = {}
        if completed_parts_hint:
            for p in completed_parts_hint:
                completed[p["PartNumber"]] = p
                if on_progress:
                    on_progress(min(chunk, file_size - (p["PartNumber"] - 1) * chunk))

        try:
            with open(entry.local_path, "rb") as fh:
                for part_num in range(1, total_parts + 1):
                    if self._cancelled.is_set():
                        raise InterruptedError("Upload cancelled")
                    if part_num in completed:
                        continue
                    offset = (part_num - 1) * chunk
                    fh.seek(offset)
                    data = fh.read(chunk)
                    resp = self.client.upload_part(
                        Bucket=self.bucket,
                        Key=s3_key,
                        UploadId=upload_id,
                        PartNumber=part_num,
                        Body=data,
                    )
                    completed[part_num] = {
                        "PartNumber": part_num,
                        "ETag": resp["ETag"],
                    }
                    if on_progress:
                        on_progress(len(data))
        except InterruptedError:
            raise
        except Exception:
            raise

        parts = [completed[i] for i in sorted(completed)]
        self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        return upload_id, parts

    def upload_file(
        self,
        entry: FileEntry,
        s3_key: str,
        *,
        on_progress: ProgressCallback | None = None,
        resume_upload_id: str | None = None,
        resume_parts: list[dict[str, Any]] | None = None,
    ) -> tuple[str | None, list[dict[str, Any]] | None]:
        """Upload a single file, choosing single-part or multipart automatically.

        Returns (upload_id, parts) for multipart, or (None, None) for small files.
        """
        if entry.size < self.chunk_size and not resume_upload_id:
            self._upload_small(entry, s3_key, on_progress=on_progress)
            return None, None
        return self._upload_multipart(
            entry,
            s3_key,
            on_progress=on_progress,
            upload_id=resume_upload_id,
            completed_parts_hint=resume_parts,
        )

    # ── Batch upload with progress ───────────────────────────────────────

    def upload_batch(
        self,
        files: list[FileEntry],
        *,
        skip_existing: bool = False,
        checkpoint_cb: Callable[[FileEntry, str, str | None, list | None], None] | None = None,
        resume_info: dict[str, Any] | None = None,
    ) -> UploadStats:
        """Upload a list of files with concurrent threads and Rich progress.

        ``checkpoint_cb(entry, s3_key, upload_id, parts)`` is called after
        each file completes to allow the checkpoint module to persist state.

        ``resume_info`` maps relative_key -> {"upload_id": ..., "parts": [...]}.
        """
        stats = UploadStats(
            total_files=len(files),
            total_bytes=sum(f.size for f in files),
            start_time=time.monotonic(),
            speed_tracker=SpeedTracker(),
        )

        existing_keys: set[str] | None = None
        if skip_existing:
            self.console.print("[dim]Checking for existing files in S3…[/dim]")
            existing_keys = self._list_existing_keys(files)
            if existing_keys:
                self.console.print(f"[dim]  {len(existing_keys)} file(s) already exist, will be skipped.[/dim]")

        resume = resume_info or {}

        # Handle Ctrl+C gracefully
        original_sigint = signal.getsignal(signal.SIGINT)

        def _handle_interrupt(signum, frame):
            self._cancelled.set()
            self.console.print("\n[yellow]Interrupt received – finishing current uploads…[/yellow]")

        signal.signal(signal.SIGINT, _handle_interrupt)

        progress = create_upload_progress(self.console)
        overall_task = progress.add_task(
            "overall",
            filename="Overall",
            total=stats.total_bytes,
        )

        file_start_times: dict[str, float] = {}

        try:
            with progress:
                with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
                    futures: dict[Future, FileEntry] = {}
                    for entry in files:
                        if self._cancelled.is_set():
                            break

                        s3_key = entry.relative_key

                        if existing_keys is not None and s3_key in existing_keys:
                            stats.skipped_files += 1
                            progress.update(overall_task, advance=entry.size)
                            if self.audit:
                                self.audit.log_file_skipped(str(entry.local_path), s3_key, "already exists in S3")
                            continue

                        file_task = progress.add_task(
                            f"file_{s3_key}",
                            filename=entry.local_path.name,
                            total=entry.size,
                        )

                        r_info = resume.get(entry.relative_key, {})

                        if self.audit:
                            self.audit.log_file_start(str(entry.local_path), s3_key, entry.size)
                        file_start_times[s3_key] = time.monotonic()

                        def _do_upload(
                            _entry=entry,
                            _key=s3_key,
                            _ftask=file_task,
                            _r=r_info,
                        ):
                            def _on_progress(delta: int) -> None:
                                progress.update(_ftask, advance=delta)
                                progress.update(overall_task, advance=delta)
                                stats.speed_tracker.record(delta)
                                with self._lock:
                                    stats.transferred_bytes += delta

                            uid, parts = self.upload_file(
                                _entry,
                                _key,
                                on_progress=_on_progress,
                                resume_upload_id=_r.get("upload_id"),
                                resume_parts=_r.get("parts"),
                            )
                            return _entry, _key, uid, parts

                        future = pool.submit(_do_upload)
                        futures[future] = entry

                    for future in as_completed(futures):
                        entry = futures[future]
                        try:
                            result_entry, s3_key, uid, parts = future.result()
                            file_elapsed = time.monotonic() - file_start_times.get(s3_key, time.monotonic())
                            with self._lock:
                                stats.completed_files += 1
                            if self.audit:
                                self.audit.log_file_done(str(result_entry.local_path), s3_key, result_entry.size, file_elapsed)
                            if checkpoint_cb:
                                checkpoint_cb(result_entry, s3_key, uid, parts)
                        except InterruptedError:
                            with self._lock:
                                stats.failed_files += 1
                                stats.errors.append((str(entry.local_path), "Cancelled by user"))
                            if self.audit:
                                self.audit.log_file_error(str(entry.local_path), entry.relative_key, "Cancelled by user")
                        except Exception as exc:
                            with self._lock:
                                stats.failed_files += 1
                                stats.errors.append((str(entry.local_path), str(exc)))
                            if self.audit:
                                self.audit.log_file_error(str(entry.local_path), entry.relative_key, str(exc))
        finally:
            signal.signal(signal.SIGINT, original_sigint)

        stats.end_time = time.monotonic()
        render_summary(stats, self.console)
        return stats

    # ── Helpers ──────────────────────────────────────────────────────────

    def _list_existing_keys(self, files: list[FileEntry]) -> set[str]:
        """List which of the given file keys already exist in S3 (key+size match)."""
        expected = {f.relative_key: f.size for f in files}
        existing: set[str] = set()
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key in expected and obj["Size"] == expected[key]:
                    existing.add(key)
        return existing

    def key_exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False
