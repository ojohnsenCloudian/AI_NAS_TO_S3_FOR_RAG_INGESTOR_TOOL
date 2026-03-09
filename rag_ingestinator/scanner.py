"""File discovery – walk directories, apply include/exclude globs, gather metadata."""

from __future__ import annotations

import fnmatch
import os
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from rag_ingestinator.utils import file_extension, format_size


@dataclass
class FileEntry:
    """A single file to be uploaded."""

    local_path: Path
    relative_key: str  # S3 key (relative portion)
    size: int

    @property
    def extension(self) -> str:
        return file_extension(self.local_path.name)


@dataclass
class ScanResult:
    """Aggregated results of scanning one or more source paths."""

    files: list[FileEntry] = field(default_factory=list)
    total_size: int = 0
    skipped_dirs: int = 0
    skipped_files: int = 0

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def largest_file(self) -> FileEntry | None:
        return max(self.files, key=lambda f: f.size) if self.files else None

    def extension_breakdown(self) -> dict[str, tuple[int, int]]:
        """Return {ext: (count, total_bytes)} sorted by total bytes descending."""
        counts: Counter[str] = Counter()
        sizes: Counter[str] = Counter()
        for f in self.files:
            ext = f.extension or "(no ext)"
            counts[ext] += 1
            sizes[ext] += f.size
        return {
            ext: (counts[ext], sizes[ext])
            for ext in sorted(sizes, key=sizes.get, reverse=True)  # type: ignore[arg-type]
        }


def _matches_any(name: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(name, p) for p in patterns)


def scan_paths(
    paths: list[Path],
    *,
    recursive: bool = True,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    prefix: str = "",
) -> ScanResult:
    """
    Scan one or more filesystem paths and return matching files.

    For each input path:
    - If it's a file, include it directly (subject to include/exclude).
    - If it's a directory and recursive=True, walk it recursively.
    - If it's a directory and recursive=False, include only immediate files.

    The ``relative_key`` for each file is built from the relative path within
    the scanned directory (or just the filename for single files), prefixed
    with ``prefix``.
    """
    result = ScanResult()
    include = include or []
    exclude = exclude or []

    for source in paths:
        source = source.resolve()

        if source.is_file():
            name = source.name
            if include and not _matches_any(name, include):
                result.skipped_files += 1
                continue
            if exclude and _matches_any(name, exclude):
                result.skipped_files += 1
                continue
            key = f"{prefix}/{name}".lstrip("/") if prefix else name
            size = source.stat().st_size
            result.files.append(FileEntry(local_path=source, relative_key=key, size=size))
            result.total_size += size
            continue

        if not source.is_dir():
            continue

        base = source
        walker = os.walk(source) if recursive else [(str(source), [], os.listdir(source))]

        for dirpath_str, dirnames, filenames in walker:
            dirpath = Path(dirpath_str)
            for fname in filenames:
                fpath = dirpath / fname
                if not fpath.is_file():
                    continue
                if include and not _matches_any(fname, include):
                    result.skipped_files += 1
                    continue
                if exclude and _matches_any(fname, exclude):
                    result.skipped_files += 1
                    continue
                try:
                    size = fpath.stat().st_size
                except OSError:
                    result.skipped_files += 1
                    continue
                rel = fpath.relative_to(base)
                key = f"{prefix}/{rel}".lstrip("/") if prefix else str(rel)
                # Normalise to forward slashes for S3 keys
                key = key.replace(os.sep, "/")
                result.files.append(FileEntry(local_path=fpath, relative_key=key, size=size))
                result.total_size += size

    # Sort by relative key for predictable ordering
    result.files.sort(key=lambda f: f.relative_key)
    return result
