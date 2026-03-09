"""Shared helpers: size formatting, time formatting, etc."""

from __future__ import annotations

import math


def format_size(size_bytes: int | float) -> str:
    """Human-readable file size (e.g. '4.2 GB')."""
    if size_bytes == 0:
        return "0 B"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    i = min(i, len(units) - 1)
    value = size_bytes / (1024**i)
    return f"{value:.1f} {units[i]}" if i > 0 else f"{int(value)} B"


def format_duration(seconds: float) -> str:
    """Human-readable duration (e.g. '1h 23m 04s')."""
    if seconds < 0:
        return "—"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h}h {m:02d}m {s:02d}s"
    if m > 0:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def format_speed(bytes_per_sec: float) -> str:
    """Human-readable transfer speed (e.g. '45.2 MB/s')."""
    return f"{format_size(bytes_per_sec)}/s"


def file_extension(name: str) -> str:
    """Return lowercased file extension including the dot, or '' if none."""
    dot = name.rfind(".")
    if dot == -1 or dot == len(name) - 1:
        return ""
    return name[dot:].lower()
