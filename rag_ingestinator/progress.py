"""Rich progress display with per-file and overall ETA / speed tracking."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from rich.console import Console
from rich.progress import (
    Progress,
    BarColumn,
    DownloadColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
    TaskID,
    SpinnerColumn,
)
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from rag_ingestinator.utils import format_size, format_speed, format_duration

ROLLING_WINDOW_SEC = 5.0


@dataclass
class SpeedTracker:
    """Calculates rolling-window average speed to keep ETA stable."""

    _samples: deque[tuple[float, int]] = field(default_factory=deque)
    _total_bytes: int = 0
    _start_time: float = field(default_factory=time.monotonic)
    _peak: float = 0.0

    def record(self, bytes_delta: int) -> None:
        now = time.monotonic()
        self._samples.append((now, bytes_delta))
        self._total_bytes += bytes_delta
        cutoff = now - ROLLING_WINDOW_SEC
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()
        speed = self.rolling_speed
        if speed > self._peak:
            self._peak = speed

    @property
    def rolling_speed(self) -> float:
        """Bytes per second over the rolling window."""
        if len(self._samples) < 2:
            return self.overall_speed
        window = self._samples[-1][0] - self._samples[0][0]
        if window <= 0:
            return 0.0
        total = sum(b for _, b in self._samples)
        return total / window

    @property
    def overall_speed(self) -> float:
        elapsed = time.monotonic() - self._start_time
        return self._total_bytes / elapsed if elapsed > 0 else 0.0

    @property
    def peak_speed(self) -> float:
        return self._peak

    @property
    def total_bytes(self) -> int:
        return self._total_bytes


@dataclass
class UploadStats:
    """Aggregated stats updated during upload, used for the summary."""

    total_files: int = 0
    completed_files: int = 0
    failed_files: int = 0
    skipped_files: int = 0
    total_bytes: int = 0
    transferred_bytes: int = 0
    start_time: float = 0.0
    end_time: float = 0.0
    errors: list[tuple[str, str]] = field(default_factory=list)
    speed_tracker: SpeedTracker = field(default_factory=SpeedTracker)

    @property
    def elapsed(self) -> float:
        end = self.end_time or time.monotonic()
        return end - self.start_time if self.start_time else 0.0


def create_upload_progress(console: Console) -> Progress:
    """Build a Rich Progress instance styled for uploads."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.0f}%",
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
        expand=True,
    )


def render_summary(stats: UploadStats, console: Console) -> None:
    """Print a final summary panel after uploads complete."""
    elapsed = stats.elapsed
    avg_speed = stats.transferred_bytes / elapsed if elapsed > 0 else 0.0
    peak = stats.speed_tracker.peak_speed

    rows: list[str] = [
        f"[bold]Files:[/bold]       {stats.completed_files} uploaded, {stats.failed_files} failed, {stats.skipped_files} skipped / {stats.total_files} total",
        f"[bold]Transferred:[/bold] {format_size(stats.transferred_bytes)} / {format_size(stats.total_bytes)}",
        f"[bold]Time:[/bold]        {format_duration(elapsed)}",
        f"[bold]Avg speed:[/bold]   {format_speed(avg_speed)}",
        f"[bold]Peak speed:[/bold]  {format_speed(peak)}",
    ]

    if stats.errors:
        rows.append("")
        rows.append(f"[bold red]Errors ({len(stats.errors)}):[/bold red]")
        for path, err in stats.errors[:20]:
            rows.append(f"  [red]✗[/red] {path}: {err}")
        if len(stats.errors) > 20:
            rows.append(f"  … and {len(stats.errors) - 20} more")

    color = "green" if stats.failed_files == 0 else "yellow"
    title = "Upload Complete" if stats.failed_files == 0 else "Upload Complete (with errors)"
    console.print(Panel("\n".join(rows), title=title, border_style=color, expand=False))
