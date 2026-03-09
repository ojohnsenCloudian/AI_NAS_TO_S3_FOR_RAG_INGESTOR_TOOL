"""Network benchmark – measure latency and throughput to the target S3 bucket."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

from rag_ingestinator.config import S3Config, get_s3_client
from rag_ingestinator.utils import format_size, format_speed, format_duration


BENCHMARK_SIZES_MB = [1, 4]  # upload test objects of these sizes
LATENCY_SAMPLES = 5


@dataclass
class BenchmarkResult:
    latency_ms: float  # average HEAD request latency
    upload_mbps: float  # measured upload throughput in megabits/sec
    upload_bytes_per_sec: float  # measured upload throughput in bytes/sec
    region: str

    def estimate_seconds(self, total_bytes: int, concurrency: int = 1) -> float:
        """Estimate upload time for a given total size.

        We apply a conservative factor of 0.75 to account for overhead
        (multipart initiation, completion, retries, small-file penalty).
        """
        if self.upload_bytes_per_sec <= 0:
            return float("inf")
        raw = total_bytes / self.upload_bytes_per_sec
        return raw / (0.75 * min(concurrency, 4))

    def estimate_range_seconds(self, total_bytes: int, concurrency: int = 1) -> tuple[float, float]:
        """Return (optimistic, conservative) time estimates."""
        if self.upload_bytes_per_sec <= 0:
            return (float("inf"), float("inf"))
        raw = total_bytes / self.upload_bytes_per_sec
        effective = min(concurrency, 4)
        optimistic = raw / effective
        conservative = raw / (0.7 * effective)
        return optimistic, conservative


def run_benchmark(cfg: S3Config, console=None) -> BenchmarkResult:
    """Run latency and throughput tests against the configured S3 bucket.

    Uploads small temporary objects and immediately deletes them.
    """
    client = get_s3_client(cfg)
    bucket = cfg.bucket

    if console:
        console.print("[bold]Running network benchmark…[/bold]\n")

    # ── Latency ──────────────────────────────────────────────────────────
    latencies: list[float] = []
    for i in range(LATENCY_SAMPLES):
        if console:
            console.print(f"  Latency probe {i + 1}/{LATENCY_SAMPLES}…", end="\r")
        t0 = time.perf_counter()
        try:
            client.head_bucket(Bucket=bucket)
        except Exception:
            pass
        latencies.append((time.perf_counter() - t0) * 1000)

    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
    if console:
        console.print(f"  Latency:  [cyan]{avg_latency:.0f} ms[/cyan] avg")

    # ── Throughput ───────────────────────────────────────────────────────
    speeds: list[float] = []  # bytes/sec per test
    for size_mb in BENCHMARK_SIZES_MB:
        key = f"__rag_ingestinator_benchmark_{uuid.uuid4().hex}"
        payload = b"\x00" * (size_mb * 1024 * 1024)

        if console:
            console.print(f"  Uploading {size_mb} MB test object…", end="\r")

        t0 = time.perf_counter()
        client.put_object(Bucket=bucket, Key=key, Body=payload)
        elapsed = time.perf_counter() - t0

        if elapsed > 0:
            speeds.append(len(payload) / elapsed)

        # Clean up immediately
        try:
            client.delete_object(Bucket=bucket, Key=key)
        except Exception:
            pass

    avg_speed = sum(speeds) / len(speeds) if speeds else 0.0
    upload_mbps = (avg_speed * 8) / (1024 * 1024)

    if console:
        console.print(f"  Throughput: [cyan]{format_speed(avg_speed)}[/cyan] ({upload_mbps:.1f} Mbps)")

    return BenchmarkResult(
        latency_ms=avg_latency,
        upload_mbps=upload_mbps,
        upload_bytes_per_sec=avg_speed,
        region=cfg.region,
    )
