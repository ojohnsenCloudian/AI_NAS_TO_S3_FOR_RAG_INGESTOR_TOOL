"""CLI entry point for RAG Ingestinator."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, Confirm
from rich.table import Table

from rag_ingestinator import __version__
from rag_ingestinator.config import (
    S3Config,
    load_config,
    save_config,
    validate_s3_connection,
    get_s3_client,
    DEFAULT_CHUNK_SIZE_MB,
    DEFAULT_CONCURRENCY,
)
from rag_ingestinator.scanner import scan_paths, ScanResult
from rag_ingestinator.benchmark import run_benchmark
from rag_ingestinator.uploader import S3Uploader
from rag_ingestinator.checkpoint import CheckpointManager
from rag_ingestinator.audit import AuditLogger, list_log_files, read_log_entries, LOGS_DIR
from rag_ingestinator.utils import format_size, format_duration, format_speed

console = Console()
err_console = Console(stderr=True)

app = typer.Typer(
    name="rag-ingestinator",
    help="Ingest files from local, NAS (NFS/SMB/CIFS), or GPFS filesystems into S3-compatible storage.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)


def _require_config() -> S3Config:
    cfg = load_config()
    if not cfg.is_configured():
        err_console.print("[red]Not configured.[/red] Run [bold]rag-ingestinator configure[/bold] first.")
        raise typer.Exit(1)
    return cfg


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"rag-ingestinator {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False, "--version", "-V", help="Show version and exit.", callback=_version_callback, is_eager=True
    ),
) -> None:
    """Ingest files from local, NAS (NFS/SMB/CIFS), or GPFS filesystems into S3-compatible storage."""


# ── configure ────────────────────────────────────────────────────────────────


@app.command()
def configure() -> None:
    """Interactive wizard to set up S3 credentials and defaults."""
    existing = load_config()
    console.print(Panel("[bold]RAG Ingestinator Configuration Wizard[/bold]", expand=False))

    def _ask(label: str, default: str, password: bool = False) -> str:
        return Prompt.ask(label, default=default or None, password=password) or ""

    endpoint_url = _ask(
        "S3 endpoint URL (leave empty for AWS, or enter custom e.g. https://s3.example.com)",
        existing.endpoint_url,
    )
    verify_ssl = True
    if endpoint_url:
        verify_ssl = Confirm.ask("Verify SSL certificate?", default=existing.verify_ssl)

    cfg = S3Config(
        aws_access_key_id=_ask("Access Key ID", existing.aws_access_key_id),
        aws_secret_access_key=_ask("Secret Access Key", existing.aws_secret_access_key, password=True),
        region=_ask("Region", existing.region),
        endpoint_url=endpoint_url,
        verify_ssl=verify_ssl,
        bucket=_ask("Default bucket name", existing.bucket),
        prefix=_ask("Default S3 key prefix (optional)", existing.prefix),
        chunk_size_mb=IntPrompt.ask("Multipart chunk size (MB)", default=existing.chunk_size_mb or DEFAULT_CHUNK_SIZE_MB),
        concurrency=IntPrompt.ask("Max concurrent uploads", default=existing.concurrency or DEFAULT_CONCURRENCY),
    )

    console.print("\n[bold]Testing S3 connectivity…[/bold]")
    ok, msg = validate_s3_connection(cfg)
    if ok:
        console.print(f"[green]✓[/green] {msg}")
    else:
        console.print(f"[red]✗[/red] {msg}")
        if not Confirm.ask("Save configuration anyway?", default=False):
            raise typer.Abort()

    save_config(cfg)
    console.print("[green]Configuration saved.[/green]")


# ── upload ───────────────────────────────────────────────────────────────────


@app.command()
def upload(
    paths: Annotated[list[Path], typer.Argument(help="File(s) or directory(ies) to upload.")],
    bucket: Annotated[Optional[str], typer.Option("-b", "--bucket", help="Override default S3 bucket.")] = None,
    prefix: Annotated[Optional[str], typer.Option("-p", "--prefix", help="S3 key prefix.")] = None,
    recursive: Annotated[bool, typer.Option("-r", "--recursive", help="Recurse into directories.")] = False,
    include: Annotated[Optional[list[str]], typer.Option("--include", help="Glob include patterns.")] = None,
    exclude: Annotated[Optional[list[str]], typer.Option("--exclude", help="Glob exclude patterns.")] = None,
    chunk_size: Annotated[Optional[int], typer.Option("--chunk-size", help="Multipart chunk size in MB.")] = None,
    concurrency: Annotated[Optional[int], typer.Option("--concurrency", help="Max concurrent uploads.")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Benchmark network and show estimated upload time.")] = False,
    resume: Annotated[bool, typer.Option("--resume", help="Resume the most recent interrupted session.")] = False,
    skip_existing: Annotated[bool, typer.Option("--skip-existing", help="Skip files already in S3 (key + size match).")] = False,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Show full file listing in dry-run.")] = False,
) -> None:
    """Upload files or directories to S3."""
    cfg = _require_config()
    effective_bucket = bucket or cfg.bucket
    effective_prefix = prefix if prefix is not None else cfg.prefix
    effective_concurrency = concurrency or cfg.concurrency
    effective_chunk = (chunk_size or cfg.chunk_size_mb) * 1024 * 1024

    # ── Scan ──
    for p in paths:
        if not p.exists():
            err_console.print(f"[red]Path not found:[/red] {p}")
            raise typer.Exit(1)

    console.print("[bold]Scanning files…[/bold]")
    scan = scan_paths(
        paths,
        recursive=recursive,
        include=include or [],
        exclude=exclude or [],
        prefix=effective_prefix,
    )

    if not scan.files:
        console.print("[yellow]No files matched.[/yellow]")
        raise typer.Exit(0)

    console.print(f"  Found [cyan]{scan.file_count}[/cyan] files, [cyan]{format_size(scan.total_size)}[/cyan] total\n")

    # ── Dry-run ──
    if dry_run:
        _do_dry_run(cfg, scan, effective_bucket, effective_concurrency, verbose)
        return

    # ── Confirm large uploads ──
    if scan.file_count > 100 or scan.total_size > 1_073_741_824:
        if not Confirm.ask(
            f"Upload {scan.file_count} files ({format_size(scan.total_size)}) to s3://{effective_bucket}?",
            default=True,
        ):
            raise typer.Abort()

    # ── Resume ──
    cpm = CheckpointManager()
    resume_info: dict = {}
    session = None

    if resume:
        session = cpm.find_latest_session()
        if session is None:
            err_console.print("[yellow]No previous session found to resume.[/yellow]")
        else:
            console.print(f"[bold]Resuming session [cyan]{session.session_id}[/cyan][/bold]")
            resume_info = cpm.build_resume_info(session, cfg)
            incomplete = cpm.incomplete_files(session)
            incomplete_keys = {fc.s3_key for fc in incomplete}
            scan.files = [f for f in scan.files if f.relative_key in incomplete_keys]
            scan.total_size = sum(f.size for f in scan.files)
            if not scan.files:
                console.print("[green]All files already uploaded.[/green]")
                cpm.delete_session(session.session_id)
                return

    if session is None:
        session = cpm.create_session(effective_bucket, effective_prefix)
        for f in scan.files:
            cpm.register_file(session, str(f.local_path), f.relative_key, f.size)

    # ── Audit logger ──
    audit = AuditLogger(session.session_id, effective_bucket, cfg.endpoint_url)
    audit.log_session_start(
        total_files=scan.file_count,
        total_bytes=scan.total_size,
        source_paths=[str(p) for p in paths],
        prefix=effective_prefix,
        concurrency=effective_concurrency,
        chunk_size_mb=effective_chunk // (1024 * 1024),
        resume=resume,
    )

    # ── Upload ──
    uploader = S3Uploader(
        cfg,
        console,
        bucket=effective_bucket,
        concurrency=effective_concurrency,
        chunk_size=effective_chunk,
        audit=audit,
    )

    def _on_file_done(entry, s3_key, uid, parts):
        cpm.mark_file_done(session, s3_key, uid, parts)

    stats = uploader.upload_batch(
        scan.files,
        skip_existing=skip_existing,
        checkpoint_cb=_on_file_done,
        resume_info=resume_info,
    )

    audit.log_session_end(
        completed=stats.completed_files,
        failed=stats.failed_files,
        skipped=stats.skipped_files,
        transferred_bytes=stats.transferred_bytes,
        elapsed_seconds=stats.elapsed,
    )
    console.print(f"[dim]Audit log: {audit.log_path}[/dim]")

    if stats.failed_files == 0:
        cpm.delete_session(session.session_id)
    else:
        console.print(
            f"[yellow]Session [bold]{session.session_id}[/bold] saved. "
            f"Re-run with [bold]--resume[/bold] to retry failed files.[/yellow]"
        )


def _do_dry_run(
    cfg: S3Config,
    scan: ScanResult,
    bucket: str,
    concurrency: int,
    verbose: bool,
) -> None:
    """Run network benchmark and display estimated upload report."""
    # ── File summary ──
    table = Table(title="File Summary", expand=False)
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_row("Total files", str(scan.file_count))
    table.add_row("Total size", format_size(scan.total_size))
    if scan.largest_file:
        table.add_row("Largest file", f"{scan.largest_file.local_path.name} ({format_size(scan.largest_file.size)})")
    if scan.skipped_files:
        table.add_row("Skipped (filtered)", str(scan.skipped_files))
    console.print(table)
    console.print()

    # Extension breakdown
    breakdown = scan.extension_breakdown()
    if breakdown:
        ext_table = Table(title="File Types", expand=False)
        ext_table.add_column("Extension")
        ext_table.add_column("Count", justify="right")
        ext_table.add_column("Size", justify="right")
        for ext, (count, size) in list(breakdown.items())[:15]:
            ext_table.add_row(ext, str(count), format_size(size))
        console.print(ext_table)
        console.print()

    # ── Network benchmark ──
    try:
        result = run_benchmark(cfg, console=console)
    except Exception as exc:
        err_console.print(f"[red]Benchmark failed:[/red] {exc}")
        err_console.print("Cannot estimate upload time without network connectivity.")
        return

    console.print()

    # ── Time estimate ──
    opt, cons = result.estimate_range_seconds(scan.total_size, concurrency)

    est_table = Table(title="Upload Estimate", expand=False)
    est_table.add_column("Metric", style="bold")
    est_table.add_column("Value", justify="right")
    est_table.add_row("Target bucket", f"s3://{bucket}")
    est_table.add_row("Region", result.region)
    est_table.add_row("Latency", f"{result.latency_ms:.0f} ms")
    est_table.add_row("Upload speed", f"{format_speed(result.upload_bytes_per_sec)} ({result.upload_mbps:.1f} Mbps)")
    est_table.add_row("Concurrency", str(concurrency))
    est_table.add_row("Est. time (optimistic)", format_duration(opt))
    est_table.add_row("Est. time (conservative)", format_duration(cons))

    # Rough cost estimate: $0.005 per 1000 PUT requests + $0.023/GB/month storage
    put_cost = (scan.file_count / 1000) * 0.005
    storage_cost = (scan.total_size / (1024**3)) * 0.023
    est_table.add_row("Est. PUT cost", f"${put_cost:.4f}")
    est_table.add_row("Est. storage cost/mo", f"${storage_cost:.4f}")
    console.print(est_table)
    console.print()

    # ── Optional file listing ──
    if verbose or scan.file_count <= 50:
        file_table = Table(title="Files to Upload", expand=False)
        file_table.add_column("#", justify="right")
        file_table.add_column("S3 Key")
        file_table.add_column("Size", justify="right")
        for i, f in enumerate(scan.files, 1):
            file_table.add_row(str(i), f.relative_key, format_size(f.size))
        console.print(file_table)
    elif scan.file_count > 50:
        console.print(f"[dim]Showing first 50 of {scan.file_count} files (use --verbose for all):[/dim]")
        file_table = Table(expand=False)
        file_table.add_column("#", justify="right")
        file_table.add_column("S3 Key")
        file_table.add_column("Size", justify="right")
        for i, f in enumerate(scan.files[:50], 1):
            file_table.add_row(str(i), f.relative_key, format_size(f.size))
        console.print(file_table)


# ── status ───────────────────────────────────────────────────────────────────


@app.command()
def status(
    cleanup: Annotated[bool, typer.Option("--cleanup", help="Abort stale multipart uploads and remove sessions.")] = False,
) -> None:
    """Show active/incomplete upload sessions."""
    cfg = _require_config()
    cpm = CheckpointManager()
    sessions = cpm.list_sessions()

    if not sessions:
        console.print("[dim]No active upload sessions.[/dim]")
        return

    table = Table(title="Upload Sessions", expand=False)
    table.add_column("Session ID", style="cyan")
    table.add_column("Bucket")
    table.add_column("Files", justify="right")
    table.add_column("Progress", justify="right")
    table.add_column("Created")

    for s in sessions:
        pct = (s.completed_files / s.total_files * 100) if s.total_files else 0
        created = time.strftime("%Y-%m-%d %H:%M", time.localtime(s.created_at))
        table.add_row(
            s.session_id,
            s.bucket,
            f"{s.completed_files}/{s.total_files}",
            f"{pct:.0f}%",
            created,
        )

    console.print(table)

    if cleanup:
        for s in sessions:
            aborted = cpm.abort_stale_uploads(s, cfg)
            cpm.delete_session(s.session_id)
            console.print(
                f"  [yellow]Session {s.session_id}:[/yellow] aborted {aborted} multipart upload(s), session removed."
            )


# ── ls ───────────────────────────────────────────────────────────────────────


@app.command(name="ls")
def list_bucket(
    prefix: Annotated[Optional[str], typer.Argument(help="S3 key prefix to filter.")] = None,
    bucket: Annotated[Optional[str], typer.Option("-b", "--bucket", help="Override default bucket.")] = None,
    max_keys: Annotated[int, typer.Option("--max", help="Maximum keys to display.")] = 100,
) -> None:
    """List files in the configured S3 bucket."""
    cfg = _require_config()
    effective_bucket = bucket or cfg.bucket
    client = get_s3_client(cfg)

    kwargs: dict = {"Bucket": effective_bucket, "MaxKeys": max_keys}
    if prefix:
        kwargs["Prefix"] = prefix

    table = Table(title=f"s3://{effective_bucket}/{prefix or ''}", expand=False)
    table.add_column("Key")
    table.add_column("Size", justify="right")
    table.add_column("Last Modified")

    count = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            table.add_row(
                obj["Key"],
                format_size(obj["Size"]),
                obj["LastModified"].strftime("%Y-%m-%d %H:%M"),
            )
            count += 1
            if count >= max_keys:
                break
        if count >= max_keys:
            break

    if count == 0:
        console.print("[dim]No objects found.[/dim]")
    else:
        console.print(table)
        if count >= max_keys:
            console.print(f"[dim]Showing first {max_keys} objects. Use --max to increase.[/dim]")


# ── logs ─────────────────────────────────────────────────────────────────────


@app.command()
def logs(
    show: Annotated[Optional[str], typer.Argument(help="Log filename or index (from list) to display.")] = None,
    tail: Annotated[int, typer.Option("--tail", "-n", help="Show last N entries.")] = 0,
    json_output: Annotated[bool, typer.Option("--json", help="Output raw JSONL instead of formatted table.")] = False,
) -> None:
    """View audit logs from past upload sessions."""
    log_files = list_log_files()

    if not log_files:
        console.print("[dim]No audit logs found.[/dim]")
        console.print(f"[dim]Log directory: {LOGS_DIR}[/dim]")
        return

    # If no argument, list available logs
    if show is None:
        table = Table(title="Audit Logs", expand=False)
        table.add_column("#", justify="right", style="cyan")
        table.add_column("Log File")
        table.add_column("Size", justify="right")
        table.add_column("Modified")

        for i, lf in enumerate(log_files, 1):
            stat = lf.stat()
            modified = time.strftime("%Y-%m-%d %H:%M", time.localtime(stat.st_mtime))
            table.add_row(str(i), lf.name, format_size(stat.st_size), modified)

        console.print(table)
        console.print(f"\n[dim]Log directory: {LOGS_DIR}[/dim]")
        console.print("[dim]Use [bold]rag-ingestinator logs <#>[/bold] to view a specific log.[/dim]")
        return

    # Resolve the log file
    target: Path | None = None
    try:
        idx = int(show)
        if 1 <= idx <= len(log_files):
            target = log_files[idx - 1]
    except ValueError:
        pass

    if target is None:
        candidate = LOGS_DIR / show
        if candidate.exists():
            target = candidate

    if target is None:
        for lf in log_files:
            if show in lf.name:
                target = lf
                break

    if target is None:
        err_console.print(f"[red]Log not found:[/red] {show}")
        raise typer.Exit(1)

    entries = read_log_entries(target)

    if tail > 0:
        entries = entries[-tail:]

    if json_output:
        import json as json_mod
        for entry in entries:
            console.print(json_mod.dumps(entry, indent=2))
        return

    # Formatted display
    console.print(Panel(f"[bold]{target.name}[/bold]", expand=False))

    for entry in entries:
        event = entry.get("event", "unknown")
        ts = entry.get("timestamp", "")
        ts_short = ts[11:19] if len(ts) >= 19 else ts

        if event == "session_start":
            console.print(f"\n[bold green]SESSION START[/bold green]  {ts}")
            console.print(f"  User:       {entry.get('user', '?')}@{entry.get('hostname', '?')}")
            console.print(f"  Bucket:     {entry.get('bucket', '?')}")
            console.print(f"  Endpoint:   {entry.get('endpoint', '?')}")
            console.print(f"  Sources:    {', '.join(entry.get('source_paths', []))}")
            console.print(f"  Files:      {entry.get('total_files', '?')} ({entry.get('total_size_human', '?')})")
            console.print(f"  Concurrency: {entry.get('concurrency', '?')}, Chunk: {entry.get('chunk_size_mb', '?')} MB")
            if entry.get("resume"):
                console.print("  [yellow]Resumed session[/yellow]")

        elif event == "file_done":
            speed = entry.get("speed_human", "")
            console.print(
                f"  [green]OK[/green]  {ts_short}  {entry.get('s3_key', '?')}  "
                f"{entry.get('size_human', '?')}  {entry.get('elapsed_seconds', '?')}s  {speed}"
            )

        elif event == "file_error":
            console.print(
                f"  [red]FAIL[/red] {ts_short}  {entry.get('s3_key', '?')}  "
                f"[red]{entry.get('error', '?')}[/red]"
            )

        elif event == "file_skipped":
            console.print(
                f"  [yellow]SKIP[/yellow] {ts_short}  {entry.get('s3_key', '?')}  "
                f"[dim]{entry.get('reason', '?')}[/dim]"
            )

        elif event == "file_start":
            pass  # Don't clutter output with starts; done/error/skip is enough

        elif event == "session_end":
            console.print(f"\n[bold green]SESSION END[/bold green]    {ts}")
            console.print(
                f"  Completed: {entry.get('completed_files', '?')} | "
                f"Failed: {entry.get('failed_files', '?')} | "
                f"Skipped: {entry.get('skipped_files', '?')}"
            )
            console.print(
                f"  Transferred: {entry.get('transferred_human', '?')} in {entry.get('elapsed_human', '?')} "
                f"({entry.get('avg_speed_human', '?')})"
            )

        elif event == "benchmark":
            console.print(f"  [cyan]BENCH[/cyan] {ts_short}  Latency: {entry.get('latency_ms', '?')} ms, "
                          f"Speed: {entry.get('upload_mbps', '?')} Mbps")

    console.print()
