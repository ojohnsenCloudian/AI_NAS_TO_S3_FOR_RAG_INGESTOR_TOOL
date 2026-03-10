# RAG Ingestinator

Ingest files from local, NAS (NFS/SMB/CIFS), or GPFS filesystems into any S3-compatible object store with multipart upload, resume support, and real-time progress tracking.

Works with **AWS S3**, **Cloudian HyperStore**, **MinIO**, **Ceph**, and any S3-compatible endpoint.

## Features

- **Single file, multi-file, or full directory uploads** to S3-compatible storage
- **Custom S3 endpoints** – use Cloudian HyperStore, MinIO, Ceph, or any S3-compatible API
- **Supports any mounted filesystem** – local disks, NFS, SMB/CIFS, GPFS (IBM Spectrum Scale)
- **Multipart upload** with configurable chunk size for large files
- **Resumable uploads** – interrupted sessions can be continued from where they left off
- **Network benchmark dry-run** – test connectivity and estimate upload time before committing
- **Real-time progress** – per-file and overall progress bars with speed and ETA
- **Glob filtering** – include/exclude files by pattern
- **Skip existing** – avoid re-uploading files already in S3
- **Audit logging** – structured JSONL logs for every upload session (who, what, when, success/fail per file)
- **Interactive configuration** – guided setup for credentials, endpoint, and defaults

## Installation

```bash
pip install -e .
```

## Quick Start

```bash
# Configure S3 credentials and defaults
rag-ingestinator configure

# Upload a single file
rag-ingestinator upload report.pdf

# Upload an entire directory recursively
rag-ingestinator upload ./data/ -r

# Upload from a mounted NFS/SMB/GPFS share
rag-ingestinator upload /mnt/nas/research_data/ -r

# Dry-run with network benchmark
rag-ingestinator upload ./data/ -r --dry-run

# Upload only PDFs, skip files already in S3
rag-ingestinator upload ./docs/ -r --include "*.pdf" --skip-existing

# Resume an interrupted upload
rag-ingestinator upload ./data/ -r --resume

# List files in S3 bucket
rag-ingestinator ls

# Check upload session status
rag-ingestinator status

# View audit logs
rag-ingestinator logs

# View a specific log (by index from the list)
rag-ingestinator logs 1

# View last 20 entries of a log
rag-ingestinator logs 1 --tail 20

# Export raw JSONL for external tools
rag-ingestinator logs 1 --json
```

## Configuration

Run `rag-ingestinator configure` to set up your credentials and defaults interactively. Configuration is stored in `~/.rag_ingestinator/config.yaml`.

The wizard will prompt for:
- S3 endpoint URL (leave blank for AWS, or enter your Cloudian HyperStore / MinIO / Ceph URL)
- SSL verification toggle (useful for self-signed certs)
- Access Key ID and Secret Access Key
- Region, default bucket, key prefix
- Multipart chunk size and concurrency

## Audit Logs

Every upload session is logged to `~/.rag_ingestinator/logs/` in JSONL format. Each log line is a JSON object recording:
- **session_start**: user, hostname, bucket, endpoint, source paths, file count/size
- **file_done**: local path, S3 key, size, upload duration, speed
- **file_error**: local path, S3 key, error message
- **file_skipped**: local path, S3 key, reason (e.g. already exists)
- **session_end**: totals for completed/failed/skipped, total transfer, elapsed time, avg speed

Use `rag-ingestinator logs` to browse and `--json` to pipe raw JSONL to external audit tools.

## License

MIT
