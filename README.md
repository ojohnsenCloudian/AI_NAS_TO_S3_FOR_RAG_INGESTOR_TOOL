# RAG Ingestinator

Ingest files from local, NAS (NFS/SMB/CIFS), or GPFS filesystems into Amazon S3 with multipart upload, resume support, and real-time progress tracking.

## Features

- **Single file, multi-file, or full directory uploads** to S3
- **Supports any mounted filesystem** – local disks, NFS, SMB/CIFS, GPFS (IBM Spectrum Scale)
- **Multipart upload** with configurable chunk size for large files
- **Resumable uploads** – interrupted sessions can be continued from where they left off
- **Network benchmark dry-run** – test connectivity and estimate upload time before committing
- **Real-time progress** – per-file and overall progress bars with speed and ETA
- **Glob filtering** – include/exclude files by pattern
- **Skip existing** – avoid re-uploading files already in S3
- **Interactive configuration** – guided setup for S3 credentials and defaults

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
```

## Configuration

Run `rag-ingestinator configure` to set up your S3 credentials and defaults interactively. Configuration is stored in `~/.rag_ingestinator/config.yaml`.

## License

MIT
