"""Configuration management – read/write ~/.rag_ingestinator/config.yaml."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

import yaml


CONFIG_DIR = Path.home() / ".rag_ingestinator"
CONFIG_FILE = CONFIG_DIR / "config.yaml"
CHECKPOINTS_DIR = CONFIG_DIR / "checkpoints"

DEFAULT_CHUNK_SIZE_MB = 8
DEFAULT_CONCURRENCY = 4


@dataclass
class S3Config:
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    region: str = "us-east-1"
    endpoint_url: str = ""  # custom S3 endpoint (e.g. Cloudian HyperStore)
    verify_ssl: bool = True
    bucket: str = ""
    prefix: str = ""
    chunk_size_mb: int = DEFAULT_CHUNK_SIZE_MB
    concurrency: int = DEFAULT_CONCURRENCY

    @property
    def chunk_size_bytes(self) -> int:
        return self.chunk_size_mb * 1024 * 1024

    def is_configured(self) -> bool:
        return bool(self.aws_access_key_id and self.aws_secret_access_key and self.bucket)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> S3Config:
        known = {f.name for f in field()} if False else set(S3Config.__dataclass_fields__)
        filtered = {k: v for k, v in data.items() if k in known}
        return cls(**filtered)


def ensure_dirs() -> None:
    """Create config and checkpoint directories if they don't exist."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)


def load_config() -> S3Config:
    """Load configuration from disk, returning defaults if file doesn't exist."""
    if not CONFIG_FILE.exists():
        return S3Config()
    try:
        data = yaml.safe_load(CONFIG_FILE.read_text()) or {}
        return S3Config.from_dict(data)
    except Exception:
        return S3Config()


def save_config(cfg: S3Config) -> None:
    """Persist configuration to disk."""
    ensure_dirs()
    CONFIG_FILE.write_text(yaml.dump(cfg.to_dict(), default_flow_style=False, sort_keys=False))


def get_boto3_session(cfg: S3Config):
    """Build a boto3 Session from config."""
    import boto3

    kwargs: dict[str, Any] = {"region_name": cfg.region}
    if cfg.aws_access_key_id:
        kwargs["aws_access_key_id"] = cfg.aws_access_key_id
    if cfg.aws_secret_access_key:
        kwargs["aws_secret_access_key"] = cfg.aws_secret_access_key
    return boto3.Session(**kwargs)


def get_s3_client(cfg: S3Config):
    """Return a boto3 S3 client from config."""
    kwargs: dict[str, Any] = {}
    if cfg.endpoint_url:
        kwargs["endpoint_url"] = cfg.endpoint_url
    if not cfg.verify_ssl:
        kwargs["verify"] = False
    return get_boto3_session(cfg).client("s3", **kwargs)


def validate_s3_connection(cfg: S3Config) -> tuple[bool, str]:
    """Test that we can reach the configured bucket. Returns (ok, message)."""
    try:
        client = get_s3_client(cfg)
        client.head_bucket(Bucket=cfg.bucket)
        return True, f"Successfully connected to s3://{cfg.bucket}"
    except Exception as exc:
        return False, f"Connection failed: {exc}"
