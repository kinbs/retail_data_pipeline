"""Data-lake access layer.

The pipeline only ever talks to this module, never to boto3 directly. That keeps
``extract`` / ``load`` identical whether the lake is a real S3 bucket or a local
folder used as a stand-in, which is what lets the project run on a laptop with
no AWS account.

Both implementations expose the same four operations:

    download(key, local_path)  -> Path
    put_file(local_path, key, metadata)
    put_bytes(body, key, metadata)
    list_objects(prefix)       -> list[LakeObject]
    latest(prefix)             -> key or None
"""
from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from log_config import setup_logger
from scripts import config

logger = setup_logger(__name__, str(config.LOGS_DIR / 'storage.log'))


@dataclass
class LakeObject:
    key: str
    last_modified: datetime
    metadata: dict = field(default_factory=dict)


def md5_of(payload: bytes) -> str:
    return hashlib.md5(payload).hexdigest()


# --------------------------------------------------------------------------- #
# Local folder "bucket"
# --------------------------------------------------------------------------- #
class LocalLake:
    """Emulates an S3 bucket with a directory tree.

    Object metadata (hash, record count, ...) is stored in a side-car JSON file
    under ``<root>/.metadata`` so the de-duplication logic behaves exactly like
    the S3 variant.
    """

    def __init__(self, root: Path):
        self.root = Path(root)
        self.meta_root = self.root / '.metadata'
        self.root.mkdir(parents=True, exist_ok=True)
        self.meta_root.mkdir(parents=True, exist_ok=True)

    # -- helpers ----------------------------------------------------------- #
    def _file(self, key: str) -> Path:
        return self.root / key

    def _meta_file(self, key: str) -> Path:
        safe = key.replace('/', '__')
        return self.meta_root / f'{safe}.json'

    def _write_meta(self, key: str, metadata: dict) -> None:
        self._meta_file(key).write_text(json.dumps(metadata, indent=2), encoding='utf-8')

    def _read_meta(self, key: str) -> dict:
        path = self._meta_file(key)
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except json.JSONDecodeError:
            return {}

    # -- API --------------------------------------------------------------- #
    def download(self, key: str, local_path: Path) -> Path:
        src = self._file(key)
        if not src.exists():
            raise FileNotFoundError(f'Object not found in local lake: {key}')
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, local_path)
        logger.info(f'File downloaded from lake: {src} -> {local_path}')
        return local_path

    def put_file(self, local_path: Path, key: str, metadata: dict | None = None) -> str:
        return self.put_bytes(Path(local_path).read_bytes(), key, metadata)

    def put_bytes(self, body: bytes, key: str, metadata: dict | None = None) -> str:
        dest = self._file(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)
        self._write_meta(key, metadata or {})
        logger.info(f'File uploaded to lake: {dest} ({len(body)} bytes)')
        return key

    def list_objects(self, prefix: str = '') -> list[LakeObject]:
        base = self.root / prefix if prefix else self.root
        if not base.exists():
            return []
        results: list[LakeObject] = []
        for path in base.rglob('*'):
            if not path.is_file():
                continue
            rel = path.relative_to(self.root).as_posix()
            if rel.startswith('.'):
                continue
            results.append(
                LakeObject(
                    key=rel,
                    last_modified=datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc),
                    metadata=self._read_meta(rel),
                )
            )
        return sorted(results, key=lambda o: o.last_modified)

    def latest(self, prefix: str = '') -> str | None:
        objects = self.list_objects(prefix)
        if not objects:
            logger.warning(f'No objects found under prefix "{prefix}" in {self.root}')
            return None
        return objects[-1].key


# --------------------------------------------------------------------------- #
# Real S3 bucket
# --------------------------------------------------------------------------- #
class S3Lake:
    def __init__(self) -> None:
        import boto3  # imported lazily so local mode does not need boto3

        self.bucket = config.AWS_S3_BUCKET_NAME
        self.client = boto3.client(
            's3',
            aws_access_key_id=config.AWS_ACCESS_KEY,
            aws_secret_access_key=config.AWS_SECRET_KEY,
            region_name=config.AWS_REGION,
            endpoint_url=config.AWS_ENDPOINT_URL,
        )

    def download(self, key: str, local_path: Path) -> Path:
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket, key, str(local_path))
        logger.info(f'File downloaded from S3: s3://{self.bucket}/{key}')
        return local_path

    def put_file(self, local_path: Path, key: str, metadata: dict | None = None) -> str:
        self.client.upload_file(
            str(local_path), self.bucket, key, ExtraArgs={'Metadata': _s3_metadata(metadata)}
        )
        logger.info(f'File {key} uploaded to s3://{self.bucket}')
        return key

    def put_bytes(self, body: bytes, key: str, metadata: dict | None = None) -> str:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body,
                               Metadata=_s3_metadata(metadata))
        logger.info(f'File {key} uploaded to s3://{self.bucket}')
        return key

    def list_objects(self, prefix: str = '') -> list[LakeObject]:
        paginator = self.client.get_paginator('list_objects_v2')
        results: list[LakeObject] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                head = self.client.head_object(Bucket=self.bucket, Key=obj['Key'])
                results.append(
                    LakeObject(
                        key=obj['Key'],
                        last_modified=obj['LastModified'],
                        metadata=head.get('Metadata', {}),
                    )
                )
        return sorted(results, key=lambda o: o.last_modified)

    def latest(self, prefix: str = '') -> str | None:
        objects = self.list_objects(prefix)
        if not objects:
            logger.error(f'No files found in s3://{self.bucket}/{prefix}')
            return None
        latest = objects[-1].key
        logger.info(f'Latest file found: {latest}')
        return latest


def _s3_metadata(metadata: dict | None) -> dict:
    """S3 metadata values must be strings."""
    return {str(k): str(v) for k, v in (metadata or {}).items()}


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #
_INSTANCE: LocalLake | S3Lake | None = None


def get_lake() -> LocalLake | S3Lake:
    global _INSTANCE
    if _INSTANCE is None:
        _INSTANCE = LocalLake(config.LOCAL_LAKE_DIR) if config.USE_LOCAL else S3Lake()
    return _INSTANCE


def find_by_hash(prefix: str, content_hash: str) -> str | None:
    """Return the key of an object already carrying ``content_hash``, if any."""
    for obj in get_lake().list_objects(prefix):
        if obj.metadata.get('hash') == content_hash:
            return obj.key
    return None


def ensure_raw_data(local_csv: Path = None) -> str:
    """Make sure the raw CSV exists in the lake, seeding it from the repo copy."""
    lake = get_lake()
    key = config.RAW_FILE_KEY
    local_csv = Path(local_csv or config.LOCAL_RAW_CSV)

    existing = [o.key for o in lake.list_objects(config.RAW_PREFIX)]
    if key in existing:
        logger.info(f'Raw data already present in lake: {key}')
        return key

    if not local_csv.exists():
        raise FileNotFoundError(
            f'Raw CSV not found ({local_csv}) and nothing under "{config.RAW_PREFIX}" in the lake.'
        )
    logger.info(f'Seeding lake with {local_csv} -> {key}')
    lake.put_file(local_csv, key, {'source': 'repository', 'uploaded_at': datetime.now().isoformat()})
    return key


def iter_keys(prefix: str) -> Iterable[str]:
    return (o.key for o in get_lake().list_objects(prefix))
