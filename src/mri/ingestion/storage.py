"""Object-store abstraction: MinIO for runtime, in-memory for tests.

The pipeline only depends on the small :class:`ObjectStore` protocol, which
keeps the core logic testable without a running MinIO server.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from mri.ingestion.exceptions import ObjectStoreError


@dataclass(frozen=True)
class ObjectStat:
    """Minimal object metadata returned by a store."""

    size: int
    etag: str
    metadata: dict[str, str]


class ObjectStore(Protocol):
    """The minimal object-store surface used by the pipeline."""

    def ensure_bucket(self, bucket: str) -> None: ...

    def object_exists(self, bucket: str, key: str) -> bool: ...

    def put_file(
        self,
        bucket: str,
        key: str,
        path: object,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None: ...

    def put_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None: ...

    def get_bytes(self, bucket: str, key: str) -> bytes: ...

    def list_keys(self, bucket: str, prefix: str = "") -> list[str]: ...


class MinioObjectStore:
    """``ObjectStore`` backed by a MinIO/S3 server."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        *,
        secure: bool = False,
        region: str | None = None,
    ) -> None:
        from minio import Minio

        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
        )

    def bucket_exists(self, bucket: str) -> bool:
        try:
            return bool(self._client.bucket_exists(bucket))
        except Exception as exc:  # pragma: no cover - network dependent
            raise ObjectStoreError(f"cannot query bucket {bucket!r}: {exc}") from exc

    def ensure_bucket(self, bucket: str) -> None:
        from minio.error import S3Error

        try:
            if not self._client.bucket_exists(bucket):
                self._client.make_bucket(bucket)
        except S3Error as exc:  # pragma: no cover - network dependent
            if exc.code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                raise ObjectStoreError(f"cannot create bucket {bucket!r}: {exc}") from exc
        except Exception as exc:  # pragma: no cover - network dependent
            raise ObjectStoreError(f"cannot create bucket {bucket!r}: {exc}") from exc

    def object_exists(self, bucket: str, key: str) -> bool:
        from minio.error import S3Error

        try:
            self._client.stat_object(bucket, key)
            return True
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket", "NotFound"}:
                return False
            raise ObjectStoreError(f"cannot stat {bucket}/{key}: {exc}") from exc
        except Exception as exc:  # pragma: no cover - network dependent
            raise ObjectStoreError(f"cannot stat {bucket}/{key}: {exc}") from exc

    def put_file(
        self,
        bucket: str,
        key: str,
        path: Any,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        try:
            self._client.fput_object(
                bucket,
                key,
                str(path),
                content_type=content_type or "application/octet-stream",
                metadata=metadata,  # type: ignore[arg-type]
            )
        except Exception as exc:  # pragma: no cover - network dependent
            raise ObjectStoreError(f"cannot upload {path} to {bucket}/{key}: {exc}") from exc

    def put_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        import io

        try:
            self._client.put_object(
                bucket,
                key,
                io.BytesIO(data),
                length=len(data),
                content_type=content_type or "application/octet-stream",
                metadata=metadata,  # type: ignore[arg-type]
            )
        except Exception as exc:  # pragma: no cover - network dependent
            raise ObjectStoreError(f"cannot upload bytes to {bucket}/{key}: {exc}") from exc

    def get_bytes(self, bucket: str, key: str) -> bytes:
        response = None
        try:
            response = self._client.get_object(bucket, key)
            return bytes(response.read())
        except Exception as exc:  # pragma: no cover - network dependent
            raise ObjectStoreError(f"cannot download {bucket}/{key}: {exc}") from exc
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def stat(self, bucket: str, key: str) -> Any:
        try:
            return self._client.stat_object(bucket, key)
        except Exception as exc:  # pragma: no cover - network dependent
            raise ObjectStoreError(f"cannot stat {bucket}/{key}: {exc}") from exc

    def list_keys(self, bucket: str, prefix: str = "") -> list[str]:
        try:
            return [
                str(obj.object_name)
                for obj in self._client.list_objects(bucket, prefix=prefix, recursive=True)
            ]
        except Exception as exc:  # pragma: no cover - network dependent
            raise ObjectStoreError(f"cannot list {bucket}/{prefix}: {exc}") from exc


@dataclass
class _StoredObject:
    data: bytes
    content_type: str
    metadata: dict[str, str]


class InMemoryObjectStore:
    """In-process object store used by unit tests (no network)."""

    def __init__(self) -> None:
        self._buckets: dict[str, dict[str, bytes]] = {}
        self._metadata: dict[tuple[str, str], dict[str, str]] = {}
        self._content_types: dict[tuple[str, str], str] = {}

    def bucket_exists(self, bucket: str) -> bool:
        return bucket in self._buckets

    def ensure_bucket(self, bucket: str) -> None:
        self._buckets.setdefault(bucket, {})

    def object_exists(self, bucket: str, key: str) -> bool:
        return key in self._buckets.get(bucket, {})

    def put_file(
        self,
        bucket: str,
        key: str,
        path: Any,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        from pathlib import Path as _Path

        self.ensure_bucket(bucket)
        self._buckets[bucket][key] = _Path(path).read_bytes()
        self._metadata[(bucket, key)] = dict(metadata or {})
        self._content_types[(bucket, key)] = content_type or "application/octet-stream"

    def put_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        self.ensure_bucket(bucket)
        self._buckets[bucket][key] = data
        self._metadata[(bucket, key)] = dict(metadata or {})
        self._content_types[(bucket, key)] = content_type or "application/octet-stream"

    def get_bytes(self, bucket: str, key: str) -> bytes:
        if not self.object_exists(bucket, key):
            raise ObjectStoreError(f"no such object {bucket}/{key}")
        return self._buckets[bucket][key]

    def metadata_for(self, bucket: str, key: str) -> dict[str, str]:
        return dict(self._metadata.get((bucket, key), {}))

    def list_keys(self, bucket: str, prefix: str = "") -> list[str]:
        return sorted(key for key in self._buckets.get(bucket, {}) if key.startswith(prefix))


__all__ = [
    "InMemoryObjectStore",
    "MinioObjectStore",
    "ObjectStore",
    "ObjectStat",
]
