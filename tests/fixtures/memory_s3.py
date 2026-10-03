"""A boto3 client over a dict, for ``S3Client.raw_client`` in unit fakes.

Serves what the corpus series marker code calls (``deploy/corpus.py`` and
``corpus_digest.read_corpus_markers``): ``list_objects_v2`` through a
paginator, ``get_object`` and ``put_object``. Objects live in a shared
``store`` dict keyed by ``(bucket, key)`` so every ``S3Client`` a test
constructs sees the same bucket.
"""

from __future__ import annotations

import hashlib
import io
from typing import Any


class MemoryBoto:
    def __init__(self, store: dict[tuple[str, str], bytes]) -> None:
        self.store = store
        self.puts: list[tuple[str, str]] = []

    def get_paginator(self, operation: str) -> Any:
        assert operation == "list_objects_v2", operation
        store = self.store

        class _Paginator:
            def paginate(self, Bucket: str, Prefix: str = "", **_kw: Any):  # noqa: N803
                keys = sorted(k for b, k in store if b == Bucket and k.startswith(Prefix))
                yield {
                    "Contents": [
                        {
                            "Key": k,
                            "Size": len(store[(Bucket, k)]),
                            "ETag": f'"{hashlib.md5(store[(Bucket, k)]).hexdigest()}"',  # noqa: S324
                        }
                        for k in keys
                    ]
                }

        return _Paginator()

    def list_objects_v2(
        self,
        Bucket: str,  # noqa: N803
        Prefix: str = "",  # noqa: N803
        MaxKeys: int = 1000,  # noqa: N803
        **_kw: Any,
    ) -> dict[str, Any]:
        keys = sorted(k for b, k in self.store if b == Bucket and k.startswith(Prefix))[:MaxKeys]
        return {"KeyCount": len(keys), "Contents": [{"Key": k} for k in keys]}

    def get_object(self, Bucket: str, Key: str, **_kw: Any) -> dict[str, Any]:  # noqa: N803
        try:
            body = self.store[(Bucket, Key)]
        except KeyError:
            from botocore.exceptions import ClientError

            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject"
            ) from None
        return {"Body": io.BytesIO(body)}

    def put_object(self, Bucket: str, Key: str, Body: bytes, **_kw: Any) -> dict[str, Any]:  # noqa: N803
        self.store[(Bucket, Key)] = bytes(Body)
        self.puts.append((Bucket, Key))
        return {}
