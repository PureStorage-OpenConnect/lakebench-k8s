"""A fake corpus bucket with series markers, and observe_corpus over it."""

from __future__ import annotations

import hashlib
import io
import json
from types import SimpleNamespace

from lakebench import corpus_digest as cd
from lakebench.config.schema import ImagesConfig
from lakebench.metrics import corpus_identity as ci
from tests.fixtures.experiment_helpers import _cfg

H1 = "a" * 64


D = "sha256:" + "1" * 64  # the image that generated the corpus


TAG = ImagesConfig().datagen  # the configured image (the default config)


SCOPE = "customer/interactions/"


def marker(cycle=0, node=0, total=2, cycles=1, h=H1, **kw):
    body = {
        "format": 1,
        "schema": "customer360",
        "model_version": "datagen-v2-rs-0.3",
        "build_commit": "abc1234",
        "cycle": cycle,
        "cycles": cycles,
        "node_id": node,
        "total_nodes": total,
        "delivery_mode": "batch",
        "files_written": 3,
        "rows_written": 10,
        "bytes_written": 100,
        "seed_ref": "42",
        "corpus_args": {"scale": 1.0},
        "corpus_args_sha256": h,
        "completed_utc": "2026-10-06T00:00:00Z",
    }
    body.update(kw)
    return body


def series(
    digest=D, image=TAG, seed_ref="42", cycles_total=1, updated="2026-10-06T00:01:00Z", **gen
):
    return {
        "format": 1,
        "schema": "customer360",
        "updated_utc": updated,
        "cycles_total": cycles_total,
        "cycles_complete": list(range(cycles_total)),
        "generation": {
            "seed_ref": seed_ref,
            "scale": 1.0,
            "image": image,
            "image_digest": digest,
            **({} if digest else {"image_digest_reason": "datagen pods ran different images"}),
            **gen,
        },
    }


class FakeBoto:
    """list_objects_v2 paginator (pages of 2) and get_object over a dict."""

    def __init__(self, objects: dict[str, bytes]):
        self.objects = dict(objects)
        self.etags = {k: hashlib.md5(v).hexdigest() for k, v in objects.items()}  # noqa: S324
        self.lists = 0
        self.gets: list[str] = []

    def get_paginator(self, op):
        assert op == "list_objects_v2"
        fake = self

        class P:
            def paginate(self, Bucket, Prefix):  # noqa: N803
                fake.lists += 1
                keys = sorted(k for k in fake.objects if k.startswith(Prefix))
                for i in range(0, max(len(keys), 1), 2):
                    yield {
                        "Contents": [
                            {
                                "Key": k,
                                "Size": len(fake.objects[k]),
                                "ETag": f'"{fake.etags[k]}"',
                            }
                            for k in keys[i : i + 2]
                        ]
                    }

        return P()

    def get_object(self, Bucket, Key):  # noqa: N803
        self.gets.append(Key)
        return {"Body": io.BytesIO(self.objects[Key])}


def bucket(markers=(), series_body=None, data=("part-0.parquet", "part-1.parquet"), scope=SCOPE):
    objs: dict[str, bytes] = {f"{scope}{name}": b"x" * 10 for name in data}
    for m in markers:
        objs[cd.marker_key(scope, m["cycle"], m["node_id"])] = json.dumps(m).encode()
    if series_body is not None:
        objs[cd.series_key(scope)] = json.dumps(series_body).encode()
    return FakeBoto(objs)


def observe(markers=(), series_body=None, lineage_path=None, **kw):
    """observe_corpus over a fake bucket, through a real config."""
    boto = bucket(markers, series_body, **kw)
    cfg = _cfg()
    obs = ci.observe_corpus(cfg, SimpleNamespace(raw_client=boto), lineage_path=lineage_path)
    return obs, boto


def two_nodes(h=H1, **kw):
    return [marker(node=0, h=h, **kw), marker(node=1, h=h, **kw)]
