"""The datagen Job template falls back to the schema's default image: a
template that disagreed would generate with an image the config does not
name, and the corpus identity would record the wrong one."""

from __future__ import annotations

import re
from pathlib import Path

from lakebench.config.schema import ImagesConfig

ROOT = Path(__file__).resolve().parents[1]


def test_job_template_default_is_the_schema_default():
    text = (ROOT / "src/lakebench/templates/datagen/job.yaml.j2").read_text()
    m = re.search(r"datagen_image \| default\('([^']+)'\)", text)
    assert m and m.group(1) == ImagesConfig().datagen
