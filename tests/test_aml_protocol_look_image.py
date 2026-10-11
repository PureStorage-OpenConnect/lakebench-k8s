"""A datagen Job whose context names no image falls back to the schema's
default: a template that disagreed would generate with an image the config does
not name, and the corpus identity would record the wrong one."""

from __future__ import annotations

import yaml

from lakebench.config.schema import ImagesConfig
from lakebench.deploy.engine import TemplateRenderer
from tests.fixtures.functional_templates_helpers import _enrich_context, _make_engine


def test_job_template_default_is_the_schema_default():
    ctx = _enrich_context(_make_engine())
    ctx.pop("datagen_image")
    rendered = TemplateRenderer().render("datagen/job.yaml.j2", ctx)
    jobs = [d for d in yaml.safe_load_all(rendered) if d and d.get("kind") == "Job"]
    assert jobs[0]["spec"]["template"]["spec"]["containers"][0]["image"] == ImagesConfig().datagen
