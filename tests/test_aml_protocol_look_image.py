"""A datagen Job whose context names no image fails to render: a template
default could disagree with the schema, generate with an image the config does
not name, and record the wrong one in the corpus identity."""

from __future__ import annotations

import pytest
from jinja2 import UndefinedError

from lakebench.deploy.engine import TemplateRenderer
from tests.fixtures.functional_templates_helpers import _enrich_context, _make_engine


def test_job_template_has_no_image_default():
    ctx = _enrich_context(_make_engine())
    ctx.pop("datagen_image")
    with pytest.raises(UndefinedError):
        TemplateRenderer().render("datagen/job.yaml.j2", ctx)
