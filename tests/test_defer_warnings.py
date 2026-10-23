"""``defer_warnings`` holds warnings until the end of the block."""

from __future__ import annotations

import logging

import pytest

from lakebench.cli._helpers import defer_warnings, print_info, print_warning


def test_warnings_are_printed_together_after_the_block(capsys):
    lg = logging.getLogger("lakebench.deploy.engine")
    with defer_warnings():
        print_warning("first")
        print_info("progress")
        lg.warning("S3 backend does not implement bucket tagging for %r", "dep")
        print_warning("first")
        mid = capsys.readouterr().err
        assert "progress" in mid
        assert "first" not in mid and "bucket tagging" not in mid
    out = capsys.readouterr().err
    assert "2 warnings:" in out
    assert out.count("first") == 1
    assert "S3 backend does not implement bucket tagging for 'dep'" in out
    assert out.index("first") < out.index("bucket tagging")


def test_warnings_print_when_the_block_raises(capsys):
    with pytest.raises(RuntimeError), defer_warnings():
        print_warning("held")
        raise RuntimeError("boom")
    assert "1 warning:" in capsys.readouterr().err
