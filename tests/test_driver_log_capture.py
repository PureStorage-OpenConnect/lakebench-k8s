"""Driver-log capture seam logic: the pure helpers that keep a long run's
driver log continuous across a severed ``kubectl logs -f``.

``kubectl logs -f`` is cut at the kubelet streamingConnectionIdleTimeout (4h
on the test cluster). Before 1.7.11 nothing restarted it, so a 24h record held
only the first ~4h of batches and the ingest ratio read a kept-up bronze as a
fraction of itself. These cover the two pure functions the resume path leans
on: ``_last_stamp`` (where to resume from) and ``merge_logs`` (how the kept and
live logs are spliced without a gap or a duplicate run).

No kubectl, no Spark: fast pure-Python.
"""

from __future__ import annotations

from lakebench.cli._driver_log_capture import DriverLogCapturer, merge_logs


class TestLastStamp:
    def test_reads_the_last_timestamped_line(self, tmp_path):
        p = tmp_path / "d.log"
        p.write_text(
            "2026-10-21T14:27:18.258Z [lb] Batch 0: writing 4,014,036 rows\n"
            "2026-10-21T18:26:03.499Z [lb] Batch 621: writing 9,064,351 rows\n"
        )
        assert DriverLogCapturer._last_stamp(p) == "2026-10-21T18:26:03.499Z"

    def test_skips_trailing_unstamped_lines(self, tmp_path):
        # A kubectl error ('error: unexpected EOF') lands after the last real
        # line with no timestamp; resume must use the last STAMPED line.
        p = tmp_path / "d.log"
        p.write_text("2026-10-21T18:26:03.499Z [lb] Batch 621\nerror: unexpected EOF\n")
        assert DriverLogCapturer._last_stamp(p) == "2026-10-21T18:26:03.499Z"

    def test_none_when_no_stamped_line(self, tmp_path):
        p = tmp_path / "d.log"
        p.write_text("error: the server doesn't have a resource type\n")
        assert DriverLogCapturer._last_stamp(p) is None

    def test_none_when_file_missing(self, tmp_path):
        assert DriverLogCapturer._last_stamp(tmp_path / "nope.log") is None


class TestMergeLogs:
    def test_overlap_of_full_tail_is_not_duplicated(self):
        # live repeats the last _OVERLAP_LINES (3) kept lines (the --since-time
        # seam) then adds new ones; the shared run must appear once.
        kept = "a\nb\nc\nd\n"
        live = "b\nc\nd\ne\nf\n"
        assert merge_logs(kept, live) == "a\nb\nc\nd\ne\nf\n"

    def test_short_overlap_may_repeat_but_never_gaps(self):
        # --since-time has 1s granularity, so the seam overlap can be shorter
        # than the 3-line dedup window. The contract is "a few repeated lines
        # are fine, a gap is not": the new lines (d, e) must all be present,
        # even if b/c repeat. Downstream keys on distinct batch numbers.
        merged = merge_logs("a\nb\nc\n", "b\nc\nd\ne\n")
        assert merged is not None
        assert merged.endswith("d\ne\n")
        assert "d" in merged and "e" in merged

    def test_disjoint_logs_are_concatenated(self):
        assert merge_logs("a\nb\n", "c\nd\n") == "a\nb\nc\nd\n"

    def test_live_fully_contained_keeps_kept(self):
        # A resume that fetched only lines already captured adds nothing.
        assert merge_logs("a\nb\nc\n", "b\nc\n") == "a\nb\nc\n"

    def test_empty_kept_returns_live(self):
        assert merge_logs(None, "a\nb\n") == "a\nb\n"
        assert merge_logs("", "a\nb\n") == "a\nb\n"

    def test_empty_live_returns_kept(self):
        assert merge_logs("a\nb\n", None) == "a\nb\n"
        assert merge_logs("a\nb\n", "") == "a\nb\n"
