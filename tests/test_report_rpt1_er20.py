"""RPT-1 report fixes R5, R6, R8, R10, R20 and R21,
plus two bottleneck numbers found while building R21.

Each test asserts the fixed number or label on a stored record against a
value the test reads from the record itself.
"""

from __future__ import annotations

import re

import pytest

from tests.fixtures.report_consistency_helpers import _render_dict
from tests.fixtures.report_goldens import page_text, render
from tests.fixtures.stored_records import load_record


def _plain(html: str) -> str:
    html = re.sub(r"<style>.*?</style>", " ", page_text(html), flags=re.S)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def _text(run_id: str) -> str:
    return _plain(render(run_id))


def _section(text: str, start: str, end: str) -> str:
    i = text.index(start)
    return text[i : text.index(end, i + len(start))]


# ---------------------------------------------------------------------------
# R5: resources as run, not config defaults
# ---------------------------------------------------------------------------


def test_r5_resources_as_run_from_the_jobs():
    record = load_record("1320bd")
    silver = next(j for j in record["jobs"] if j["job_type"] == "silver-build")
    text = _text("1320bd")
    resources = _section(text, "Resources as run", "Configuration")
    assert (
        f"silver-build {silver['executor_count']} {silver['executor_cores']} "
        f"{silver['executor_memory_gb']:.0f} GB not recorded (this record predates it)"
    ) in resources
    # The snapshot's spark.executor block sized nothing and is not shown.
    executor = record["config_snapshot"]["spark"]["executor"]
    assert executor["instances"] != silver["executor_count"]
    assert "Executors, " not in text


def test_r5_scratch_as_ran_from_provenance():
    record = load_record("1320bd")
    record["provenance"]["scratch_as_ran"] = {
        "silver-build": {"size_limit": "300Gi", "storage_class": "px-csi-scratch"},
        "gold-finalize": {"size_limit": None, "storage_class": None},
        "bronze-verify": {"not_recorded": "the SparkApplication spec was not read"},
    }
    text = _plain(_render_dict(record))
    assert "silver-build 8 4 48 GB 300Gi on px-csi-scratch" in text
    assert "no scratch PVC" in text
    assert "not recorded (the SparkApplication spec was not read)" in text


def test_r5_continuous_resources_from_the_stages():
    record = load_record("ebb26f")
    stage = next(s for s in record["pipeline_benchmark"]["stages"] if s["stage_name"] == "silver")
    resources = _section(_text("ebb26f"), "Resources as run", "Configuration")
    assert (
        f"silver-stream {stage['executor_count']} {stage['executor_cores']} "
        f"{stage['executor_memory_gb']:.0f} GB"
    ) in resources


# ---------------------------------------------------------------------------
# R6: a failed run shows no headline number
# ---------------------------------------------------------------------------


def test_r6_failed_run_headline_is_the_verdict_reason():
    """The headline is the first verdict reason a reader can act on (the
    generic exit-intent line compute_verdict puts first is skipped), the
    failed job's error follows, and no headline figure is shown anywhere
    on the page: neither the cards nor the pipeline summary line."""
    record = load_record("be2b70")
    assert record["verdict"]["status"] == "FAILED"
    reasons = record["verdict"]["reasons"]
    assert reasons[0] == "Process exit intent was not OK"
    first = reasons[1]
    scores = record["pipeline_benchmark"]["scores"]
    gold = next(j for j in record["jobs"] if j["job_type"] == "gold-finalize")
    text = _text("be2b70")
    head = _section(text, f"Run FAILED: {first}", "Bottleneck Identification")
    assert "Job timed out after 1800s" in gold["error_message"]
    assert "lakebench-gold-finalize" in head and "Job timed out after 1800s" in head
    for label in ("Time to Value", "Pipeline Throughput", "QpH", "Scale Ratio"):
        assert f"{label} - not shown: the run did not pass" in head
    # The read-first panel's headline is the same reason.
    assert f"Headline: {first}" in text
    assert f"{scores['time_to_value_seconds']:.1f}s" not in text
    assert f"{scores['pipeline_throughput_gb_per_second']:.3f} GB/s" not in text
    assert "headline figures not shown: the run is FAILED" in text


def test_r6_passed_run_keeps_its_numbers():
    ttv = load_record("5105a0")["pipeline_benchmark"]["scores"]["time_to_value_seconds"]
    text = _text("5105a0")
    assert f"Time to Value {ttv:.1f}s" in text
    assert "not shown: the run did not pass" not in text


# ---------------------------------------------------------------------------
# R8: continuous ingest labels
# ---------------------------------------------------------------------------


def test_r8_continuous_ingest_labels_and_coverage():
    record = load_record("ebb26f")
    scores = record["pipeline_benchmark"]["scores"]
    sustained = record["config_snapshot"]["sustained"]
    coverage = scores["corpus_ingest_ratio"]
    assert coverage == 0.4374
    text = _text("ebb26f")
    # The denominator is a mean-rate estimate of the rows released, and
    # the label says so; it is never the corpus.
    assert "Ingest Ratio (estimate)" in text
    assert "bronze rows / rows released by the window's end" in text
    assert "bronze rows / datagen rows" not in text
    assert f"Corpus coverage {coverage * 100:.1f}%" in text
    assert f"Window {scores['window_seconds']:,.0f}s" in text
    files, trigger = sustained["max_files_per_trigger"], sustained["bronze_trigger_interval"]
    assert f"Offered load trickle {files} file per {trigger} bronze trigger" in text
    assert "Excluded in continuous mode: AML continuous runs detection rules" in text


def test_r8_continuous_excluded_rules_are_not_no_data():
    """A record keeps the rules its run excluded: ebb26f ran before W5 and
    W6 joined continuous, so they read excluded, never no data."""
    from lakebench.metrics.storage import MetricsStorage
    from lakebench.metrics.verdict import continuous_excluded_rules

    record = load_record("ebb26f")
    excluded = continuous_excluded_rules(
        MetricsStorage.__new__(MetricsStorage)._dict_to_metrics(record)
    )
    assert {"W5_sanctions_match", "W6_pep_counterparty"} <= excluded
    text = _text("ebb26f")
    table = _section(text, "Detection Scorecard", "Continuous run: counts")
    for rule in excluded:
        row = re.search(rf"{rule} \S+ (.*?)(?= W\d|$)", table)
        assert row and "excluded in continuous mode" in row.group(1), rule
    assert "no data" not in table


def test_r8_continuous_aml_counts_rules_not_run():
    from lakebench.metrics.storage import MetricsStorage
    from lakebench.metrics.verdict import continuous_excluded_rules

    record = load_record("ebb26f")
    executed = set(record["experiment"]["rules"]["executed"])
    excluded = continuous_excluded_rules(
        MetricsStorage.__new__(MetricsStorage)._dict_to_metrics(record)
    )
    not_run = [r for r in excluded if r not in executed]
    panel = _section(_text("ebb26f"), "What limits interpretation", "Continuous pipeline")
    assert f"{len(not_run)} detection rules not run in continuous mode" in panel


@pytest.mark.parametrize(
    ("run_id", "must_contain", "must_not_contain"),
    [
        pytest.param(
            "65567b",
            lambda r: "bronze rows / generated corpus rows (released rows not recorded)",
            lambda r: "rows released by the window's end",
            id="ingest-basis-without-released-rows",
        ),
        pytest.param(
            "ebb26f",
            lambda r: (
                f"bronze bucket {r['bronze_size_gb']:.1f} GiB at run end: "
                "landing files plus the bronze table"
            ),
            lambda r: f"corpus {r['bronze_size_gb']:.1f} GiB",
            id="continuous-names-window-intake-not-corpus",
        ),
        pytest.param(
            "ebb26f",
            lambda r: f"commit {r['provenance']['git_sha'][:7]}, clean",
            lambda r: "uncommitted changes (dirty)",
            id="clean-tree-is-not-dirty",
        ),
    ],
)
def test_basis_labels_distinguish_what_the_number_divides(run_id, must_contain, must_not_contain):
    """A label names its basis and never the misleading neighbour."""
    record = load_record(run_id)
    text = _text(run_id)
    assert must_contain(record) in text
    assert must_not_contain(record) not in text


# ---------------------------------------------------------------------------
# R10: provenance and AML labels
# ---------------------------------------------------------------------------


def test_r10_provenance_in_the_front_panel():
    prov = load_record("1320bd")["provenance"]
    assert prov["git_dirty"] is True
    text = _text("1320bd")
    panel = _section(text, "Read this first", "Batch pipeline")
    assert (
        f"Provenance: lakebench {prov['lakebench_version']}, commit {prov['git_sha'][:7]}, dirty"
        in panel
    )
    assert "produced from a tree with uncommitted changes (dirty)" in panel


def test_r10_aml_recall_labelled_and_skips_counted():
    record = load_record("1320bd")
    skipped = record["experiment"]["rules"]["skipped"]
    check = record["financial_scoring"]["subject_customer_check"]
    text = _text("1320bd")
    panel = _section(text, "What limits interpretation", "Batch pipeline")
    names = ", ".join(f"{k} ({v})" for k, v in sorted(skipped.items()))
    assert f"{len(skipped)} detection rule(s) skipped: {names}" in panel
    assert "AML recall is uncalibrated and in-sample" in panel
    assert "Recall (uncalibrated, in-sample)" in text
    assert (
        f"Subject customer check: {check['status']} ({check['subjects']:,} planted subjects" in text
    )


@pytest.mark.parametrize("bad", ["substring", 7], ids=["string", "int"])
def test_r10_look_run_ids_must_be_a_list(monkeypatch, bad):
    """A string or integer run_ids names no run (no substring match, no
    crash); the same look with a real list does."""
    from lakebench.config import datagen_seed

    run_id = load_record("1320bd")["run_id"]
    if bad == "substring":
        bad = f"x{run_id}x"

    def looks_with(run_ids):
        monkeypatch.setattr(
            datagen_seed,
            "load_looks",
            lambda path=None: [{"role": "evaluation", "state": "complete", "run_ids": run_ids}],
        )
        return _text("1320bd")

    assert "Recall (registered look: evaluation)" in looks_with([run_id])
    text = looks_with(bad)
    assert "Recall (uncalibrated, in-sample)" in text
    assert "Recall (registered look" not in text


# ---------------------------------------------------------------------------
# R20: throughput over stage inputs, with the corpus beside it
# ---------------------------------------------------------------------------


def test_r20_throughput_label_and_corpus_size():
    record = load_record("5105a0")
    bronze = record["bronze_size_gb"]
    total = record["pipeline_benchmark"]["scores"]["total_data_processed_gb"]
    assert total > 2 * bronze  # stage inputs count the corpus once per stage
    text = _text("5105a0")
    assert "stage inputs processed per second (bronze, silver and gold reads)" in text
    assert f"{total:.1f} GiB of stage inputs; corpus {bronze:.1f} GiB in bronze" in text
    assert "data volume / wall-clock time" not in text


# ---------------------------------------------------------------------------
# R21: the bottleneck caption names what the bar measures
# ---------------------------------------------------------------------------


def _bottleneck_rows(run_id: str) -> dict[str, list[str]]:
    html = page_text(render(run_id))
    start = html.index("<h2>Bottleneck Identification</h2>")
    section = html[start : html.index("</section>", start)]
    rows = {}
    for m in re.finditer(r"<tr[^>]*>\s*<td[^>]*>(\w+)</td>(.*?)</tr>", section, flags=re.S):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", m.group(2), flags=re.S)
        rows[m.group(1)] = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
    return rows


def test_bottleneck_continuous_query_stage_has_no_latency_share():
    """The query stage has no micro-batch latency: its latency and share
    cells are empty and the latency shares are over the stages that have
    one."""
    stages = load_record("ebb26f")["pipeline_benchmark"]["stages"]
    timed = [s["stage_name"] for s in stages if (s.get("latency_ms") or 0) > 0]
    assert "query" not in timed
    rows = _bottleneck_rows("ebb26f")
    assert rows["query"][:2] == ["-", "-"]
    latency = {s["stage_name"]: s["latency_ms"] for s in stages if s["stage_name"] in timed}
    total = sum(latency.values())
    for name, ms in latency.items():
        assert rows[name][1] == f"{ms / total * 100:.1f}%"


def test_bottleneck_thrift_query_stage_not_costed_with_trino_cores():
    """233b69 ran its queries on Spark Thrift: no Trino cores are charged
    to its query stage, and the shares are over the Spark stages."""
    record = load_record("233b69")
    assert record["config_snapshot"]["query_engine"] == "spark-thrift"
    stages = record["pipeline_benchmark"]["stages"]
    spark = [s for s in stages if s["stage_name"] != "query"]
    total = sum(s["executor_count"] * s["executor_cores"] * s["elapsed_seconds"] for s in spark)
    silver = next(s for s in spark if s["stage_name"] == "silver")
    share = silver["executor_count"] * silver["executor_cores"] * silver["elapsed_seconds"] / total
    section = _section(_text("233b69"), "Bottleneck Identification", "Data Validity")
    assert f"{share * 100:.1f}%" in section
    assert "Not in the bar: query (the spark-thrift query engine records no cores)" in section
    query_row = re.search(r"query (\S+) (\S+) (\S+) (\S+)", section.split("% of requested")[-1])
    assert query_row and query_row.group(3) == "-" and query_row.group(4) == "-"


def test_r6_interrupted_run_keeps_its_verdict_word(monkeypatch):
    """When the verdict is INTERRUPTED the panel and the headline both say
    so (not FAILED) and no headline figure is shown. The verdict comes from
    compute_verdict; it is fixed here to isolate the rendering."""
    from lakebench.reports import front_matter

    reason = "Run interrupted (SIGINT during gold-finalize)"
    monkeypatch.setattr(front_matter, "page_verdict", lambda m: ("INTERRUPTED", [reason], ""))
    record = load_record("5105a0")
    record["success"] = False
    text = _plain(_render_dict(record))
    assert f"Run INTERRUPTED: {reason}" in text
    assert "Verdict: INTERRUPTED" in text
    assert "headline figures not shown: the run is INTERRUPTED" in text
