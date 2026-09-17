"""Per-domain scorecard extension points for the report generator.

ENG-2C.5. A ScorecardBlock owns the domain-specific parts of an HTML
scorecard: the display label that appears in the run-context banner
plus (in later revisions) any workload-specific detail rows. The block
for the run's workload schema is looked up via
:func:`get_scorecard_block`, which the report generator calls once per
render.

Only ``domain_label`` is consumed today, per the ENG-2C.5 rescope in
docs/lakebench.next-spec-addendum.md A.4. The ``render_detail_html``
hook exists so Financial can slot in Investigator-panel rows and
Customer 360 can move its existing panels off the hardcoded generator
path without another interface change.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from lakebench.metrics import PipelineMetrics


@runtime_checkable
class ScorecardBlock(Protocol):
    """Per-schema scorecard extension point.

    Implementations declare a stable ``schema_name`` slug (matching
    :class:`~lakebench.config.schema.WorkloadSchema` values) so the
    registry can be looked up by string, e.g. from a metrics.json
    ``config_snapshot`` field written by an older run.
    """

    schema_name: str
    domain_label: str

    def render_detail_html(self, metrics: PipelineMetrics) -> str:
        """Return workload-specific detail HTML for the scorecard.

        Empty string means "no extra rows"; the generator inserts it
        verbatim after the shared stage table.
        """
        ...


class Customer360ScorecardBlock:
    """Scorecard block for the Customer 360 medallion pipeline."""

    schema_name = "customer360"
    domain_label = "Customer360"

    def render_detail_html(self, metrics: PipelineMetrics) -> str:
        return ""


class FinancialScorecardBlock:
    """Scorecard block for the Financial (FinServ-Crime, AML) workload.

    Detail HTML is empty until the investigator-panel scoring surface
    from ENG-2C.4.6-7 authoring is finalised.
    """

    schema_name = "financial"
    domain_label = "Financial (FinServ-Crime, AML)"

    def render_detail_html(self, metrics: PipelineMetrics) -> str:
        return ""


_REGISTRY: dict[str, ScorecardBlock] = {
    Customer360ScorecardBlock.schema_name: Customer360ScorecardBlock(),
    FinancialScorecardBlock.schema_name: FinancialScorecardBlock(),
}


def register_scorecard_block(block: ScorecardBlock) -> None:
    """Register a ScorecardBlock, overwriting any existing entry for its schema."""
    _REGISTRY[block.schema_name] = block


def get_scorecard_block(schema_name: str | None) -> ScorecardBlock:
    """Return the ScorecardBlock for a schema.

    Unknown or missing schema names fall back to Customer 360 so older
    metrics.json files (which have no ``workload_schema`` in their
    config_snapshot) still render.
    """
    if not schema_name:
        return _REGISTRY["customer360"]
    return _REGISTRY.get(schema_name, _REGISTRY["customer360"])
