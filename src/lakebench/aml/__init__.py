"""AML (Financial-crime / AML) library.

Pure-Python code that runs on the CLI side, on the Spark driver or in plain
tests, and does not import pyspark at module level:

- :mod:`scoring_queries`: assembles the recall and precision record from the
  scoring SQL run on Trino.
- :mod:`rule_meta`: the rule metadata (priority cutoffs, rule-to-typology
  map, customer-scoped rules) shared with the Spark detection rules.
- :mod:`fidelity_gate`: the pre-registered AML fidelity gate (AP per
  typology, D5 shortcuts, band and Level-2 summary, D2, D11), reading every
  constant from ``aml_preregistration.json``.
- :mod:`reference_score`: the band leakage gate.
- :mod:`scale_invariance`, :mod:`d8_shards`, :mod:`predictions`: D8 scale
  invariance, its shard plan, and the Level-2 predictions from the
  calibration runs.
- :mod:`look_guard`: refuses the protected evaluation and robustness corpora
  outside their registered look.
"""
