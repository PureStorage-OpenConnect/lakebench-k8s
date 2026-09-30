"""AML (Financial-crime / AML) library.

Pure-Python helpers used by the Spark scoring scripts. Kept out of
``spark/scripts/`` because those live inside the Spark image and are
executed with `spark-submit`; the code here runs on the driver or in
plain-Python tests, and does not import pyspark at module level.

Current members:

- :mod:`reference_score` -- leakage-gate computation and reference-
  detector (scikit-learn GBT) training/evaluation. Used by
  ``spark/scripts/score_financial_reference.py`` and directly tested
  in ``tests/test_aml_reference_score.py``.
- :mod:`fidelity_gate` -- the pre-registered AML fidelity gate (AP per
  typology, D5 shortcuts, band and Level-2 summary, D2, D11), reading every
  constant from ``aml_preregistration.json``. Used by the same Spark script
  (silver) and by ``scripts/aml_gate.py`` (bronze, local).
"""
