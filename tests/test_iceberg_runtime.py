"""The Java 17 requirement for Iceberg 1.11.

A Java 11 image fails late and unhelpfully: an UnsupportedClassVersionError
at class load, long after lakebench reported a successful submit.
"""

import pytest

from lakebench.modules.pipeline_engines.spark.job import validate_iceberg_java_runtime


class TestJava17Requirement:
    """Iceberg 1.11.0 ships bytecode major 61; 1.10.x shipped 55."""

    def test_java17_runtime_is_required_for_iceberg_1_11(self):
        cases = [
            # (image, iceberg version, refused with "java17")
            ("apache/spark:3.5.4-python3", "1.11.0", True),  # Java 11 image, 1.11
            ("apache/spark:3.5.9-java17-python3", "1.11.0", False),  # Java 17 image
            ("apache/spark:3.5.4-python3", "1.10.1", False),  # older Iceberg stays valid
            ("apache/spark:4.0.2-python3", "1.11.0", False),  # Spark 4 ships Java 17
            ("apache/spark:4.1.1-python3", "1.11.0", False),
        ]
        for image, version, refused in cases:
            if refused:
                with pytest.raises(ValueError, match="java17"):
                    validate_iceberg_java_runtime(image, version)
            else:
                validate_iceberg_java_runtime(image, version)  # must not raise
