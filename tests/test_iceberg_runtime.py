"""The Java 17 requirement for Iceberg 1.11.

A Java 11 image fails late and unhelpfully: an UnsupportedClassVersionError
at class load, long after lakebench reported a successful submit.
"""

import pytest

from lakebench.modules.pipeline_engines.spark.job import validate_iceberg_java_runtime


class TestJava17Requirement:
    """Iceberg 1.11.0 ships bytecode major 61; 1.10.x shipped 55."""

    def test_java11_spark35_image_is_refused(self):
        with pytest.raises(ValueError, match="java17"):
            validate_iceberg_java_runtime("apache/spark:3.5.4-python3", "1.11.0")

    def test_java17_spark35_image_is_accepted(self):
        validate_iceberg_java_runtime("apache/spark:3.5.9-java17-python3", "1.11.0")

    def test_older_iceberg_on_java11_stays_valid(self):
        """The guard must not break the combination that works today."""
        validate_iceberg_java_runtime("apache/spark:3.5.4-python3", "1.10.1")

    @pytest.mark.parametrize("image", ["apache/spark:4.0.2-python3", "apache/spark:4.1.1-python3"])
    def test_spark_4_images_ship_java17_already(self, image):
        validate_iceberg_java_runtime(image, "1.11.0")
