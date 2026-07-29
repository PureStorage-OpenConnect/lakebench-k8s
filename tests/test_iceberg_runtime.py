"""Iceberg runtime selection and the Java 17 requirement.

Both failures here arrive late and unhelpfully. A wrong runtime suffix becomes
a Maven resolution error inside the driver; a Java 11 image becomes an
UnsupportedClassVersionError at class load, long after lakebench reported a
successful submit.
"""

import pytest

from lakebench.modules.pipeline_engines.spark.job import (
    iceberg_requires_java17,
    iceberg_runtime_suffix_for,
    validate_iceberg_java_runtime,
)


class TestRuntimeSuffix:
    """The suffix depends on both Spark and Iceberg versions, not Spark alone."""

    def test_spark_41_gets_its_native_runtime_from_1_11(self):
        assert iceberg_runtime_suffix_for((4, 1), "1.11.0") == "4.1"

    def test_spark_41_still_borrows_40_on_older_iceberg(self):
        """1.10.x publishes no 4.1 runtime, so a flat flip would 404."""
        assert iceberg_runtime_suffix_for((4, 1), "1.10.1") == "4.0"
        assert iceberg_runtime_suffix_for((4, 1), "1.10.0") == "4.0"

    @pytest.mark.parametrize("iceberg", ["1.10.1", "1.11.0"])
    def test_spark_40_is_unaffected(self, iceberg):
        assert iceberg_runtime_suffix_for((4, 0), iceberg) == "4.0"

    @pytest.mark.parametrize("iceberg", ["1.9.1", "1.10.1", "1.11.0"])
    def test_spark_35_is_unaffected(self, iceberg):
        assert iceberg_runtime_suffix_for((3, 5), iceberg) == "3.5"


class TestJava17Requirement:
    """Iceberg 1.11.0 ships bytecode major 61; 1.10.x shipped 55."""

    def test_1_11_requires_java17(self):
        assert iceberg_requires_java17("1.11.0")

    @pytest.mark.parametrize("version", ["1.10.1", "1.9.1", "1.5.2"])
    def test_earlier_versions_do_not(self, version):
        assert not iceberg_requires_java17(version)

    def test_java11_spark35_image_is_refused(self):
        with pytest.raises(ValueError) as exc:
            validate_iceberg_java_runtime("apache/spark:3.5.4-python3", "1.11.0")
        message = str(exc.value)
        assert "Java 17" in message
        assert "java17" in message, "the error must name the fix, not just the fault"

    def test_java17_spark35_image_is_accepted(self):
        validate_iceberg_java_runtime("apache/spark:3.5.9-java17-python3", "1.11.0")

    def test_older_iceberg_on_java11_stays_valid(self):
        """The guard must not break the combination that works today."""
        validate_iceberg_java_runtime("apache/spark:3.5.4-python3", "1.10.1")

    @pytest.mark.parametrize("image", ["apache/spark:4.0.2-python3", "apache/spark:4.1.1-python3"])
    def test_spark_4_images_ship_java17_already(self, image):
        validate_iceberg_java_runtime(image, "1.11.0")
