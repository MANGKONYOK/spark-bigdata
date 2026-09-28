import os
import sys

import pytest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, "src")
sys.path.insert(0, SRC_DIR)
# Python workers are separate processes: they only see src/ through PYTHONPATH.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [SRC_DIR, os.environ.get("PYTHONPATH")]))

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("CPE371_Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sc(spark):
    return spark.sparkContext
