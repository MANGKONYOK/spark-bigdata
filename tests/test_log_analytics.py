"""Lab 7.1 (Lab 1 Q1-Q3) checks against a synthetic log and the real spark.log."""

import os

import pytest

import log_analytics as la

SAMPLE_LOG = [
    "17/06/09 20:10:40 INFO spark.SecurityManager: Changing view acls to: yarn,curi",
    "17/06/09 20:10:40 INFO spark.SecurityManager: Changing modify acls to: yarn,curi",
    "17/06/09 20:10:45 INFO executor.Executor: Running task 0.0 in stage 0.0 (TID 0)",
    "17/06/09 20:10:45 INFO executor.Executor: Running task 1.0 in stage 0.0 (TID 1)",
    "17/06/09 20:10:46 INFO executor.Executor: Running task 0.0 in stage 1.0 (TID 2)",
    "17/06/09 20:10:48 INFO python.PythonRunner: Times: total = 40, boot = 10, init = 29, finish = 1",
    "17/06/09 20:10:48 INFO python.PythonRunner: Times: total = 41, boot = -5, init = 45, finish = 1",
    "17/06/09 20:10:48 INFO python.PythonRunner: Times: total = 42, boot = 12, init = 29, finish = 1",
    "17/06/09 20:10:48 INFO python.PythonRunner: Times: total = 39, boot = 11, init = 27, finish = 1",
    "17/06/09 20:10:48 INFO python.PythonRunner: Times: total = 900, boot = 800, init = 99, finish = 1",
    "17/06/09 20:10:49 INFO executor.Executor: Finished task 0.0 in stage 0.0 (TID 0). 2128 bytes result sent to driver",
    "17/06/09 20:10:49 INFO executor.Executor: Finished task 0.0 in stage 1.0 (TID 2). 2128 bytes result sent to driver",
    "",
    "truncated",
]


@pytest.fixture(scope="module")
def sample_rdd(sc):
    # Same filter as load_log, applied to in-memory lines (blank and truncated lines dropped)
    return sc.parallelize(SAMPLE_LOG).filter(lambda line: len(line.split()) >= 4)


@pytest.fixture(scope="module")
def real_rdd(sc):
    if not os.path.exists(la.DEFAULT_LOG_PATH):
        pytest.skip("spark.log not present")
    return la.load_log(sc).cache()


def test_parse_source_strips_colon():
    assert la.parse_source(SAMPLE_LOG[0]) == ("spark.SecurityManager", "Changing view acls to: yarn,curi")


def test_q1_counts_sorted_desc(sample_rdd):
    assert la.source_counts(sample_rdd) == [
        ("executor.Executor", 5),
        ("python.PythonRunner", 5),
        ("spark.SecurityManager", 2),
    ]


def test_q2_parse_times_allows_negative_boot():
    assert la.parse_python_times(SAMPLE_LOG[6]) == {"total": 41, "boot": -5, "init": 45, "finish": 1}
    assert la.parse_python_times("no timings here") is None


def test_q2_statistics_and_outlier(sample_rdd):
    stats = la.python_worker_statistics(sample_rdd)["total"]
    assert (stats["count"], stats["min"], stats["max"]) == (5, 39, 900)
    assert stats["mean"] == 212.4
    assert (stats["q1"], stats["median"], stats["q3"]) == (40.0, 41.0, 42.0)
    assert stats["outliers"] == 1
    outliers = la.python_worker_outliers(sample_rdd)
    assert [(o["total"], o["severity"]) for o in outliers] == [(900, "severe")]
    assert la.negative_boot_count(sample_rdd) == 1


def test_q3_lost_tasks_and_stages(sample_rdd):
    lost = la.lost_tasks(sample_rdd)
    assert [(t["tid"], t["task"], t["stage"]) for t in lost] == [(1, "1.0", "0.0")]
    assert la.stage_task_summary(sample_rdd) == [("0.0", 2, 1, 1), ("1.0", 1, 1, 0)]
    assert la.lost_stages(sample_rdd) == ["0.0"]


def test_severity_split_uses_median_multiple():
    outliers = [{"severity": "mild"}, {"severity": "severe"}, {"severity": "mild"}]
    assert la.outlier_severity_counts(outliers) == {"severe": 1, "mild": 2}
    assert la.outlier_severity_counts([]) == {"severe": 0, "mild": 0}


def test_format_table_matches_spark_style():
    assert la.format_table(["a", "bb"], [(1, 22)]).splitlines() == [
        "+-+--+", "|a|bb|", "+-+--+", "|1|22|", "+-+--+",
    ]


# ---- real spark.log: the answers reported in the Lab 7.1 notebook ----------

def test_real_q1(real_rdd):
    counts = dict(la.source_counts(real_rdd))
    assert real_rdd.count() == 2000
    assert sum(counts.values()) == 2000
    assert len(counts) == 18
    assert counts["executor.Executor"] == 606
    assert counts["python.PythonRunner"] == 375
    assert counts["spark.SecurityManager"] == 6


def test_real_q2(real_rdd):
    total = la.python_worker_statistics(real_rdd)["total"]
    assert (total["count"], total["min"], total["max"], total["mean"]) == (375, 37, 1114, 55.66)
    assert total["outliers"] == 46
    outliers = la.python_worker_outliers(real_rdd)
    assert la.outlier_severity_counts(outliers) == {"severe": 5, "mild": 41}
    assert sorted(o["total"] for o in outliers if o["severity"] == "severe") == [1072, 1074, 1077, 1078, 1114]
    assert max(o["total"] for o in outliers if o["severity"] == "mild") == 108
    assert la.negative_boot_count(real_rdd) == 169


def test_real_q3(real_rdd):
    lost = la.lost_tasks(real_rdd)
    assert [t["tid"] for t in lost] == [1350, 1351, 1352, 1353, 1354]
    assert {t["stage"] for t in lost} == {"29.0"}
    assert la.lost_stages(real_rdd) == ["29.0"]
    assert len(la.stage_task_summary(real_rdd)) == 30
