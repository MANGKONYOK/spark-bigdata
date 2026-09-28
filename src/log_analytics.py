"""
Spark Log Analytics Module (Lab 7.1).
Answers the three Lab 1 questions on spark.log with PySpark RDDs:
  Q1  how many times each source appears in the log
  Q2  statistics (min, max, avg, outliers) of the python worker (python.PythonRunner)
  Q3  which tasks or stages were lost (executor.Executor)
"""

import argparse
import os
import re
import sys
from typing import Dict, List, Optional, Tuple

from pyspark import RDD, SparkContext
from pyspark.sql import SparkSession

# Default Paths & Constants
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_LOG_PATH = os.path.join(ROOT_DIR, "spark.log")
PYTHON_RUNNER_SOURCE = "python.PythonRunner"
EXECUTOR_SOURCE = "executor.Executor"
TIME_FIELDS: List[str] = ["total", "boot", "init", "finish"]
OUTLIER_IQR_FACTOR = 1.5
# An IQR outlier is "severe" when it is more than this many times the median, otherwise "mild".
# The IQR of 'total' is only 3 ms, so the fences flag many runs that are merely a little slow;
# the severity split separates those from the real ~1.1 s cold starts.
SEVERE_MEDIAN_FACTOR = 10.0

# Line layout: "17/06/09 20:10:40 INFO executor.Executor: Running task 0.0 in stage 0.0 (TID 0)"
#               date     time     level source               message
TIMES_PATTERN = re.compile(
    r"Times: total = (-?\d+), boot = (-?\d+), init = (-?\d+), finish = (-?\d+)"
)
TASK_PATTERN = re.compile(
    r"(Running|Finished) task (\d+\.\d+) in stage (\d+\.\d+) \(TID (\d+)\)"
)


def create_spark_context(app_name: str = "CPE371_LogAnalytics") -> SparkContext:
    """
    Initializes a local SparkSession and returns its SparkContext for RDD work.
    """
    # RDD lambdas run in separate Python workers; pin them to this interpreter so a
    # different system python3 is never picked up (version mismatch, missing packages).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark.sparkContext


def load_log(sc: SparkContext, log_path: str = DEFAULT_LOG_PATH) -> RDD:
    """
    Reads the log as an RDD of lines, dropping blank or truncated lines that have no source field.
    """
    return sc.textFile(log_path).filter(lambda line: len(line.split()) >= 4)


def parse_source(line: str) -> Tuple[str, str]:
    """
    Splits one log line into (source, message). The trailing ':' on the source is removed.
    """
    parts = line.split()
    return parts[3].rstrip(":"), " ".join(parts[4:])


# ---------------------------------------------------------------------------
# Q1: occurrences per source
# ---------------------------------------------------------------------------
def source_counts(log_rdd: RDD) -> List[Tuple[str, int]]:
    """
    Q1: counts log lines per source with map + reduceByKey, most frequent first.
    Ties are broken by source name so the output order is deterministic.
    """
    return (
        log_rdd.map(lambda line: (parse_source(line)[0], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda kv: (-kv[1], kv[0]))
        .collect()
    )


# ---------------------------------------------------------------------------
# Q2: python worker statistics
# ---------------------------------------------------------------------------
def parse_python_times(line: str) -> Optional[Dict[str, int]]:
    """
    Extracts {total, boot, init, finish} in milliseconds from a PythonRunner 'Times:' message.
    """
    match = TIMES_PATTERN.search(line)
    if not match:
        return None
    return dict(zip(TIME_FIELDS, (int(v) for v in match.groups())))


def python_runner_times(log_rdd: RDD) -> RDD:
    """
    Filters python.PythonRunner lines and parses each into a dict of timings.
    """
    return (
        log_rdd.filter(lambda line: parse_source(line)[0] == PYTHON_RUNNER_SOURCE)
        .map(parse_python_times)
        .filter(lambda times: times is not None)
    )


def _quantile(sorted_values: List[int], q: float) -> float:
    """
    Linear-interpolated quantile, matching numpy's default method.
    """
    pos = (len(sorted_values) - 1) * q
    low = int(pos)
    high = min(low + 1, len(sorted_values) - 1)
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (pos - low)


def field_statistics(values_rdd: RDD, iqr_factor: float = OUTLIER_IQR_FACTOR) -> Dict[str, float]:
    """
    Computes count/min/max/mean/stdev with RDD.stats(), then quartiles and IQR outlier fences.
    A value is an outlier when it falls outside [Q1 - k*IQR, Q3 + k*IQR].
    """
    stats = values_rdd.stats()
    sorted_values = values_rdd.sortBy(lambda v: v).collect()
    q1 = _quantile(sorted_values, 0.25)
    median = _quantile(sorted_values, 0.50)
    q3 = _quantile(sorted_values, 0.75)
    iqr = q3 - q1
    lower, upper = q1 - iqr_factor * iqr, q3 + iqr_factor * iqr
    outliers = values_rdd.filter(lambda v: v < lower or v > upper).count()
    return {
        # StatCounter returns numpy floats when numpy is installed; take min/max from the
        # sorted values instead so integer millisecond fields stay plain ints.
        "count": int(stats.count()),
        "min": sorted_values[0],
        "max": sorted_values[-1],
        "mean": round(float(stats.mean()), 2),
        "stdev": round(float(stats.stdev()), 2),
        "q1": q1,
        "median": median,
        "q3": q3,
        "lower_fence": lower,
        "upper_fence": upper,
        "outliers": outliers,
    }


def python_worker_statistics(
    log_rdd: RDD, iqr_factor: float = OUTLIER_IQR_FACTOR
) -> Dict[str, Dict[str, float]]:
    """
    Q2: statistics for each timing field of the python worker, keyed by field name.
    """
    times_rdd = python_runner_times(log_rdd).cache()
    result = {
        field: field_statistics(times_rdd.map(lambda t, f=field: t[f]), iqr_factor)
        for field in TIME_FIELDS
    }
    times_rdd.unpersist()
    return result


def python_worker_outliers(
    log_rdd: RDD,
    field: str = "total",
    iqr_factor: float = OUTLIER_IQR_FACTOR,
    severe_factor: float = SEVERE_MEDIAN_FACTOR,
) -> List[Dict]:
    """
    Lists the PythonRunner records whose `field` is an IQR outlier, slowest first.
    Each record gets a 'severity': 'severe' above severe_factor x median, otherwise 'mild'.
    """
    times_rdd = python_runner_times(log_rdd)
    stats = field_statistics(times_rdd.map(lambda t: t[field]), iqr_factor)
    lower, upper = stats["lower_fence"], stats["upper_fence"]
    severe_above = severe_factor * abs(stats["median"])
    return (
        times_rdd.filter(lambda t: t[field] < lower or t[field] > upper)
        .map(lambda t: dict(t, severity="severe" if abs(t[field]) > severe_above else "mild"))
        .sortBy(lambda t: -t[field])
        .collect()
    )


def outlier_severity_counts(outliers: List[Dict]) -> Dict[str, int]:
    """
    Counts outliers per severity, always reporting both keys.
    """
    counts = {"severe": 0, "mild": 0}
    for record in outliers:
        counts[record["severity"]] += 1
    return counts


def negative_boot_count(log_rdd: RDD) -> int:
    """
    Counts records with a negative boot time. The python daemon reuses an already-running
    worker, so boot is not a real duration there; these rows are data anomalies, not fast boots.
    """
    return python_runner_times(log_rdd).filter(lambda t: t["boot"] < 0).count()


# ---------------------------------------------------------------------------
# Q3: lost tasks and stages
# ---------------------------------------------------------------------------
def parse_task_event(line: str) -> Optional[Tuple[int, Tuple[str, str, str, str]]]:
    """
    Parses an executor task event into (tid, (event, task, stage, timestamp)).
    """
    match = TASK_PATTERN.search(line)
    if not match:
        return None
    event, task, stage, tid = match.groups()
    timestamp = " ".join(line.split()[:2])
    return int(tid), (event, task, stage, timestamp)


def lost_tasks(log_rdd: RDD) -> List[Dict[str, str]]:
    """
    Q3: a task is lost when the executor logged 'Running task' for a TID but never
    'Finished task' for it. Implemented as a subtractByKey of finished TIDs from started TIDs.
    """
    events = (
        log_rdd.filter(lambda line: parse_source(line)[0] == EXECUTOR_SOURCE)
        .map(parse_task_event)
        .filter(lambda e: e is not None)
        .cache()
    )
    started = events.filter(lambda e: e[1][0] == "Running")
    finished = events.filter(lambda e: e[1][0] == "Finished")
    lost = (
        started.subtractByKey(finished)
        .map(lambda e: {
            "tid": e[0],
            "task": e[1][1],
            "stage": e[1][2],
            "started_at": e[1][3],
        })
        .sortBy(lambda t: t["tid"])
        .collect()
    )
    events.unpersist()
    return lost


def stage_task_summary(log_rdd: RDD) -> List[Tuple[str, int, int, int]]:
    """
    Per stage: (stage, started, finished, lost), ordered by stage number.
    A stage with lost > 0 never completed on this executor.
    """
    counts = (
        log_rdd.filter(lambda line: parse_source(line)[0] == EXECUTOR_SOURCE)
        .map(parse_task_event)
        .filter(lambda e: e is not None)
        .map(lambda e: (e[1][2], (1, 0) if e[1][0] == "Running" else (0, 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .map(lambda kv: (kv[0], kv[1][0], kv[1][1], kv[1][0] - kv[1][1]))
        .sortBy(lambda row: float(row[0]))
        .collect()
    )
    return counts


def lost_stages(log_rdd: RDD) -> List[str]:
    """
    Stages that have at least one lost task.
    """
    return [stage for stage, _, _, lost in stage_task_summary(log_rdd) if lost > 0]


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------
def format_table(headers: List[str], rows: List[Tuple]) -> str:
    """
    Renders rows as a Spark-style '+---+' ASCII table, numbers right-aligned.
    """
    cells = [[str(v) for v in row] for row in rows]
    widths = [max([len(h)] + [len(r[i]) for r in cells]) for i, h in enumerate(headers)]
    border = "+" + "+".join("-" * w for w in widths) + "+"
    lines = [border, "|" + "|".join(h.rjust(w) for h, w in zip(headers, widths)) + "|", border]
    for row in cells:
        lines.append("|" + "|".join(v.rjust(w) for v, w in zip(row, widths)) + "|")
    lines.append(border)
    return "\n".join(lines)


def run_report(log_path: str = DEFAULT_LOG_PATH, iqr_factor: float = OUTLIER_IQR_FACTOR) -> None:
    """
    Prints the answers to Q1-Q3.
    """
    sc = create_spark_context()
    try:
        log_rdd = load_log(sc, log_path).cache()
        print(f"[*] Loaded {log_rdd.count()} log lines from {log_path}\n")

        print("Q1: occurrences per source")
        print(format_table(["source", "count"], source_counts(log_rdd)))

        print(f"\nQ2: {PYTHON_RUNNER_SOURCE} timings in ms (outliers: {iqr_factor} x IQR)")
        stats = python_worker_statistics(log_rdd, iqr_factor)
        stat_names = ["count", "min", "max", "mean", "stdev", "q1", "median", "q3",
                      "lower_fence", "upper_fence", "outliers"]
        print(format_table(
            ["field"] + stat_names,
            [tuple([f] + [stats[f][s] for s in stat_names]) for f in TIME_FIELDS],
        ))
        print(f"[!] {negative_boot_count(log_rdd)} records have a negative boot time (reused worker)")
        outliers = python_worker_outliers(log_rdd, "total", iqr_factor)
        severity = outlier_severity_counts(outliers)
        print(f"\nQ2: {len(outliers)} outlier records on 'total': {severity['severe']} severe "
              f"(> {SEVERE_MEDIAN_FACTOR:g} x median), {severity['mild']} mild")
        if outliers:
            print(format_table(
                TIME_FIELDS + ["severity"],
                [tuple(o[f] for f in TIME_FIELDS + ["severity"]) for o in outliers],
            ))

        print(f"\nQ3: lost tasks ({EXECUTOR_SOURCE}: 'Running' with no matching 'Finished')")
        lost = lost_tasks(log_rdd)
        print(format_table(
            ["tid", "task", "stage", "started_at"],
            [(t["tid"], t["task"], t["stage"], t["started_at"]) for t in lost],
        ))
        summary = stage_task_summary(log_rdd)
        print(f"Q3: lost stages: {', '.join(lost_stages(log_rdd)) or 'none'} "
              f"({len(summary)} stages seen)")
    finally:
        sc.stop()


def main():
    parser = argparse.ArgumentParser(
        description="PySpark RDD log analytics for spark.log (Lab 7.1, Lab 1 Q1-Q3)"
    )
    parser.add_argument(
        "--log-path",
        default=DEFAULT_LOG_PATH,
        help=f"Path to the Spark executor log (default: {DEFAULT_LOG_PATH})",
    )
    parser.add_argument(
        "--iqr-factor",
        type=float,
        default=OUTLIER_IQR_FACTOR,
        help=f"IQR multiplier for outlier fences (default: {OUTLIER_IQR_FACTOR})",
    )
    args = parser.parse_args()
    run_report(log_path=args.log_path, iqr_factor=args.iqr_factor)


if __name__ == "__main__":
    main()
