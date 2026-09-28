"""
House Price Analytics Module (Lab 7.2).
Answers the two Lab 2 questions on the Branch1/Branch2 house datasets with PySpark DataFrames:
  Q1  pricing summary per MSZoning
  Q2  the n highest house prices per square foot
"""

import argparse
import os
import sys
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max as spark_max,
    min as spark_min,
    round as spark_round,
)

# Default Paths & Constants
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DATA_DIR = os.path.join(ROOT_DIR, "lab+files+and+datasets")
BRANCHES: List[str] = ["Branch1", "Branch2"]
DEFAULT_TOP_N = 5
DEFAULT_SALE_CONDITION = "Normal"

# Every CSV column is read as string; these are the ones the questions compute on.
INT_COLUMNS: List[str] = ["Id", "YearBuilt", "SalePrice", "TotalBsmtSF", "1stFlrSF", "2ndFlrSF"]
DOUBLE_COLUMNS: List[str] = ["LotFrontage"]

# decimal(10,2) rather than round(): round() on a double prints 74528.0, while the
# expected tables always show two decimals (74528.00).
MONEY = "decimal(10,2)"


def create_spark_session(app_name: str = "CPE371_HouseAnalytics") -> SparkSession:
    """
    Initializes a local SparkSession for batch DataFrame work.
    """
    # Keep Python workers on this interpreter, as in log_analytics.py.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    """
    Reads one CSV with a header row. All columns arrive as string; empty cells become NULL.
    """
    return spark.read.option("header", "true").csv(path)


def load_house_data(spark: SparkSession, data_dir: str = DEFAULT_DATA_DIR) -> DataFrame:
    """
    Joins each branch's Gov and House files on Id, then stacks the branches with unionByName.
    unionByName matches columns by name, so a reordered file cannot shift values between columns.
    """
    branches = [
        read_csv(spark, os.path.join(data_dir, f"{branch}_GovDataset.csv")).join(
            read_csv(spark, os.path.join(data_dir, f"{branch}_HouseDataset.csv")), "Id", "inner"
        )
        for branch in BRANCHES
    ]
    combined = branches[0]
    for branch_df in branches[1:]:
        combined = combined.unionByName(branch_df)
    return combined


def cast_columns(df: DataFrame) -> DataFrame:
    """
    Casts the numeric string columns to int/double. Missing LotFrontage stays NULL (no dropna),
    because every house counts towards n and avg skips NULLs on its own.
    """
    for name in INT_COLUMNS:
        df = df.withColumn(name, col(name).cast("int"))
    for name in DOUBLE_COLUMNS:
        df = df.withColumn(name, col(name).cast("double"))
    return df


# ---------------------------------------------------------------------------
# Q1: pricing summary per MSZoning
# ---------------------------------------------------------------------------
def mszoning_summary(df: DataFrame) -> DataFrame:
    """
    Q1: houses, average/max/min sale price and average lot frontage per zone,
    most expensive zone (by average price) first.
    """
    return (
        df.groupBy("MSZoning")
        .agg(
            count("*").alias("n"),
            avg("SalePrice").cast(MONEY).alias("avg_price"),
            spark_max("SalePrice").alias("max_price"),
            spark_min("SalePrice").alias("min_price"),
            avg("LotFrontage").cast(MONEY).alias("avg_lot"),
        )
        .orderBy(col("avg_price").desc(), col("MSZoning"))
    )


# ---------------------------------------------------------------------------
# Q2: highest price per square foot
# ---------------------------------------------------------------------------
def with_price_per_sqft(df: DataFrame) -> DataFrame:
    """
    Adds TotalArea (basement + first + second floor, sq ft) and PricePerSqft.
    Houses with no area are dropped so the division is always defined.
    """
    return (
        df.withColumn("TotalArea", col("TotalBsmtSF") + col("1stFlrSF") + col("2ndFlrSF"))
        .filter(col("TotalArea") > 0)
        .withColumn("PricePerSqft", spark_round(col("SalePrice") / col("TotalArea"), 2).cast(MONEY))
    )


def top_price_per_sqft(
    df: DataFrame, n: int = DEFAULT_TOP_N, sale_condition: str = DEFAULT_SALE_CONDITION
) -> DataFrame:
    """
    Q2: the n houses with the highest price per square foot among `sale_condition` sales.
    Normal sales only by default: Partial sales are homes sold before completion, priced
    against an unfinished floor area, and would otherwise take over the top of the ranking.
    Pass sale_condition=None to rank every sale.
    """
    ranked = with_price_per_sqft(df)
    if sale_condition is not None:
        ranked = ranked.filter(col("SaleCondition") == sale_condition)
    return (
        ranked.select("Id", "MSZoning", "YearBuilt", "SalePrice", "TotalArea", "PricePerSqft")
        .orderBy(col("PricePerSqft").desc(), col("Id"))
        .limit(n)
    )


def run_report(
    data_dir: str = DEFAULT_DATA_DIR,
    top_n: int = DEFAULT_TOP_N,
    sale_condition: str = DEFAULT_SALE_CONDITION,
) -> None:
    """
    Prints the answers to Q1 and Q2.
    """
    spark = create_spark_session()
    try:
        df = cast_columns(load_house_data(spark, data_dir)).cache()
        print(f"[*] Loaded {df.count()} houses from {data_dir}\n")

        print("Q1: pricing summary per MSZoning")
        mszoning_summary(df).show()

        condition = sale_condition or "all"
        print(f"Q2: top {top_n} price per square foot (SaleCondition: {condition})")
        top_price_per_sqft(df, top_n, sale_condition).show()
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="PySpark DataFrame house price analytics (Lab 7.2, Lab 2 Q1-Q2)"
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help=f"Directory holding Branch{{1,2}}_{{Gov,House}}Dataset.csv (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"How many houses to list for Q2 (default: {DEFAULT_TOP_N})",
    )
    parser.add_argument(
        "--sale-condition",
        default=DEFAULT_SALE_CONDITION,
        help=f"SaleCondition to rank in Q2, or 'all' for every sale (default: {DEFAULT_SALE_CONDITION})",
    )
    args = parser.parse_args()
    sale_condition = None if args.sale_condition.lower() == "all" else args.sale_condition
    run_report(data_dir=args.data_dir, top_n=args.top, sale_condition=sale_condition)


if __name__ == "__main__":
    main()
