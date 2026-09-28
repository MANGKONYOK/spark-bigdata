"""Lab 7.2 (Lab 2 Q1-Q2) checks against synthetic CSVs and the real house datasets."""

import os
from decimal import Decimal

import pytest

import house_analytics as ha

GOV_HEADER = "Id,LotFrontage,MSZoning,YearBuilt,SaleCondition,SalePrice,Label"
HOUSE_HEADER = "Id,TotalBsmtSF,1stFlrSF,2ndFlrSF"

SYNTHETIC = {
    "Branch1_GovDataset.csv": [
        GOV_HEADER,
        "1,60.0,RL,2000,Normal,100000,Y",
        "2,,RL,1990,Normal,250000,Y",       # missing LotFrontage: counted in n, skipped by avg
        "3,50.0,RM,1950,Partial,90000,Y",
    ],
    "Branch1_HouseDataset.csv": [HOUSE_HEADER, "1,500,500,0", "2,0,1000,1000", "3,300,0,0"],
    # Columns deliberately reordered: unionByName must still line them up.
    "Branch2_GovDataset.csv": [
        "Id,MSZoning,LotFrontage,YearBuilt,SaleCondition,SalePrice,Label",
        "4,RM,70.0,1960,Normal,90000,Y",
        "5,RL,90.0,1970,Normal,50000,Y",
        "6,RM,40.0,1980,Normal,80000,Y",
    ],
    "Branch2_HouseDataset.csv": [HOUSE_HEADER, "4,200,200,200", "5,0,500,0", "6,0,0,0"],
}


@pytest.fixture(scope="module")
def synthetic_df(spark, tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("house")
    for name, lines in SYNTHETIC.items():
        (data_dir / name).write_text("\n".join(lines) + "\n")
    return ha.cast_columns(ha.load_house_data(spark, str(data_dir))).cache()


@pytest.fixture(scope="module")
def real_df(spark):
    if not os.path.exists(os.path.join(ha.DEFAULT_DATA_DIR, "Branch1_GovDataset.csv")):
        pytest.skip("house CSVs not present in lab+files+and+datasets/")
    return ha.cast_columns(ha.load_house_data(spark)).cache()


def test_load_joins_and_unions_by_name(synthetic_df):
    rows = {r.Id: r for r in synthetic_df.collect()}
    assert sorted(rows) == [1, 2, 3, 4, 5, 6]
    assert rows[4].MSZoning == "RM" and rows[4].LotFrontage == 70.0
    assert rows[2].LotFrontage is None


def test_cast_columns_types(synthetic_df):
    types = dict(synthetic_df.dtypes)
    assert types["SalePrice"] == "int" and types["TotalBsmtSF"] == "int"
    assert types["LotFrontage"] == "double"
    assert types["MSZoning"] == "string"


def test_q1_summary(synthetic_df):
    rows = [r.asDict() for r in ha.mszoning_summary(synthetic_df).collect()]
    assert rows == [
        {"MSZoning": "RL", "n": 3, "avg_price": Decimal("133333.33"), "max_price": 250000,
         "min_price": 50000, "avg_lot": Decimal("75.00")},   # (60 + 90) / 2: NULL skipped
        {"MSZoning": "RM", "n": 3, "avg_price": Decimal("86666.67"), "max_price": 90000,
         "min_price": 80000, "avg_lot": Decimal("53.33")},
    ]


def test_q2_ranking_filter_and_ties(synthetic_df):
    # Id 3 (Partial, 300/sqft) and Id 6 (no floor area) must not appear.
    # Ids 1 and 5 tie at 100.00: the lower Id comes first.
    rows = ha.top_price_per_sqft(synthetic_df, n=10).collect()
    assert [(r.Id, r.TotalArea, r.PricePerSqft) for r in rows] == [
        (4, 600, Decimal("150.00")),
        (2, 2000, Decimal("125.00")),
        (1, 1000, Decimal("100.00")),
        (5, 500, Decimal("100.00")),
    ]
    assert [r.Id for r in ha.top_price_per_sqft(synthetic_df, n=2).collect()] == [4, 2]


def test_q2_all_sales(synthetic_df):
    ids = [r.Id for r in ha.top_price_per_sqft(synthetic_df, n=10, sale_condition=None).collect()]
    assert ids == [3, 4, 2, 1, 5]


# ---- real datasets: the tables in Doc 7.2 section 6 -------------------------

EXPECTED_Q1 = [
    ("FV", 65, "214014.06", 370878, 144152, "59.49"),
    ("RL", 1151, "191004.99", 755000, 39300, "74.68"),
    ("RH", 16, "131558.38", 200000, 76000, "58.92"),
    ("RM", 218, "126316.83", 475000, 37900, "52.37"),
    ("C (all)", 10, "74528.00", 133900, 34900, "69.70"),
]

EXPECTED_Q2 = [
    (533, "RL", 1955, 107500, 827, "129.99"),
    (393, "RL", 1959, 106500, 882, "120.75"),
    (534, "RL", 1946, 39300, 334, "117.66"),
    (186, "RM", 1892, 475000, 4143, "114.65"),
    (873, "RL", 1953, 116000, 1015, "114.29"),
]


def test_real_row_count(real_df):
    assert real_df.count() == 1460
    assert real_df.filter("LotFrontage IS NULL").count() == 259


def test_real_q1(real_df):
    rows = [tuple(str(v) if isinstance(v, Decimal) else v for v in r)
            for r in ha.mszoning_summary(real_df).collect()]
    assert rows == EXPECTED_Q1


def test_real_q2(real_df):
    rows = [tuple(str(v) if isinstance(v, Decimal) else v for v in r)
            for r in ha.top_price_per_sqft(real_df).collect()]
    assert rows == EXPECTED_Q2


def test_real_q2_without_filter_is_led_by_partial_sales(real_df):
    ids = [r.Id for r in ha.top_price_per_sqft(real_df, sale_condition=None).collect()]
    assert ids[:2] == [689, 899] and 804 in ids
