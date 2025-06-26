from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd


def extract_rez_cf(fuel_list: list, ppa_details_df: pd.DataFrame):
    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    dfs = []
    for fuel_type in fuel_list:
        # df = load_rez_table(spark, fuel_type)
        df = spark.table(f"dev.silver.aurora_rez_{fuel_type.lower()}")

        rez_list = list(
            ppa_details_df.loc[ppa_details_df["fuel"] == fuel_type, "rez"].unique()
        )
        df_unpivoted = df.unpivot(
            ids="settlementdate",
            values=rez_list,
            variableColumnName="rez",
            valueColumnName="rez_cf",
        ).withColumn("fuel", F.lit(fuel_type))
        dfs.append(df_unpivoted)

    if dfs and len(dfs) > 1:
        result_df = dfs[0]
        for df in dfs[1:]:
            result_df = result_df.unionByName(df)
    elif dfs and len(dfs) == 1:
        result_df = dfs[0]
    else:
        raise ValueError("No dataframes to union")

    result_df = prep_datetime_columns(result_df, rez=True)
    return result_df


def load_rez_table(spark, fuel_type):
    if fuel_type == "Solar":
        rez_df = spark.table("dev.silver.aurora_rez_solar")
    elif fuel_type == "Wind":
        rez_df = spark.table("dev.silver.aurora_rez_wind")
    return rez_df


def prep_datetime_columns(df, duid: bool = False, rez: bool = False):
    datetime_str = "settlementdate"
    if not rez and not duid:
        datetime_str = "interval_date_time"

    if duid:
        df = df.withColumn(
            datetime_str, F.expr(f"{datetime_str} + interval 29 minutes")
        )
    else:
        df = df.withColumn(
            datetime_str, F.expr(f"{datetime_str} + interval 30 minutes")
        )

    df = (
        df.withColumn("month_id", F.month(datetime_str))
        .withColumn("day_id", F.dayofmonth(datetime_str))
        .withColumn("period_id", F.hour(datetime_str) * 2 + F.minute(datetime_str) / 30)
    )

    if rez or duid:
        df = df.drop(datetime_str)

    return df


def calculate_amount(df, amount_col_name: str):
    if amount_col_name == "cost_amount":
        buy_sell_str = "Buy"
    elif amount_col_name == "income_amount":
        buy_sell_str = "Sell"

    df = df.withColumn(
        amount_col_name,
        F.when(
            F.col("buy_sell") == buy_sell_str,
            F.col("volume_mwh") * F.col("contract_price"),
        ).otherwise(
            F.when(
                F.col("instrument_calculation") == "Swap",
                F.col("volume_mwh") * F.col("rrp"),
            )
            .when(
                F.col("instrument_calculation") == "Cap",
                F.when(
                    F.col("rrp") > F.col("strike_price"),
                    F.col("volume_mwh") * (F.col("rrp") - F.col("strike_price")),
                ).otherwise(F.lit(0)),
            )
            .when(
                F.col("instrument_calculation") == "Floor",
                F.when(
                    F.col("rrp") < F.col("strike_price"),
                    F.col("volume_mwh") * (F.col("strike_price") - F.col("rrp")),
                ).otherwise(F.lit(0)),
            )
            .otherwise(F.lit(0))
        ),
    )

    return df


def prepare_base_dataframe(df):
    """Create a common transformation function for shared logic"""
    return (
        df.withColumn(
            "product_start_date", F.to_date("product_start_date", format="d/M/y")
        )
        .withColumn("product_end_date", F.to_date("product_end_date", format="d/M/y"))
        .withColumn("month_id", F.month("interval_date_time"))
        .withColumn("day_id", F.dayofmonth("interval_date_time"))
        .withColumn(
            "period_id",
            F.hour("interval_date_time") * 2 + F.minute("interval_date_time") / 30,
        )
    )


def standardize_df(df, group_name):
    return df.select(
        [
            "scenario",
            "group",
            "product_id",
            "product_name",
            "deal_id",
            "deal_name",
            "status",
            "deal_date",
            "strategy",
            "regionid",
            "interval_date_time",
            "interval_date",
            "buy_sell",
            "volume_mwh",
            "rrp",
            "cost_amount",
            "income_amount",
            "earnings",
        ]
    ).withColumn("group", F.lit(group_name))


def populate_aurora_table(
    retail_earnings, ppa_earnings, wholesale_earnings, templers_earnings
):
    """Populate the Aurora table with earnings data"""
