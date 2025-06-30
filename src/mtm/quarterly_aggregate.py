from datetime import date
from pyspark.sql import functions as F, types as T
from src import data, spark

QUARTERLY_AGG_TABLE = "exploration.earnings_forecast.mtm_scenario_quarterly_aggregate_earnings"

def update_quarterly_aggregate():

    TODAY_DATE_STRING =  date.today().strftime("%Y-%m-%d")
    spark.sql(f"DELETE FROM {QUARTERLY_AGG_TABLE} WHERE run_date = '{TODAY_DATE_STRING}';")

    (
        data.earnings.daily_mtm_scenario_earnings()
        .groupBy(
            F.lit(date.today()).cast(T.DateType()).alias("run_date"),
            F.col("product_id"),
            F.col("deal_id"),
            F.col("instrument_id"),
            F.col("region_number"),
            F.col("buy"),
            F.year("interval_date").cast(T.ShortType()).alias("calendar_year"),
            F.quarter("interval_date").cast(T.ByteType()).alias("calendar_quarter")
        )
        .agg(
            F.sum(F.col("volume_mwh")).cast(T.FloatType()).alias("volume_mwh"),
            F.sum(F.col("income")).cast(T.FloatType()).alias("income"),
            F.sum(F.col("cost")).cast(T.FloatType()).alias("cost")
        )
        .write.mode("append")
        .saveAsTable(QUARTERLY_AGG_TABLE)
    )
