from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def clean_and_aggregate_earnings(
    df: DataFrame,
    deal_info_df: DataFrame,
    curve_name: str,
    start_date: str = "2025-01-01",
    end_date: str = "2026-12-31",
    sample_filter_df: DataFrame = None,
) -> DataFrame:
    df = df.filter(
        F.col("status").isin(["Approved", "Confirmed", "Pending", "Hypothetical"])
    )

    if sample_filter_df:
        df = df.join(sample_filter_df, ["sample_id", "year", "quarter"], "left_semi")

    df = (
        df.withColumn("year", F.year("interval_date"))
        .withColumn("quarter", F.quarter("interval_date"))
        .withColumn(
            "earnings",
            F.when(
                F.col("deal_id") == 2893,
                F.col("income_amount") - F.col("cost_amount") * ((90 - 26) / 90),
            )
            .when(
                F.col("deal_id") == 495,
                -F.when(F.col("rrp") > 300, F.col("rrp") - 300).otherwise(0)
                * F.col("volume_mwh"),
            )
            .when(
                (F.col("deal_id") == 1760) & (F.col("group") == "ppa_lgc"),
                F.col("income_amount"),
            )
            .otherwise(F.col("earnings")),
        )
        .filter(F.col("interval_date").between(F.lit(start_date), F.lit(end_date)))
        .filter(~F.col("deal_id").isin([785, 786, 787]))
        .filter(
            ~(
                (F.col("group") == "BESS")
                & (F.col("quarter") == 2)
                & (F.col("year") == 2025)
            )
        )
        .join(
            deal_info_df.select("product_id", "instrument").distinct(),
            "product_id",
            "left",
        )
        .groupBy(
            "deal_id",
            "deal_name",
            "product_name",
            "instrument",
            "status",
            "buy_sell",
            "strategy",
            "regionid",
            "group",
            "year",
            "quarter",
        )
        .agg(
            F.sum("volume_mwh").alias("volume_mwh"),
            F.sum("cost_amount").alias("total_cost"),
            F.sum("income_amount").alias("total_income"),
            F.sum("earnings").alias("total_earnings"),
            F.mean("rrp").alias("twp"),
        )
        .withColumn("curve", F.lit(curve_name))
    )
    return df