from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def filter_by_status(
    df: DataFrame,
    status_list: list = ["Approved", "Confirmed", "Pending", "Hypothetical"]
) -> DataFrame:
    return df.filter(F.col("status").isin(status_list))


def filter_deals_by_id(df: DataFrame, deal_ids: list = [785, 786, 787]) -> DataFrame:
    return df.filter(~F.col("deal_id").isin(deal_ids))


def filter_bess_by_quarter_year(df: DataFrame, quarter: int, year: int) -> DataFrame:
    return df.filter(
        ~(
            (F.col("group") == "BESS")
            & (F.col("quarter") == quarter)
            & (F.col("year") == year)
        )
    )


def apply_custom_earnings_logic(df: DataFrame) -> DataFrame:
    return df.withColumn(
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


def clean_and_aggregate_earnings(
    df: DataFrame,
    deal_info_df: DataFrame,
    curve_name: str,
    start_date: str = "2025-01-01",
    end_date: str = "2026-12-31",
    sample_filter_df: DataFrame = None,
    rank: int = 30,
    filter_status: bool = True,
    status_list: list = None,
    custom_earnings: bool = True,
    filter_deals: bool = True,
    filter_bess: bool = True,
    bess_quarter: int = 2,
    bess_year: int = 2025,
    is_cap_adjusted: bool = False,
    is_aurora: bool = False,
    agg_by_month: bool = False,
) -> DataFrame:

    if filter_status:
        df = filter_by_status(df, status_list=status_list)

    if agg_by_month:
        df = (
            df.withColumn("year", F.year("interval_date"))
            .withColumn("quarter", F.quarter("interval_date"))
            .withColumn("month", F.month("interval_date"))
        )
    else:
        df = df.withColumn("year", F.year("interval_date")).withColumn(
            "quarter", F.quarter("interval_date")
        )

    if sample_filter_df:
        filtered_sample = sample_filter_df.filter(F.col("earnings_rank") == rank)
        df = df.join(filtered_sample, ["sample_id", "year", "quarter"], "left_semi")

    if custom_earnings:
        df = apply_custom_earnings_logic(df)

    df = df.filter(F.col("interval_date").between(F.lit(start_date), F.lit(end_date)))

    if filter_deals:
        df = filter_deals_by_id(df)

    if filter_bess:
        df = filter_bess_by_quarter_year(df, bess_quarter, bess_year)

    product_instrument_df = deal_info_df.select("product_id", "instrument").distinct()
    df = df.join(product_instrument_df, "product_id", "left")

    if is_cap_adjusted:
        df = (
            df.groupBy(
                "sample_id",
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
            .groupBy(
                "deal_id",
                "deal_name",
                "status",
                "product_name",
                "instrument",
                "buy_sell",
                "strategy",
                "regionid",
                "group",
                "year",
                "quarter",
            )
            .agg(
                F.mean("volume_mwh").alias("volume_mwh"),
                F.mean("total_cost").alias("total_cost"),
                F.mean("total_income").alias("total_income"),
                F.mean("total_earnings").alias("total_earnings"),
                F.mean("twp").alias("twp"),
            )
        )
    elif is_aurora:
        groupby_cols = [
            F.col("scenario").alias("curve"),
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
        ]
        if agg_by_month:
            groupby_cols.append("month")
        else:
            groupby_cols.append("quarter")

        df = df.groupBy(groupby_cols).agg(
            F.sum("volume_mwh").alias("volume_mwh"),
            F.sum("cost_amount").alias("total_cost"),
            F.sum("income_amount").alias("total_income"),
            F.sum("earnings").alias("total_earnings"),
            F.mean("rrp").alias("twp"),
        )
    else:
        df = df.groupBy(
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
        ).agg(
            F.sum("volume_mwh").alias("volume_mwh"),
            F.sum("cost_amount").alias("total_cost"),
            F.sum("income_amount").alias("total_income"),
            F.sum("earnings").alias("total_earnings"),
            F.mean("rrp").alias("twp"),
        )

    if not is_aurora:
        df = df.withColumn("curve", F.lit(curve_name))

    return df
