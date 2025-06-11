
import polars as pl
from pyspark.sql import functions as F, Window as W, DataFrame

from src import data


def find_unadjusted_mtm_price_samples(model_name: str) -> DataFrame:
    """
    Takes a price model name and finds the sample for each quarter that
    minimizes the deviation from the futures market. Returns the best
    samples.

    Args:
        model_name: str
    Return:
        DataFrame:
            - sample_id: short
            - interval_date: date
            - period_id: short
            - region_number: byte
            - rrp: float

    Note: There are futures prices for Tasmania, so the price predictions
    for Tasmania do not factor into this analysis.
    """
    price_samples = data.price_model_scenario.price_simulations(model_name)
    lgc_curve = data.market_price.lgc_prices()
    futures_prices = data.market_price.quarterly_futures_prices()
    region_numbers = data.price_model_scenario.region_numbers()

    price_samples = (
        price_samples
        .withColumn("year", F.year("interval_date"))
        .withColumn("quarter", F.quarter("interval_date"))
    )

    quarterly_sample_prices = (
        price_samples
        .join(region_numbers, "region_number")
        .join(
            lgc_curve
            .withColumnRenamed("price", "lgc_price"),
            F.col("interval_date").between(F.col("period_start"), F.col("period_end"))
        )
        .withColumn(
            "rrp_energy",
            F.when(
                F.col("rrp" < -F.col("lgc_price")),
                -F.col("lgc_price")
            )
            .when(
                F.col("rrp") > F.lit(300),
                F.lit(300)
            )
            .otherwise(F.col("rrp"))
        )
        .groupBy("sample_id", "regionid", "year", "quarter")
        .agg(F.mean("rrp_energy").alias("modelled_energy_price"))
        .join(
            futures_prices
            .select(
                F.year("period_start").alias("year"),
                F.quarter("period_start").alias("quarter"),
                "regionid",
                F.col("energy").alias("market_energy_price")
            ),
            ["year", "quarter", "regionid"]
        )
    )

    best_samples = (
        quarterly_sample_prices
        .withColumn("squared_error", (F.col("modelled_energy_price") - F.col("market_energy_price"))**2)
        .groupBy("sample_id", "year", "quarter")
        .agg(F.mean("squared_error").alias("mse"))
        .withColumn(
            "rank",
            F.row_number().over(
                W.partitionBy("year", "quarter").orderBy(F.col("mse").asc())
            )
        )
        .filter(F.col("rank") == 1)
    )

    output = (
        price_samples
        .join(
            best_samples,
            ["sample_id", "year", "quarter"],
            "left_semi"
        )
        .select(
            "sample_id",
            "interval_date",
            "period_id",
            "region_number",
            "rrp"
        )
    )

    return output

def add_cap_spikes(price_sample: DataFrame, market_price_cap=20_300) -> DataFrame:
    """
    Takes an unadjusted forward price simulation and adds cap spikes to match the futures expected payout
    """
    futures_prices = data.market_price.quarterly_futures_prices()
    region_numbers = data.price_model_scenario.region_numbers()

    (
        price_sample
        .withColumn(
            "price_rank",
            F.row_number().over(
                W.partitionBy("")
            )
        )
    )

    return

def apply_energy_adjustments(
        price_sample: DataFrame
    ) -> DataFrame:
    lgc_curve = data.market_price.lgc_prices()
    futures_prices = data.market_price.quarterly_futures_prices()
    region_numbers = data.price_model_scenario.region_numbers()


    return



