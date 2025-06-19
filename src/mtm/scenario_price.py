import polars as pl

from datetime import date
from pyspark.sql import functions as F, Window as W, types as T, DataFrame

from src import data, spark

CAP_STRIKE = 300

FUTURES_PRICES = data.market_price.quarterly_futures_prices().cache()

MTM_PRICE_TABLE = "exploration.earnings_forecast.daily_mtm_scenario_prices"

def generate_mtm_scenario_prices(model_name: str) -> None:
    f"""
    Given the name of a price model, find the quarterly samples that most closely align with 
    the futures market then apply adjustments so that the they are completely aligned with the futures market.

    Writes to the result to a `{MTM_PRICE_TABLE}`.

    Args:
        model_name: str
    Return: None
    """
    price_sample = find_unadjusted_mtm_price_samples(model_name)
    price_sample = add_cap_spikes(price_sample)
    price_sample = apply_lgc_adjustment(price_sample)
    price_sample = apply_energy_adjustment(price_sample)
    (
        spark.createDataFrame(
            price_sample.select(
                "model_id",
                "sample_id",
                "interval_date",
                "period_id",
                "region_number",
                "rrp"
            ).to_arrow(),
            schema=T.StructType([
                T.StructField("model_id", T.ShortType(), True),
                T.StructField("sample_id", T.ShortType(), True),
                T.StructField("interval_date", T.DateType(), True),
                T.StructField("period_id", T.ShortType(), True),
                T.StructField("region_number", T.ByteType(), True),
                T.StructField("rrp", T.FloatType(), True)
            ])
        )
        .orderBy("interval_date", "period_id", "region_number")
        .write.format("delta")
        .mode("overwrite").saveAsTable(MTM_PRICE_TABLE)
    )
    return



def find_unadjusted_mtm_price_samples(model_name: str) -> pl.DataFrame:
    """
    Takes a price model name and finds the sample for each quarter that
    minimizes the deviation from the futures market. Returns the best
    samples.

    Args:
        model_name: str
    Return:
        pl.DataFrame:
            - model_id: short
            - sample_id: short
            - interval_date: date
            - period_id: short
            - region_number: byte
            - rrp: float

    Note: There are no futures prices for Tasmania, so the price
    predictions for Tasmania do not factor into this analysis.
    """
    price_samples = data.price_model_scenario.price_simulations(model_name)
    lgc_curve = data.market_price.lgc_prices()
    futures_prices = data.market_price.quarterly_futures_prices()
    region_numbers = data.price_model_scenario.region_numbers()

    price_samples = (
        price_samples
        .withColumn("year", F.year("interval_date"))
        .withColumn("quarter", F.quarter("interval_date"))
        .filter(F.col("interval_date") >= F.lit(date.today()))
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
            "rrp_energy_model",
            F.when(
                F.col("rrp") < -F.col("lgc_price"),
                -F.col("lgc_price")
            )
            .when(
                F.col("rrp") > F.lit(300),
                F.lit(300)
            )
            .otherwise(F.col("rrp"))
        )
        .groupBy("sample_id", "regionid", "year", "quarter")
        .agg(F.mean("rrp_energy_model").alias("modelled_energy_price"))
        .join(
            futures_prices
            .select(
                F.year("period_start").alias("year"),
                F.quarter("period_start").alias("quarter"),
                "regionid",
                F.col("energy_boq").alias("market_energy_price")
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

    output = pl.from_arrow(
        price_samples
        .join(
            best_samples,
            ["sample_id", "year", "quarter"],
            "left_semi"
        )
        .select(
            "model_id",
            "sample_id",
            "interval_date",
            "period_id",
            "region_number",
            "rrp"
        )
        .toArrow()
    )

    return output

def add_cap_spikes(price_sample: pl.DataFrame, market_price_cap:int=20_300) -> pl.DataFrame:
    """
    Takes an unadjusted forward price simulation and adds cap spikes to match the futures expected payout.

    Compares existing cap premiums (prices above $300) with futures cap prices by quarter
    and region, then adds additional cap spikes to the highest-priced intervals until
    the modelled cap matches the market expectation.

    Args:
        price_sample: pl.DataFrame with columns:
            - model_id: short
            - sample_id: short
            - interval_date: date
            - period_id: short
            - region_number: byte
            - rrp: float
        market_price_cap: int, default 20_300
            Market price cap threshold

    Returns:
        pl.DataFrame:
            - sample_id: short
            - interval_date: date
            - period_id: short
            - region_number: byte
            - rrp: float (adjusted with cap spikes)
    """
    region_numbers = data.price_model_scenario.region_numbers()
    futures_prices = pl.from_arrow(
        data.market_price.quarterly_futures_prices()
        .join(
            region_numbers,
            "regionid"
        )
        .select(
            F.year("period_start").alias("year"),
            F.quarter("period_start").alias("quarter"),
            "region_number",
            F.col("cap_boq").alias("futures_cap")
        )
        .toArrow()
    )

    price_sample = (
        price_sample
        .with_columns(
            pl.col("interval_date").dt.year().alias("year"),
            pl.col("interval_date").dt.quarter().alias("quarter")
        )
        .with_columns(
            pl.col("rrp")
            .rank(method="ordinal", descending=True)
            .over(["year", "quarter", "region_number"])
            .alias("price_rank")
        )
    )

    existing_cap = (
        price_sample
        .join(
            futures_prices,
            ["year", "quarter", "region_number"]
        )
        .group_by(["year", "quarter", "region_number"])
        .agg(
            (pl.col("rrp").clip(pl.lit(CAP_STRIKE)) - pl.lit(CAP_STRIKE))
            .sum()
            .alias("existing_cap"),
            pl.col("futures_cap").sum().alias("futures_cap")
        )
        .with_columns(
            (pl.col("futures_cap") - pl.col("existing_cap"))
            .alias("additional_cap_required")
        )
    )

    price_sample = (
        price_sample
        .join(existing_cap, ["year", "quarter", "region_number"])
        .with_columns(
            pl.when(
                (pl.col("additional_cap_required") < pl.lit(0)) &
                (pl.col("rrp") > CAP_STRIKE)
            )
            .then(pl.lit(CAP_STRIKE))
            .otherwise(pl.col("rrp"))
            .alias("rrp"),
            pl.when(pl.col("additional_cap_required") < pl.lit(0) )
            .then(pl.col("futures_cap"))
            .otherwise(pl.col("additional_cap_required"))
            .alias("additional_cap_required")
        )
        .with_columns(
            pl.min_horizontal(
                pl.lit(market_price_cap) - pl.col("rrp"),
                pl.lit(market_price_cap) - pl.lit(CAP_STRIKE)
            ).alias("cap_headroom")
        )
        .with_columns(
            pl.col("cap_headroom")
            .cum_sum()
            .over(["year", "quarter", "region_number"], order_by="price_rank")
            .alias("cumulative_headroom")
        )
        .with_columns(
            pl.when((pl.col("additional_cap_required") - pl.col("cumulative_headroom") + pl.col("cap_headroom")) > 0)
            .then(
                pl.min_horizontal(
                    pl.col("cap_headroom"),
                    pl.col("additional_cap_required") + pl.col("cap_headroom") - pl.col("cumulative_headroom")
                )
            )
            .otherwise(pl.lit(0))
            .alias("additional_cap")
        )
        .with_columns(
            pl.when(pl.col("additional_cap") > pl.lit(0))
            .then(
                pl.max_horizontal(pl.col("rrp"), pl.lit(CAP_STRIKE)) +
                pl.col("additional_cap")
            ).otherwise(pl.col("rrp")).alias("rrp")
        )
    )

    output = (
        price_sample
        .select(
            "model_id",
            "sample_id",
            "interval_date",
            "period_id",
            "region_number",
            "rrp"
        )
    )
    return output

def apply_lgc_adjustment(price_sample: pl.DataFrame):
    """
    Applies Large-scale Generation Certificate (LGC) price adjustments to price samples.
    
    Sets a floor price based on the negative LGC price for each year. When the RRP
    is below the negative LGC price, it is adjusted to the negative LGC price floor.

    Args:
        price_sample: pl.DataFrame with columns:
            - sample_id: short
            - interval_date: date
            - period_id: short
            - region_number: byte
            - rrp: float

    Returns:
        pl.DataFrame:
            - model_id: short
            - sample_id: short
            - interval_date: date
            - period_id: short
            - region_number: byte
            - rrp: float (adjusted with LGC floor)
    """
    lgc_curve = pl.from_arrow(
        data.market_price.lgc_prices()
        .select(
            F.year("period_start").alias("year"),
            F.col("price").alias("lgc_price")
        )
        .toArrow()
    )

    output = (
        price_sample
        .with_columns(pl.col("interval_date").dt.year().alias("year"))
        .join(
            lgc_curve,
            "year"
        )
        .select(
            "model_id",
            "sample_id",
            "interval_date",
            "period_id",
            "region_number",
            pl.col("rrp").clip(-pl.col("lgc_price")).alias("rrp")
        )
    )

    return output


def apply_energy_adjustment(
        price_sample: pl.DataFrame,
        max_iterations: int=10
    ) -> pl.DataFrame:
    f"""
    Iteratively adjusts energy prices to match futures market expectations.
    
    Applies a scaling factor to prices between 0 and {CAP_STRIKE} to align
    the average quarterly prices with futures energy prices. The adjustment
    is applied iteratively to converge on the target prices.
    
    Args:
        price_sample: pl.DataFrame with columns:
            - sample_id: short
            - interval_date: date
            - period_id: short
            - region_number: byte
            - rrp: float
        max_iterations: int, default 10
            Maximum number of iterative adjustments to apply
            
    Returns:
        pl.DataFrame:
            - sample_id: short
            - interval_date: date
            - period_id: short
            - region_number: byte
            - rrp: float (adjusted prices)
    """
    region_numbers = data.price_model_scenario.region_numbers()
    futures_prices = pl.from_arrow(
        data.market_price.quarterly_futures_prices()
        .join(
            region_numbers,
            "regionid"
        )
        .select(
            F.year("period_start").alias("year"),
            F.quarter("period_start").alias("quarter"),
            "region_number",
            F.col("energy_boq").alias("futures_energy")
        )
        .toArrow()
    )

    while True:
        mid_price_adjustment = (
            price_sample
            .group_by([
                pl.col("interval_date").dt.year().alias("year"),
                pl.col("interval_date").dt.quarter().alias("quarter"),
                pl.col("region_number")
            ])
            .agg(
                pl.col("rrp").clip(pl.lit(0), pl.lit(CAP_STRIKE)).mean().alias("mid_prices"),
                pl.col("rrp").clip(upper_bound=pl.lit(0)).mean().alias("neg_prices")
            )
            .join(
                futures_prices,
                ["year", "quarter", "region_number"]
            )
            .select(
                "year",
                "quarter",
                "region_number",
                ((pl.col("futures_energy") - pl.col("neg_prices"))/pl.col("mid_prices")).alias("adjustment_factor"),
                (pl.col("futures_energy") - pl.col("neg_prices") - pl.col("mid_prices")).alias("error")
            )
        )

        if mid_price_adjustment["error"].abs().mean() < 0.001:
            break

        price_sample = (
            price_sample
            .with_columns(
                pl.col("interval_date").dt.year().alias("year"),
                pl.col("interval_date").dt.quarter().alias("quarter")
            )
            .join(
                mid_price_adjustment,
                ["year", "quarter", "region_number"]
            )
            .select(
                "model_id",
                "sample_id",
                "interval_date",
                "period_id",
                "region_number",
                pl.when(
                    pl.col("rrp").is_between(pl.lit(0), pl.lit(CAP_STRIKE))
                )
                .then(
                    (pl.col("rrp")*pl.col("adjustment_factor")).clip(upper_bound=pl.lit(CAP_STRIKE))
                )
                .otherwise(pl.col("rrp"))
                .alias("rrp")
            )
        )

    return price_sample



