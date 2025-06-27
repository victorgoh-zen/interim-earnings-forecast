import polars as pl

from pyspark.sql import functions as F
from src.earnings import settlement_calculations as calcs
from src import data, spark


MTM_EARNINGS_TABLE = "exploration.earnings_forecast.daily_mtm_scenario_earnings"

def calculate_earnings_forecast() -> None:
    """
    Performs earnings calcluations on the MtM earnings scenario writes out the result.
    """
    # Deal details
    deal_settlement_details = pl.from_arrow(data.earnings.deal_settlement_details().toArrow())

    # Price data
    spot_prices = pl.from_arrow(data.earnings.daily_mtm_scenario_prices().toArrow())
    lgc_prices = pl.from_arrow(
        data.market_price.lgc_prices()
        .join(
            data.date_calendar,
            F.col("date").between(F.col("period_start"), F.col("period_end"))
        )
        .select(
            F.col("date").alias("interval_date"),
            F.col("price").alias("lgc_price")
        )
        .toArrow()
    )

    # Generation data
    generation_profiles = pl.from_arrow(data.earnings.daily_mtm_scenario_generation_profiles().toArrow())
    generation_profiles = apply_generation_turndown(
        turndowns=(
            deal_settlement_details
            .filter(pl.col("turndown").is_not_null())
            .select(
                "product_id",
                "region_number",
                "start_date",
                "end_date",
                "turndown"
            )
        ),
        generation_profiles=generation_profiles,
        spot_prices=spot_prices
    )


    # Retail data
    load_profiles = pl.from_arrow(data.earnings.daily_mtm_scenario_load_profiles().toArrow())
    rate_calendar = pl.from_arrow(data.earnings.retail_rate_calendar().toArrow())

    # Wholesale product profiles
    product_profiles = pl.from_arrow(data.earnings.daily_mtm_scenario_product_profiles().toArrow())

    # Calculations
    earnings_df = pl.concat([
        calcs.ppa_energy(
            deal_info=(
                deal_settlement_details
                .filter(
                    pl.col("instrument") == "ppa_energy"
                )
                .select(
                    "deal_id",
                    "product_id",
                    "instrument_id",
                    "start_date",
                    "end_date",
                    "region_number",
                    "floor",
                    "turndown",
                    "price",
                    pl.col("quantity").alias("quantity_factor")   
                )
            ),
            spot_prices=spot_prices,
            generation_profiles=generation_profiles
        ),

        calcs.asset_toll_energy(
            deal_info=(
                deal_settlement_details
                .filter(pl.col("instrument") == "asset_toll_energy")
                .select(
                    "deal_id",
                    "product_id",
                    "instrument_id",
                    "start_date",
                    "end_date",
                    "region_number",
                    "tolling_fee",
                    pl.col("quantity").alias("quantity_factor")
                )
            ),
            spot_prices=spot_prices,
            generation_profiles=generation_profiles
        ),

        calcs.generation_lgc(
            deal_info=(
                deal_settlement_details
                .filter(pl.col("instrument") == "generation_lgc")
                .select(
                    "deal_id",
                    "product_id",
                    "instrument_id",
                    "start_date",
                    "end_date",
                    "region_number",
                    pl.col("lgc_price").alias("price"),
                    pl.col("quantity").alias("quantity_factor"),
                    "lgc_percentage"
                )
            ),
            generation_profiles=generation_profiles,
            lgc_prices=lgc_prices
        ),

        calcs.retail_energy(
            deal_info=(
                deal_settlement_details
                .filter(pl.col("instrument") == "retail_energy")
                .select(
                    "deal_id",
                    "product_id",
                    "instrument_id",
                    "start_date",
                    "end_date"
                )
            ),
            load_profiles=load_profiles,
            spot_prices=spot_prices,
            rate_calendar=rate_calendar
        ),

        calcs.retail_lgc(
            deal_info=(
                deal_settlement_details
                .filter(pl.col("instrument") == "retail_lgc")
                .select(
                    "deal_id",
                    "product_id",
                    "instrument_id",
                    "start_date",
                    "end_date",
                    "lgc_percentage",
                    pl.col("lgc_price").alias("price")
                )
            ),
            load_profiles=load_profiles,
            lgc_prices=lgc_prices
        ),

        calcs.flat_energy_swap(
            deal_info=(
                deal_settlement_details
                .filter(pl.col("instrument") == "flat_energy_swap")
                .select(
                    "deal_id",
                    "product_id",
                    "instrument_id",
                    "start_date",
                    "end_date",
                    "region_number",
                    pl.col("quantity").alias("quantity_mw"),
                    "price"
                )
            ),
            spot_prices=spot_prices
        ),

        calcs.flat_energy_cap(
            deal_info=(
                deal_settlement_details
                .filter(pl.col("instrument") == "flat_energy_cap")
                .select(
                    "deal_id",
                    "product_id",
                    "instrument_id",
                    "start_date",
                    "end_date",
                    "region_number",
                    pl.col("quantity").alias("quantity_mw"),
                    "price",
                    "strike"
            )
            ),
            spot_prices=spot_prices
        ),

        calcs.profiled_energy_swap(
            deal_info=(
                deal_settlement_details
                .filter(pl.col("instrument") == "profiled_energy_swap")
                .select(
                    "deal_id",
                    "product_id",
                    "instrument_id",
                    "start_date",
                    "end_date",
                    "region_number",
                    pl.col("quantity").alias("quantity_mw"),
                    "price"
                )
            ),
            product_profiles=product_profiles,
            spot_prices=spot_prices
        ),

        calcs.profiled_energy_cap(
            deal_info=(
                deal_settlement_details
                .filter(pl.col("instrument") == "profiled_energy_cap")
                .select(
                    "deal_id",
                    "product_id",
                    "instrument_id",
                    "start_date",
                    "end_date",
                    "region_number",
                    pl.col("quantity").alias("quantity_mw"),
                    "price"
                )
            ),
            product_profiles=product_profiles,
            spot_prices=spot_prices
        )

    ])

    (
        spark.createDataFrame(
            earnings_df
            .join(
                deal_settlement_details
                .select(
                    "deal_id",
                    "product_id",
                    "buy"
                )
                .unique(),
                ["deal_id", "product_id"]
            )
            .select(
                "deal_id",
                "product_id",
                "instrument_id",
                "region_number",
                "buy",
                "interval_date",
                "period_id",
                "volume_mwh",
                pl.when(pl.col("buy"))
                .then(pl.col("buy_income"))
                .otherwise(pl.col("sell_income"))
                .alias("income"),
                pl.when(pl.col("buy"))
                .then(pl.col("sell_income"))
                .otherwise(pl.col("buy_income"))
                .alias("cost")
            )
            .to_arrow()
        )
        .write.mode("overwrite")
        .saveAsTable(MTM_EARNINGS_TABLE)
    )

    return

def apply_generation_turndown(
    turndowns: pl.DataFrame,
    generation_profiles: pl.DataFrame,
    spot_prices: pl.DataFrame
):
    """
    Args:
        turndowns: pl.DataFrame
            - product_id: int16
            - region_number: int8
            - start_date: date
            - end_date: date
            - turndown: float64
        generation_profiles: pl.DataFrame
            - product_id: int16
            - interval_date: date
            - period_id: int16
            - generation_mwh: float
        spot_prices:
            - region_number: int8
            - interval_date: date
            - period_id: int16
            - rrp: float
    """
    result = (
        generation_profiles
        .join(
            turndowns,
            "product_id",
            "left"
        )
        .filter(
            (pl.col("interval_date").is_between(pl.col("start_date"), pl.col("end_date"))) |
            pl.col("turndown").is_null()
        )
        .join(
            spot_prices,
            ["interval_date", "period_id", "region_number"],
            "left"
        )
        .select(
            "product_id",
            "interval_date",
            "period_id",
            pl.when(
                (pl.col("turndown").is_not_null()) &
                (pl.col("rrp") < pl.col("turndown"))
            )
            .then(pl.lit(0).cast(pl.Float32()))
            .otherwise(pl.col("generation_mwh"))
            .alias("generation_mwh")
        )
    )

    return result