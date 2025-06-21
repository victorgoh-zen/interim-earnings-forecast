from pyspark.sql import functions as F, DataFrame
from src import data, spark

spark.sql("USE CATALOG exploration;")
spark.sql("USE SCHEMA earnings_forecast;")

def instruments() -> DataFrame:
    return spark.table("instruments")

def deal_settlement_details() -> DataFrame:
    return (
        spark.table("deal_settlement_details")
        .join(
            instruments(),
            "instrument"
        )
    )

def retail_rate_calendar() -> DataFrame:
    return spark.table("retail_rate_calendar")

def scenario_generation_profiles() -> DataFrame:
    return spark.table("scenario_generation_profiles")

def scenario_load_profiles() -> DataFrame:
    return spark.table("scenario_load_profiles")

def daily_mtm_scenario_prices() -> DataFrame:
    return spark.table("daily_mtm_scenario_prices")

def daily_mtm_scenario_generation_profiles() -> DataFrame:
    """
    Creates generation profiles for MtM scenario.

    Args: None

    Returns: pyspark.sql.DataFrame
        - product_id: string
        - interval_date: date
        - period_id: int
        - generation_mwh: float 
    """
    sample_years = (
        spark.table("exploration.scenario_modelling.price_model_sample_details")
        .select(
            "model_id",
            "sample_id",
            "gen_reference_year"
        )
    )

    sample_index = (
        daily_mtm_scenario_prices()
        .select(
            "model_id",
            "sample_id",
            "interval_date",
            F.month("interval_date").alias("month_id"),
            F.dayofmonth("interval_date").alias("day_id"),
            F.col("period_id")
        )
        .distinct()
        .join(
            sample_years,
            ["model_id", "sample_id"]
        )
    )



    output = (
        scenario_generation_profiles()
        .withColumnRenamed("year", "gen_reference_year")
        .join(
            sample_index,
            ["model_id", "gen_reference_year", "month_id", "day_id", "period_id"]
        )
        .select(
            "product_id",
            "interval_date",
            "period_id",
            "generation_mwh"
        )
    )

    if output.count() == 0:
        raise Exception("Could not create scenario generation profiles. Earnings forecast scenario tables likely out of sync.")

    return output

def daily_mtm_scenario_load_profiles() -> DataFrame:
    """
    Creates load profiles for MtM scenario.

    Args: None

    Returns: pyspark.sql.DataFrame
        - product_id: short
        - jurisdiction_id: byte
        - region_number: byte
        - interval_date: date
        - period_id: short
        - load_mwh: float
    """
    sample_years = (
        spark.table("exploration.scenario_modelling.price_model_sample_details")
        .select(
            "model_id",
            "sample_id",
            "demand_reference_year"
        )
    )

    sample_index = (
        daily_mtm_scenario_prices()
        .select(
            "model_id",
            "sample_id",
            "interval_date",
            F.month("interval_date").alias("month_id"),
            F.dayofmonth("interval_date").alias("day_id"),
            F.col("period_id")
        )
        .distinct()
        .join(
            sample_years,
            ["model_id", "sample_id"]
        )
    )

    output = (
        scenario_load_profiles()
        .withColumnRenamed("year", "demand_reference_year")
        .join(
            sample_index,
            ["model_id", "demand_reference_year", "month_id", "day_id", "period_id"]
        )
        .join(data.jurisdictions(), "jurisdiction_id")
        .select(
            "product_id",
            "jurisdiction_id",
            "region_number",
            "interval_date",
            "period_id",
            "load_mwh"
        )
    )

    return output

def daily_mtm_scenario_product_profiles() -> DataFrame:
    products = (
        deal_settlement_details()
        .filter(F.col("instrument").isin(["profiled_energy_swap",  "profiled_energy_cap"]))
        .select("product_id")
        .distinct()
    )

    date_index = (
        daily_mtm_scenario_prices()
        .select("interval_date")
        .distinct()
    )

    output = (
        data.sim.product_profiles()
        .join(products, "product_id", "left_semi")
        .join(date_index, "interval_date", "left_semi")
        .select(
            "product_id",
            "interval_date",
            "period_id",
            F.col("profile_mw").cast("float").alias("volume_factor"),
            F.lit(1).cast("float").alias("price_factor"),
            F.col("profile_strike").cast("float").alias("strike")
        )
    )

    return output


    
