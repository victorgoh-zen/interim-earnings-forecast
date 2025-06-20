import os
import pandas as pd
from datetime import date
from typing import Union
from pyspark.sql import functions as F, Window as W, DataFrame
from src import spark, data

spark.sql("USE CATALOG exploration;")
spark.sql("USE SCHEMA scenario_modelling;")


def price_simulations(model_name: str) -> DataFrame:
    """
    Retrieve price simulation data for a specific price model..
    
    Args:
        model_name (str): The name of the price model to filter simulations for.
    
    Returns:
        pyspark.sql.DataFrame
            - model_id: short
            - sample_id: short
            - interval_date: date
            - period_id: short
            - region_number: byte
            - rrp: float
    

    """
    price_models = spark.table("price_models").filter(F.col("model_name") == model_name)
    price_simulations = spark.table("price_model_simulations")

    if price_models.count() == 0:
        raise Exception(f"{model_name} is not the name of a trained price model.") 

    output = (
        price_simulations
        .join(
            price_models,
            "model_id",
            "left_semi"
        )
        .select(
            F.col("model_id"),
            F.col("sample_id"),
            F.col("interval_date"),
            F.col("period_id"),
            F.col("region_number"),
            F.col("rrp")
        )
    )

    if output.count() == 0:
        raise Exception(f"Missing price simulations or model: {model_name}.")

    return output

def region_numbers() -> DataFrame:
    """
    Retrieve region number lookup data as a broadcast variable.

    Returns:
        pyspark.sql.DataFrame: A broadcast DataFrame containing region number
        mapping information from the "region_numbers" table.
    """
    return F.broadcast(spark.table("exploration.scenario_modelling.region_numbers"))

def jurisdictions() -> DataFrame:
    return F.broadcast(spark.table("exploration.scenario_modelling.jurisdictions"))

def sample_details(model_id: Union[int, str]) -> DataFrame:
    """
    Retrieve sample configuration details for a specific price model.
    
    Args:
        model_id (str | int): The name or integer ID of the price model to get sample details for.
    
    Returns:
        pyspark.sql.DataFrame: A DataFrame containing:
            - sample_id: short
            - demand_reference_year: short
            - gen_reference_year: short
            - scheduled_avail_reference_year: short
    
    """
    price_models = spark.table("price_models")
    sample_details = spark.table("price_model_sample_details")

    if type(model_id) == int:
        output = (
            sample_details
            .filter(F.col("model_id") == model_id)
        )

    elif type(model_id) == str:
        output = (
            sample_details
            .join(
                price_models
                .filter(F.col("model_name") == model_id),
                "model_id",
                "left_semi"
            )
            
        )
    else:
        raise Exception(f"Invalid model_id: {model_id}. Must be an integer or string.")

    return (
        output
        .select(
            "model_id",
            "sample_id",
            "demand_reference_year",
            "gen_reference_year",
            "scheduled_avail_reference_year"
        )
    )

def intermittent_generation_profiles(model_id: Union[int, str]) -> DataFrame:
    """
    Retrieve intermittent generation profiles for renewable energy units (solar and wind)
    based on historical SCADA data, availability, and regional capacity factors.
    
    Args:
        model_id (Union[int, str]): The name or integer ID of the price model to get 
                                   generation profiles for.
    
    Returns:
        pyspark.sql.DataFrame: A DataFrame containing:
            - model_id: integer
            - product_id: integer  
            - year: short
            - month_id: short
            - day_id: short
            - period_id: short
            - generation_mw: float
    """
    calendar = spark.table("prod.silver.calendar_datetime")

    file_dir = os.path.dirname(os.path.abspath(__file__))
    ppa_details_file_path = os.path.join(file_dir, "..", "..", "utils", "ppa_details.csv")
    if not os.path.isfile(ppa_details_file_path):
        raise Exception(f"Missing ppa_details.csv file at {ppa_details_file_path}")

    ppa_details = F.broadcast(
        spark.createDataFrame(
            pd.read_csv(ppa_details_file_path)
        )
        .join(
            region_numbers(),
            "regionid"
        )
        .select(
            "product_id",
            "duid",
            "region_number",
            "fuel",
            "size_mw"
        )
    )

    regional_intermittent_capacity = (
        spark.table("intermittent_capacity")
        .select(
            "region_number",
            F.year("interval_date").alias("year"),
            F.month("interval_date").alias("month_id"),
            F.dayofmonth("interval_date").alias("day_id"),
            "period_id",
            "solar_avail_cf",
            "wind_avail_cf"
        )
        .unpivot(
            ids=["region_number", "year", "month_id", "day_id", "period_id"],
            values=["solar_avail_cf", "wind_avail_cf"],
            variableColumnName="column_name",
            valueColumnName="regional_capacity_factor"
        )
        .withColumn("fuel", 
            F.when(F.col("column_name") == "solar_avail_cf", "Solar")
            .when(F.col("column_name") == "wind_avail_cf", "Wind")
        )
        .select("region_number", "year", "month_id", "day_id", "period_id", "fuel", "regional_capacity_factor")
    )


    unit_scada = (
        spark.table("prod.silver_mms.dispatch_unit_scada")
        .join(
            ppa_details,
            "duid",
            "left_semi"
        )
        .withColumn(
            "period_id",
            F.when(
                (F.minute("settlementdate") == 0) &
                (F.hour("settlementdate") == 0),
                F.lit(288)
            )
            .otherwise(
                F.minute("settlementdate") / 5 + F.hour("settlementdate") * 12
            )
            .cast("integer")
        )
        .withColumn(
            "interval_date",
            F.when(
                F.col("period_id") == 288,
                F.date_sub(F.to_date("settlementdate"), 1)
            )
            .otherwise(F.to_date("settlementdate"))
        )
        .select(
            "duid",
            F.year("interval_date").alias("year"),
            F.month("interval_date").alias("month_id"),
            F.dayofmonth("interval_date").alias("day_id"),
            "period_id",
            "scadavalue"
        )
    )

    unit_availability = (
        spark.table("prod.silver_mms.dispatchload")
        .join(
            ppa_details,
            "duid",
            "left_semi"
        )
        .withColumn(
            "period_id",
            F.when(
                (F.minute("settlementdate") == 0) &
                (F.hour("settlementdate") == 0),
                F.lit(288)
            )
            .otherwise(
                F.minute("settlementdate") / 5 + F.hour("settlementdate") * 12
            )
            .cast("integer")
        )
        .withColumn(
            "interval_date",
            F.when(
                F.col("period_id") == 288,
                F.date_sub(F.to_date("settlementdate"), 1)
            )
            .otherwise(F.to_date("settlementdate"))
        )
        .select(
            "duid",
            F.year("interval_date").alias("year"),
            F.month("interval_date").alias("month_id"),
            F.dayofmonth("interval_date").alias("day_id"),
            "period_id",
            "availability"
        )
    )

    loss_factors = (
        spark.table("prod.silver_mms.dudetailsummary")
        .join(
            ppa_details,
            "duid",
            "left_semi"
        )
        .withColumn(
            "rank",
            F.row_number().over(
                W.partitionBy("duid")
                .orderBy(F.col("end_date").desc())
            )
        )
        .filter(F.col("rank") == 1)
        .select(
            "duid",
            F.col("transmissionlossfactor").cast("float"),
            F.col("distributionlossfactor").cast("float")
        )
    )

    sample_years = F.broadcast(
        sample_details(model_id)
        .select("model_id", F.col("gen_reference_year").alias("year"))
        .distinct()
    )

    if sample_years.count() == 0:
        raise Exception(f"No samples found for model_id: {model_id}")

    product_profiles = (
        calendar
        .join(
            sample_years,
            "year"
        )
        .select(
            "model_id",
            "year",
            F.col("month").alias("month_id"),
            F.col("day_id"),
            F.col("period").alias("period_id")
        )
        .crossJoin(ppa_details)
        .join(
            unit_scada,
            ["duid", "year", "month_id", "day_id", "period_id"],
            "left"
        )
        .join(
            unit_availability,
            ["duid", "year", "month_id", "day_id", "period_id"],
            "left"
        )
        .join(
            regional_intermittent_capacity,
            ["region_number", "year", "month_id", "day_id", "period_id", "fuel"],
            "left"
        )
        .join(
            loss_factors,
            "duid",
            "left"
        )
        .fillna({"transmissionlossfactor": 1.0, "distributionlossfactor": 1.0})
        .withColumn(
            "generation_mwh",
            (
                F.coalesce(
                    F.col("availability"),
                    F.col("scadavalue"),
                    F.col("regional_capacity_factor") * F.col("size_mw")
                ) * F.col("transmissionlossfactor") * F.col("distributionlossfactor")
            ).cast("float")
        )
        .dropna(subset=["generation_mwh"])
        .select(
            "model_id",
            F.col("product_id").cast("integer"),
            F.col("year").cast("short"),
            F.col("month_id").cast("short"),
            F.col("day_id").cast("short"),
            F.col("period_id").cast("short"),
            "generation_mwh"
        )
    )

    return product_profiles

def load_profiles(model_id: Union[int, str]) -> DataFrame:
    """
    Retrieve load profiles for a specific price model
    based on short-term load forecast (STLF) predictions.
    
    Args:
        model_id (Union[int, str]): The name or integer ID of a price model
    
    Returns:
        pyspark.sql.DataFrame: A DataFrame containing:
            - model_id: integer
            - product_id: integer
            - jurisdiction_id: tinyint
            - year: integer
            - month_id: integer
            - day_id: integer
            - period_id: integer
            - quantity_mwh: float
    Notes:
        - Would prefer to have some smooth interpolation from 30 min to 5 min granularity.
        - Need to work with Khai to get the stlf to run on future date features
    """
    stlf_backcast = spark.table("exploration.khai_chang.stlf_backcast_2011_2024")
    calendar = spark.table("prod.silver.calendar_datetime")
    sample_years = F.broadcast(
        sample_details(model_id)
        .select("model_id", F.col("gen_reference_year").alias("year"))
        .distinct()
    )

    if sample_years.count() == 0:
        raise Exception(f"No samples found for model_id: {model_id}")

    index = (
        calendar
        .join(
            sample_years,
            "year"
        )
        .select(
            F.col("model_id"),
            F.col("date").alias("interval_date"),
            F.col("year"),
            F.col("month").alias("month_id"),
            F.col("day_id"),
            F.col("period_30min"),
            F.col("period").alias("period_id")
        )
        .join(
            stlf_backcast.select("product_id", "jurisdiction", "interval_date").distinct(),
            "interval_date"
        )
    )

    loss_factors = (
        spark.table("prod.gold.stlf_average_loss_factors")
        .withColumn(
            "rank",
            F.row_number().over(
                W.partitionBy("product_id", "jurisdiction")
                .orderBy(F.col("published_datetime").desc())
            )
        )
        .filter(F.col("rank") == 1)
        .select(
            "product_id", 
            "jurisdiction",
            "loss_factor"
        )
    )

    load_profiles = (
        index
        .join(
            stlf_backcast
            .select(
                F.col("interval_date"),
                F.col("period_id").alias("period_30min"),
                F.col("product_id"),
                F.col("jurisdiction"),
                (F.col("quantity")*2/1_000).alias("quantity_mw")
            ),
            ["product_id", "jurisdiction", "interval_date", "period_30min"]
        )
        .join(
            loss_factors,
            ["product_id", "jurisdiction"],
            "left"
        )
        .fillna({"loss_factor": 1.0})
        .join(
            jurisdictions(),
            "jurisdiction"
        )
        .select(
            "model_id",
            F.col("product_id").cast("integer"),
            "jurisdiction_id",
            F.col("year").cast("short"),
            F.col("month_id").cast("short"),
            F.col("day_id").cast("short"),
            F.col("period_id").cast("short"),
            (F.col("quantity_mw")*F.col("loss_factor")/12).cast("float").alias("load_mwh")
        )
    )
    return load_profiles

def storage_dispatch_profiles(model_id: Union[int, str]) -> DataFrame:
    """
    Returns: pyspark.sql.DataFrame
        - product_id: intege
        - sample_id: short
        - interval_date: date
        - period_id: short
        - quantity_mw: float
    """
    ...
    return









