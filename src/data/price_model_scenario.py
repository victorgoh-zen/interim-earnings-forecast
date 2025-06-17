
from datetime import date
from pyspark.sql import functions as F, Window as W, DataFrame
from src import spark

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
    return F.broadcast(spark.table("region_numbers"))

def sample_details(model_name):
    """
    Retrieve sample configuration details for a specific price model.
    
    Args:
        model_name (str): The name of the price model to get sample details for.
    
    Returns:
        pyspark.sql.DataFrame: A DataFrame containing:
            - sample_id: short
            - demand_reference_year: short
            - gen_reference_year: short
            - scheduled_avail_reference_year: short
    
    """
    price_models = spark.table("price_models")
    sample_details = spark.table("price_model_sample_details")

    output = (
        sample_details
        .join(
            price_models
            .filter(F.col("model_name") == model_name),
            "model_id",
            "left_semi"
        )
        .select(
            "sample_id",
            "demand_reference_year",
            "gen_reference_year",
            "scheduled_avail_reference_year"
        )
    )

    return output
