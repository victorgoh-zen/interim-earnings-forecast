import polars as pl

from datetime import date
from typing import Union
from pyspark.sql import functions as F, Window as W, types as T, DataFrame

from src import data

def update_price_model_deal_profiles(model_id: Union[int, str]) -> None:
    
    (
        data.price_model_scenario.intermittent_generation_profiles(model_id)
        .write.mode("overwrite")
        .saveAsTable("exploration.earnings_forecast.scenario_generation_profiles")
    )

    (
        # Calculae storage profiles and append to generation profiles
    )


    (
        data.price_model_scenario.load_profiles(model_id)
        .write.mode("overwrite")
        .saveAsTable("exploration.earnings_forecast.scenario_load_profiles")
    )

    return

def generate_storage_profiles(model_id: Union[int, str]):
    spot_prices = (
        data.price_model_scenario.price_simulations(model_id)
    )


    return