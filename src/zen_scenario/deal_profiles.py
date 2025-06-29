import polars as pl
import multiprocessing

from datetime import date
from joblib import Parallel, delayed
from typing import Union
from tqdm import tqdm
from pyspark.sql import functions as F, Window as W, types as T, DataFrame

from src import data, spark
from storage_model import StorageModel

def update_deal_profiles(model_id: Union[int, str]) -> None:
    
    (
        data.zen_scenario.intermittent_generation_profiles(model_id)
        .write.mode("overwrite")
        .saveAsTable("exploration.earnings_forecast.zen_scenario_generation_profiles")
    )

    (
        data.zen_scenario.load_profiles(model_id)
        .write.mode("overwrite")
        .saveAsTable("exploration.earnings_forecast.zen_scenario_load_profiles")
    )

    return

def generate_storage_profiles(model_id: Union[int, str]):
    storage_details = data.sim.storage_details().toPandas()
    lgc_prices = data.market_price.lgc_prices()    
    num_cores = multiprocessing.cpu_count()

    model_id = data.zen_scenario.model_id(model_id)
    
    for _, row in storage_details.iterrows():
        product_id = row["product_id"]
        region_number = row["region_number"]
        duid = row["duid"]

        capacity_mw = row["capacity"]
        duration_hours = row["duration"]
        round_trip_efficiency = row["rte"]

        model = StorageModel(capacity_mw, duration_hours, round_trip_efficiency)

        spot_prices = pl.from_arrow(
            data.zen_scenario.price_simulations(model_id)
            .join(
                lgc_prices
                .withColumnRenamed("price", "lgc_price"),
                F.year("interval_date") == F.year("period_start")
            )
            .filter(F.col("region_number") == region_number)
            .select(
                "sample_id",
                "interval_date",
                "period_id",
                F.when(
                    F.col("rrp") < -F.col("lgc_price"),
                    -F.col("lgc_price")
                )
                .otherwise(F.col("rrp"))
                .alias("rrp")
            )
            .toArrow()
        )

        partitions = spot_prices.select(
            "sample_id",
            pl.col("interval_date").dt.year().alias("year")
        ).unique()

        results = Parallel(n_jobs=num_cores)(
            delayed(model.solve)(
                spot_prices.filter(
                    (pl.col("sample_id") == sample_id) &
                    (pl.col("interval_date").dt.year() == year)
                )
            ) for sample_id, year in tqdm(partitions.iter_rows(), total=partitions.height)
        )

        results = pl.concat(results)
        
        spark.sql(f"DELETE FROM exploration.earnings_forecast.zen_scenario_storage_profiles WHERE product_id = {product_id};")
        
        (
            spark.createDataFrame(results.to_arrow())
            .join(
                spark.table("prod.silver_mms.dudetailsummary")
                .filter(F.col("duid") == F.lit(duid))
                .select(
                    F.to_date("start_date").alias("start_date"),
                    F.to_date("end_date").alias("end_date"),
                    (
                        F.col("transmissionlossfactor") *
                        F.col("distributionlossfactor")
                    ).cast("float").alias("loss_factor")
                ),
                (F.col("interval_date").between(F.col("start_date"), F.col("end_date"))),
                "left"
            )
            .fillna({"loss_factor": 1.0})
            .select(
                F.lit(model_id).cast("short").alias("model_id"),
                F.lit(product_id).cast(T.IntegerType()).alias("product_id"),
                F.col("sample_id").cast(T.ShortType()),
                F.col("interval_date"),
                F.col("period_id").cast(T.ShortType()),
                F.when(
                    F.col("metered_energy_mwh") < 0,
                    F.col("metered_energy_mwh") / F.col("loss_factor")
                )
                .otherwise(F.col("metered_energy_mwh") * F.col("loss_factor"))
                .cast("float")
                .alias("generation_mwh")
            )
            .orderBy("sample_id", "product_id", "interval_date", "period_id")
            .write.mode("append")
            .saveAsTable("exploration.earnings_forecast.zen_scenario_storage_profiles")
        )

    return