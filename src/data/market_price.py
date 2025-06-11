from datetime import date
from pyspark.sql import functions as F, Window as W, DataFrame
from src import spark

def quarterly_futures_prices() -> DataFrame:
    """
    Retrieve current quarterly futures prices for Swap and Cap products.

    Filters HVB market price data for quarterly flat load products with end dates
    in the future, deduplicates by latest consolidation date, and pivots
    by product type.
    
    Returns:
        DataFrame:
            - regionid: string
            - period: string
            - period_start: date
            - period_end: date
            - swap: float
            - cap: float
            - energy: float
    """
    hvb_prices = spark.table("prod.silver.market_price_hvb_price")

    output = (
        hvb_prices
        .filter(
            (F.col("product").isin(["Swap", "Cap"])) &
            (F.col("period_end") >= date.today()) &
            (F.col("period").like("Q%")) &
            (F.col("load") == "Flat")
        )
        .withColumn(
            "row_number", 
            F.row_number().over(
                W.partitionBy("product", "period", "node")
                .orderBy(F.col("consolidate_date").desc())
            )
        )
        .filter(F.col("row_number") == 1)
        .groupBy(
            "node",
            "period",
            "period_start",
            "period_end"
        )
        .pivot("product", ["Swap", "Cap"])
        .agg(F.mean("price").cast("float"))
        .withColumn(
            "energy",
            F.col("Swap") - F.col("Cap")
        )
        .select(
            F.concat(F.col("node"), F.lit(1)).alias("regionid"),
            F.col("period"),
            F.col("period_start"),
            F.col("period_end"),
            F.col("Swap").alias("swap"),
            F.col("Cap").alias("Cap"),
            F.col("energy")
        )
    )

    return output

def lgc_prices() -> DataFrame:
    """
    Retrieve current Large-scale Generation Certificate (LGC) prices.

    Filters HVB market price data for LGC products with end dates in the future,
    excluding spot prices, and deduplicates by latest consolidation date.
    
    Returns:
        DataFrame:
            - period: string
            - period_start: date
            - period_end: date
            - price: float
    """
    hvb_prices = spark.table("prod.silver.market_price_hvb_price")

    output = (
        hvb_prices
        .filter(
            (F.col("product") == "LGC") &
            (~F.col("period").ilike("spot")) &
            (F.col("period_end") >= date.today())
        )
        .withColumn(
            "row_number",
            F.row_number().over(
                W.partitionBy("product", "period")
                .orderBy(F.col("consolidate_date").desc())
            )
        )
        .filter(F.col("row_number") == 1)
        .select(
            "period",
            "period_start",
            "period_end",
            "price"
        )
    )

    return output