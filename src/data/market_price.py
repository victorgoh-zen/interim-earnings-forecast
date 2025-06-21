from datetime import date, timedelta
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
            - swap_boq: float
            - cap_boq: float
            - energy_boq: float
    """
    hvb_prices = spark.table("prod.silver.market_price_hvb_price")
    spot_prices = spark.table("prod.silver_mms.tradingprice")
    calendar = spark.table("prod.silver.calendar_date")

    today = date.today()
    current_year = today.year 
    current_quarter = (today.month - 1) // 3 + 1

    qtd_prices = (
        spot_prices
        .withColumn(
            "interval_date",
            F.when(
                (F.hour("settlementdate") == 0) &
                (F.minute("settlementdate") == 0),
                F.date_sub(F.to_date("settlementdate"), 1)
            )
            .otherwise(F.to_date("settlementdate"))
        )
        .withColumn("year", F.year("interval_date"))
        .withColumn("quarter", F.quarter("interval_date"))
        .filter(
            (F.col("interval_date") < F.lit(today)) &
            (F.col("year") == F.lit(current_year)) &
            (F.col("quarter") == F.lit(current_quarter))
        )
        .groupBy(
            "year",
            "quarter",
            "regionid"
        )
        .agg(
            F.mean(F.col("rrp")).alias("qtd_swap_payout"),
            F.mean(
                F.when(
                    F.col("rrp") > 300,
                    F.col("rrp") - F.lit(300)
                )
                .otherwise(F.lit(0))
            ).alias("qtd_cap_payout"),
            F.countDistinct("interval_date").alias("days_qtd")
        )
        .join(
            calendar
            .groupBy("year", "quarter")
            .agg(F.countDistinct("date").alias("days_in_quarter")),
            ["year", "quarter"]
        )
    )

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
            "period_end",
            "consolidate_date"
        )
        .pivot("product", ["Swap", "Cap"])
        .agg(F.mean("price").cast("float"))
        .withColumn("regionid",  F.concat(F.col("node"), F.lit(1)))
        .withColumn("quarter", F.quarter("period_start"))
        .withColumn("year", F.year("period_start"))
        .join(
            qtd_prices,
            ["regionid", "quarter", "year"],
            "left"
        )
        .withColumn(
            "swap_boq",
            F.when(
                F.col("qtd_swap_payout").isNotNull(),
                (F.col("Swap") - F.col("qtd_swap_payout")*F.col("days_qtd")/F.col("days_in_quarter"))
                * F.col("days_in_quarter") / (F.col("days_in_quarter") - F.col("days_qtd"))
            )
            .otherwise(F.col("Swap"))
        )
        .withColumn(
            "cap_boq",
            F.when(
                F.col("qtd_cap_payout").isNotNull(),
                (F.col("Cap") - F.col("qtd_cap_payout")*F.col("days_qtd")/F.col("days_in_quarter"))
                * F.col("days_in_quarter") / (F.col("days_in_quarter") - F.col("days_qtd"))
            )
            .otherwise(F.col("cap"))
        )
        .withColumn(
            "energy",
            F.col("Swap") - F.col("Cap")
        )
        .withColumn(
            "energy_boq",
            F.col("swap_boq") - F.col("cap_boq")
        )
        .select(
            F.col("regionid"),
            F.col("period"),
            F.col("period_start"),
            F.col("period_end"),
            F.col("Swap").alias("swap"),
            F.col("Cap").alias("cap"),
            F.col("energy"),
            F.col("swap_boq"),
            F.col("cap_boq"),
            F.col("energy_boq")
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