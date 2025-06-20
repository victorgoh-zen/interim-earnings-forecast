from datetime import date, timedelta
from pyspark.sql import functions as F, DataFrame
from src import data

SETTLEMENT_DETAILS_TABLE = "exploration.earnings_forecast.deal_settlement_details"
RATE_CALENDAR_TABLE = "exploration.earnings_forecast.retail_rate_calendar"

def deal_settlement_details() -> DataFrame:
    """
    Consolidates deal info needed for settlement calculations from various sources
    and formats them into a single data frame.

    Deal types:
        Supported:
            - PPA (CFD and toll, energy and LGC)
            - Retail (energy and LGC)
            - Wholesale
                - Swap (Flat and profiled)
                - Cap (Flat and profiled)
        Unsupported:
            - Floors (excluding PPA floors)
            - Options
            - Wholesale LGC trades
            - Quantos
                Cap quantos on the books have been entered as vanilla caps
                so they will be present but incorrectly evaluated
            - Storage
                Logic for evaluating asset toll has already been implemented
                but need to set up storage dispatch profiles

    Args: None

    Returns: pyspark.sql.DataFrame
        - deal_id: short
        - product_id: short
        - instrument: string
        - buy_sell: string
        - region_number: short
        - start_date: date
        - end_date: date
        - quantity: double
        - price: double
        - strike: double
        - tolling_fee: double
        - floor: double
        - turndown: double
        - lgc_price: double
        - lgc_percentage: double
    """

    complete_deal_info = data.sim.complete_deal_info().cache()
    ppa_details = data.sim.ppa_details().cache()
    lgc_rates = data.sim.lgc_rates().cache()
    deal_factors = data.sim.deal_factors().cache()
    product_profiles = data.sim.product_profiles().cache()
    date_calendar = (
        data.date_calendar
        .withColumnRenamed("date", "interval_date")
        # For MtM don't need more than 3 years, but for other applications will need to add 
        # date filtering for each deal category
        .filter(F.col("interval_date").between(F.lit(date.today()), F.lit(date.today() + timedelta(days=365*4))))
        .cache()
    )

    ppa_energy_info = (
        date_calendar
        .select("interval_date")
        .join(
            ppa_details.alias("ppa_details"),
            (F.col("interval_date").between(F.col("start_date"), F.col("end_date")))
        )
        .join(
            complete_deal_info.select("product_id", "deal_id", "buy_sell", "price"),
            ["product_id", "deal_id"]
        )
        .join(
            deal_factors
            .filter(F.col("type") == "R")
            .withColumnRenamed("factor", "escalation_factor")
            .alias("escalation_factors"),
            (F.col("ppa_details.deal_id") == F.col("escalation_factors.deal_id")) &
            F.col("interval_date").between(
                F.col("escalation_factors.from_date"),
                F.col("escalation_factors.to_date")
            ),
            "left"
        )
        .join(
            deal_factors
            .filter(F.col("type") == "M")
            .withColumn(
                "days_in_period",
                F.date_diff(F.col("to_date"), F.col("from_date")) + 1
            )
            .select(
                "deal_id",
                "from_date",
                "to_date",
                (F.col("factor") / F.col("days_in_period")).alias("tolling_fee")
            )
            .alias("tolling_fees"),
            (F.col("ppa_details.deal_id") == F.col("tolling_fees.deal_id")) &
            F.col("interval_date").between(
                F.col("tolling_fees.from_date"),
                F.col("tolling_fees.to_date")
            ),
            "left"
        )
        # .join(
        #     lgc_rates,
        #     ["product_id", "interval_date"],
        #     "left"
        # )
        .fillna({"escalation_factor": 1})
        .groupBy(
            "ppa_details.deal_id",
            "ppa_details.product_id",
            "buy_sell",
            "region_number",
            (F.col("price")*F.col("escalation_factor")).cast("double").alias("price"),
            "quantity_factor",
            "floor",
            "turndown",
            # "lgc_price",
            # "lgc_percentage",
            "tolling_fee"
        )
        .agg(
            F.min("interval_date").alias("start_date"),
            F.max("interval_date").alias("end_date")
        )
        .select(
            "deal_id",
            "product_id",
            F.when(
                F.col("tolling_fee").isNotNull(),
                F.lit("asset_toll_energy")
            ).otherwise(F.lit("ppa_energy")).alias("instrument"),
            "buy_sell",
            "region_number",
            "start_date",
            "end_date",
            F.col("quantity_factor").cast("double").alias("quantity"),
            "price",
            F.lit(None).cast("double").alias("strike"),
            F.col("tolling_fee").cast("double"),
            F.col("floor").cast("double"),
            F.col("turndown").cast("double"),
            # F.when(
            #     F.col("tolling_fee").isNotNull(),
            #     F.lit(0) # Hack for TB2, would be better to have this reflected in the source table
            # )
            # .otherwise(F.col("lgc_price"))
            # .cast("double")
            # .alias("lgc_price"),
            # F.col("lgc_percentage").cast("double")
            F.lit(None).cast("double").alias("lgc_price"),
            F.lit(None).cast("double").alias("lgc_percentage")
        )
        .orderBy("deal_id", "product_id", "start_date", "end_date")
    )
    
    ppa_lgc_info = (
        date_calendar
        .select("interval_date")
        .join(
            lgc_rates
            .withColumnRenamed("product_id", "lgc_rate_product_id")
            .alias("lgc_rates"),
            [ "interval_date"]
        )
        .join(
            ppa_details.alias("ppa_details"),
            F.col("interval_date").between(F.col("start_date"), F.col("end_date")) &
            (F.col("lgc_rate_product_id") == F.col("ppa_details.product_id"))
        )
        .withColumn(
            "lgc_price",
            F.when(
                F.col("product_id") == F.lit(1255),
                F.lit(0) # Hack for TB2, would be better to have this reflected in the source table
            )
            .otherwise(F.col("lgc_price"))
            .cast("double")
        )
        .join(
            complete_deal_info.select("product_id", "deal_id", "buy_sell"),
            ["product_id", "deal_id"]
        )
        .groupBy(
            "deal_id",
            "product_id",
            "buy_sell",
            "region_number",
            "quantity_factor",
            "lgc_price",
            "lgc_percentage"
        )
        .agg(
            F.min("interval_date").alias("start_date"),
            F.max("interval_date").alias("end_date")
        )
        .select(
            "deal_id",
            "product_id",
            F.lit("generation_lgc").alias("instrument"),
            "buy_sell",
            "region_number",
            F.col("start_date"),
            F.col("end_date"),
            F.col("quantity_factor").cast("double").alias("quantity"),
            F.lit(None).cast("double").alias("price"),
            F.lit(None).cast("double").alias("strike"),
            F.lit(None).cast("double").alias("tolling_fee"),
            F.lit(None).cast("double").alias("floor"),
            F.lit(None).cast("double").alias("turndown"),
            F.col("lgc_price").cast("double"),
            F.col("lgc_percentage").cast("double")   
        )
        .dropna(subset=["lgc_price"])
    )

    retail_energy_info = (
        date_calendar
        .select("interval_date")
        .join(
            complete_deal_info
            .filter(F.col("instrument") == "Retail Customer"),
            F.col("interval_date").between(F.col("product_start_date"), F.col("product_end_date"))
        )
        .join(data.region_numbers(), "regionid")
        # .join(lgc_rates, ["product_id", "interval_date"])
        .groupBy(
            "deal_id",
            "product_id",
            "buy_sell",
            "region_number",
            # "lgc_price",
            # "lgc_percentage"
        )
        .agg(
            F.min("interval_date").alias("start_date"),
            F.max("interval_date").alias("end_date")
        )
        .select(
            "deal_id",
            "product_id",
            F.lit("retail_energy").alias("instrument"),
            "buy_sell",
            "region_number",
            F.col("start_date"),
            F.col("end_date"),
            F.lit(None).cast("double").alias("quantity"),
            F.lit(None).cast("double").alias("price"),
            F.lit(None).cast("double").alias("strike"),
            F.lit(None).cast("double").alias("tolling_fee"),
            F.lit(None).cast("double").alias("floor"),
            F.lit(None).cast("double").alias("turndown"),
            # F.col("lgc_price").cast("double"),
            # F.col("lgc_percentage").cast("double")
            F.lit(None).cast("double").alias("lgc_price"),
            F.lit(None).cast("double").alias("lgc_percentage")
        )
        .orderBy("deal_id", "product_id", "start_date", "end_date")
    )

    retail_lgc_info = (
        date_calendar
        .select("interval_date")
        .join(
            complete_deal_info
            .filter(F.col("instrument") == "Retail Customer"),
            F.col("interval_date").between(F.col("product_start_date"), F.col("product_end_date"))
        )
        .join(data.region_numbers(), "regionid")
        .join(lgc_rates, ["product_id", "interval_date"])
        .groupBy(
            "deal_id",
            "product_id",
            "buy_sell",
            "region_number",
            "lgc_price",
            "lgc_percentage"
        )
        .agg(
            F.min("interval_date").alias("start_date"),
            F.max("interval_date").alias("end_date")
        )
        .select(
            "deal_id",
            "product_id",
            F.lit("retail_lgc").alias("instrument"),
            "buy_sell",
            "region_number",
            F.col("start_date"),
            F.col("end_date"),
            F.lit(None).cast("double").alias("quantity"),
            F.lit(None).cast("double").alias("price"),
            F.lit(None).cast("double").alias("strike"),
            F.lit(None).cast("double").alias("tolling_fee"),
            F.lit(None).cast("double").alias("floor"),
            F.lit(None).cast("double").alias("turndown"),
            F.col("lgc_price").cast("double"),
            F.col("lgc_percentage").cast("double")
        )
        .orderBy("deal_id", "product_id", "start_date", "end_date")
    )

    wholesale_deal_info = (
        date_calendar
        .select("interval_date")
        .join(
            complete_deal_info
            .join(data.region_numbers(), "regionid")
            .filter(
                F.col("instrument_calculation").isin(["Swap", "Cap"]) &
                ~F.col("product_name").like("%ACCU%") & # Is there a way to positively identify energy contracts instead of exluding non-energy?
                (F.col("optionality") == "Firm")        # Would generaly prefer a more explicit way of identify contract types
                # To be implemented:
                #   - Options 3? (No options currently past Q2 25(?), and earnings methodology not specified)
                #   - Quantos 2? (Wind quanto til Aug 25, but not properly captured in Sim)
                #   - Floor 1 (Easy)
            )
            .alias("deal_info"),
            F.col("interval_date").between(F.col("product_start_date"), F.col("product_end_date"))
        )
        .join(
            deal_factors
            .filter(F.col("type") == "R")
            .withColumnRenamed("factor", "escalation_factor")
            .alias("escalation_factors"),
            (F.col("deal_info.deal_id") == F.col("escalation_factors.deal_id")) &
            F.col("interval_date").between(
                F.col("escalation_factors.from_date"),
                F.col("escalation_factors.to_date")
            ),
            "left"
        )
        .fillna({"escalation_factor": 1.0})
        .groupBy(
            "product_id",
            "instrument_calculation",
            "deal_info.deal_id",
            "buy_sell",
            "region_number",
            "quantity",
            "price",
            "strike_price"
        )
        .agg(
            F.min("interval_date").alias("start_date"),
            F.max("interval_date").alias("end_date")
        )
        .join(
            product_profiles
            .select(
                "product_id",
                F.col("profile_mw").isNotNull().alias("shape_product")
            )
            .distinct(),
            "product_id",
            "left"
        )
        .fillna({"shape_product": False})
        .select(
            "deal_id",
            "product_id",
            F.when(
                F.col("shape_product") &
                (F.col("instrument_calculation") == "Swap"),
                F.lit("profiled_energy_swap")
            ).when(
                F.col("shape_product") &
                (F.col("instrument_calculation") == "Cap"),
                F.lit("profiled_energy_cap")
            ).when(
                (~F.col("shape_product")) &
                (F.col("instrument_calculation") == "Swap"),
                F.lit("flat_energy_swap")
            ).when(
                (~F.col("shape_product")) &
                (F.col("instrument_calculation") == "Cap"),
                F.lit("flat_energy_cap")
            ).alias("instrument"),
            "buy_sell",
            "region_number",
            F.col("start_date"),
            F.col("end_date"),
            F.col("quantity").cast("double"),
            F.col("price").cast("double"),
            F.col("strike_price").cast("double").alias("strike"),
            F.lit(None).cast("double").alias("tolling_fee"),
            F.lit(None).cast("double").alias("floor"),
            F.lit(None).cast("double").alias("turndown"),
            F.lit(None).cast("double").alias("lgc_price"),
            F.lit(None).cast("double").alias("lgc_percentage")
        )
        .orderBy("deal_id", "product_id", "start_date", "end_date")
    )

    output = (
        ppa_energy_info
        .unionByName(ppa_lgc_info)
        .unionByName(retail_energy_info)
        .unionByName(retail_lgc_info)
        .unionByName(wholesale_deal_info)
    )

    return output

def update_deal_settlement_details_table() -> None:
    (
        deal_settlement_details()
        .write.format("delta").mode("overwrite")
        .saveAsTable(SETTLEMENT_DETAILS_TABLE)
    )
    return

def update_retail_rate_calendar_table() -> None:
    (
        data.sim.retail_rate_calendar()
        .filter(F.col("interval_date") >= F.lit(date.today()))
        .write.format("delta").mode("overwrite")
        .saveAsTable(RATE_CALENDAR_TABLE)
    )
    return