import os
import pandas as pd

from datetime import date
from pyspark.sql import functions as F, Window as W, DataFrame
from src import spark, data

def complete_deal_info() -> DataFrame:
    """
    Joins deal info from numerous Sim tables into a single DataFrame.
    """
    output = (
        spark.table("prod.bronze.etrm_sim_legs").alias("legs")
        .join(
            spark.table("prod.bronze.etrm_sim_deals").alias("deals"), 
            F.col("legs.dealid") == F.col("deals.id"),
            "left_outer"
        )
        .join(
            spark.table("prod.bronze.etrm_sim_products").alias("products"), 
            F.col("legs.productid") == F.col("products.id"),
            "left_outer"
        )
        .join(
            spark.table("prod.bronze.etrm_sim_books").alias("books"), 
            F.col("deals.bookid") == F.col("books.id"), 
            "left_outer"
        )
        .join(
            spark.table("prod.bronze.etrm_sim_strategies").alias("strategies"), 
            F.col("deals.strategyid") == F.col("strategies.id"), 
            "left_outer"
        )
        .join(
            spark.table("prod.bronze.etrm_sim_companies").alias("companies"), 
            F.col("deals.companyid") == F.col("companies.id"), 
            "left_outer"
        )
        .join(
            spark.table("prod.bronze.etrm_sim_companies").alias("entities"), 
            F.col("deals.tradingentityid") == F.col("entities.id"), 
            "left_outer"
        )
        .join(
            spark.table("prod.bronze.etrm_sim_calendars").alias("calendars"), 
            F.col("deals.calendarid") == F.col("calendars.id"), 
            "left_outer"
        )
        .join(
            spark.table("prod.bronze.etrm_sim_companies").alias("brokers"), 
            F.col("deals.brokerid") == F.col("brokers.id"), 
            "left_outer"
        )
        .join(
            spark.table("prod.bronze.etrm_sim_companies").alias("clearers"), 
            F.col("deals.clearerid") == F.col("clearers.id"), 
            "left_outer"
        )
        .join(
            spark.table("prod.bronze.etrm_sim_instruments").alias("instruments"), 
            F.col("products.instrumentid") == F.col("instruments.id"), 
            "left_outer"
        )
        .join(
            spark.table("prod.bronze.etrm_sim_markets").alias("markets"), 
            F.col("instruments.marketid") == F.col("markets.id"), 
            "left_outer"
        )
        .join(
            spark.table("prod.bronze.etrm_sim_nodes").alias("nodes"), 
            F.col("products.nodeid") == F.col("nodes.id"), 
            "left_outer"
        )
        .filter(F.col("books.active") == "Y")
        .select(
            F.col("deals.id").cast("short").alias("deal_id"),
            F.col("deals.name").alias("deal_name"),
            F.row_number().over(
                W.partitionBy("deals.id").orderBy("legs.transferdate")
            ).alias("leg_number"),
            F.col("products.id").cast("short").alias("product_id"),
            F.col("products.name").alias("product_name"),
            F.col("deals.buysell").alias("buy_sell"),
            F.to_date("deals.dealdate").alias("deal_date"),
            F.col("deals.status").alias("status"),
            F.col("products.schedule").alias("product_schedule"),
            F.to_date("products.startdate").alias("product_start_date"),
            F.to_date("products.enddate").alias("product_end_date"),
            F.col("products.term").alias("product_term"),
            F.col("deals.optionality").alias("optionality"),
            F.col("deals.optioncode").alias("option_code"),
            F.col("deals.optiontype").alias("option_type"),
            F.col("deals.optionprice").alias("option_price"),
            F.col("legs.quantity").alias("quantity"),
            F.col("legs.price").alias("price"),
            F.col("legs.brokerage").alias("brokerage"),
            F.col("products.strike").alias("strike_price"),
            F.col("nodes.code").alias("regionid"),
            F.substring(
                F.col("nodes.code"), 
                1, 
                F.length(F.col("nodes.code")) - 1
            ).alias("region"),
            F.col("books.name").alias("book"),
            F.col("strategies.name").alias("strategy"),
            F.col("entities.name").alias("trading_entity"),
            F.col("companies.name").alias("counterparty"),
            F.col("brokers.name").alias("broker"),
            F.col("clearers.name").alias("clearer"),
            F.col("calendars.name").alias("settlement_calendar"),
            F.col("instruments.name").alias("instrument"),
            F.col("instruments.calculation").alias("instrument_calculation"),
            F.col("markets.name").alias("market"),
            F.to_date("legs.transferdate").alias("transfer_date"),
            F.to_date("legs.paymentdate").alias("payment_date")
        )
    )
    return output

def deal_factors():
    sim_deal_factors = spark.table("prod.bronze.etrm_sim_deal_factors")

    output = (
        sim_deal_factors
        .select(
            F.col("dealid").cast("short").alias("deal_id"),
            F.col("type"),
            F.col("fromdate").cast("date").alias("from_date"),
            F.lead(
                F.date_sub(F.col("fromdate"), 1),
                1,
                date(9999, 12, 31)
            )
            .over(
                W.partitionBy("dealid", "type").orderBy("fromdate")
            ).alias("to_date"),
            "factor"
        )
    )

    return output

###############################################################################################
##################### DON'T ALLOW THIS TO BECOME A PERMANENT SOLUTION -_- #####################
#####################   NEED A BETTER PLACE FOR CAPTURING THESE DETAILS   #####################
###############################################################################################
def ppa_details():
    file_dir = os.path.dirname(os.path.abspath(__file__))
    ppa_details_file_path = os.path.join(file_dir, "..", "..", "supplementary_deal_capture", "ppa_details.csv")
    if not os.path.isfile(ppa_details_file_path):
        raise Exception(f"Missing ppa_details.csv file at {ppa_details_file_path}")
    
    ppa_details = (
        spark.createDataFrame(pd.read_csv(
            ppa_details_file_path,
            parse_dates=["product_start_date", "product_end_date"],
            date_format="%d/%m/%Y"
        ))
        .join(data.region_numbers(), "regionid")
        .select(
            F.col("deal_id").cast("short"),
            F.col("product_id").cast("short"),
            F.col("name"),
            F.col("duid"),
            F.col("fuel"),
            F.col("floor").cast("double"),
            F.col("turndown").cast("double"),
            F.col("size_mw").cast("double"),
            F.col("rez"),
            F.col("regionid"),
            F.col("region_number"),
            F.to_date("product_start_date").alias("start_date"),
            F.to_date("product_end_date").alias("end_date"),
            F.col("quantity_factor").cast("double")
        )
    )
    return F.broadcast(ppa_details)

def storage_details() -> DataFrame:
    file_dir = os.path.dirname(os.path.abspath(__file__))
    storage_details_file_path = os.path.join(file_dir, "..", "..", "supplementary_deal_capture", "storage_details.csv")
    if not os.path.isfile(storage_details_file_path):
        raise Exception(f"Missing `storage_details.csv` file at {storage_details_file_path}")

    output = (
        spark.createDataFrame(pd.read_csv(
            storage_details_file_path,
            parse_dates=["start_date", "end_date"],
            date_format="%Y-%m-%d"
        ))
        .join(data.region_numbers(), "regionid")
        .select(
            F.col("deal_id").cast("short"),
            F.col("product_id").cast("short"),
            F.col("name"),
            F.col("duid"),
            F.col("buy_sell"),
            F.col("regionid"),
            F.col("region_number"),
            F.col("start_date").cast("date"),
            F.col("end_date").cast("date"),
            F.col("capacity").cast("double"),
            F.col("duration").cast("double"),
            F.col("rte").cast("double"),
            F.col("toll").cast("double")
        )
    )

    return output

###############################################################################################
###############################################################################################
###############################################################################################
###############################################################################################

def calendar() -> DataFrame:
    jurisdictions = data.jurisdictions()

    output = (
        spark.table("prod.silver.calendar_datetime")
        .crossJoin(jurisdictions)
        .join(
            spark.table("prod.silver.calendar_public_holiday")
            .select(
                "date",
                "jurisdiction",
                "holiday_name"
            ),
            ["date", "jurisdiction"],
            "left"
        )
        .select(
            F.col("date").alias("interval_date"),
            F.col("jurisdiction_id"),
            F.col("period").cast("short").alias("period_id"),
            F.col("day_of_week"),
            F.col("holiday_name").isNotNull().alias("public_holiday")
        )
        .withColumn(
            "day_type",
            F.when(
                F.col("public_holiday") | (F.col("day_of_week") == 7),
                F.lit("S")
            ).when(
                F.col("day_of_week") == 6,
                F.lit("6")
            ).otherwise(F.lit("W"))
        )
        .distinct()
    )

    return output

def retail_rates() -> DataFrame:
    """
    Pulls rates from sim profiles table.
    """

    # There must be a better way to do this
    product_jurisdictions = (
        spark.table("prod.silver.stlf_predictions")
        .join(
            data.jurisdictions(),
            "jurisdiction"
        )
        .select(
            "product_id",
            "jurisdiction_id"
        )
        .distinct()
    )

    output = (
        spark.table("prod.bronze.etrm_sim_profiles")
        .filter(
            (F.col("code") == "R") &
            (F.col("interval") == 5)
        )
        .join(
            spark.table("prod.bronze.etrm_sim_products")
            .select(
                F.col("id").alias("productid"),
                F.col("enddate").alias("product_end_date")
            ),
            "productid"
        )
        .withColumn(
            "todate",
            F.least(
                F.lead(
                    F.date_add(F.col("fromdate"), -1),
                    1,
                    date(9999, 12, 13)
                ).over(
                    W.partitionBy("productid", "code", "daytype", "period")
                    .orderBy("fromdate")
                ),
                F.col("product_end_date")
            )
        )
        .withColumnRenamed("productid", "product_id")
        .join(product_jurisdictions, "product_id")
        .select(
            F.col("product_id").cast("short").alias("product_id"),
            F.col("fromdate").alias("from_date"),
            F.col("todate").alias("to_date"),
            F.col("jurisdiction_id"),
            F.col("daytype").alias("day_type"),
            F.col("period").alias("period_id"),
            F.col("value").alias("rate")
        )
    )

    return output

def retail_rate_calendar(
    start_date: date=date(2000, 1, 1),
    end_date: date=date(2050, 12 ,31)
) -> DataFrame:
    """
    Creates an interval level rate calendar for all Sim products with a rate.
    """

    output = (
        calendar()
        .filter(F.col("interval_date").between(F.lit(start_date), F.lit(end_date)))
        .alias("calendar")
        .join(
            retail_rates()
            .filter(
                (F.col("from_date") <= F.lit(end_date)) &
                (F.col("to_date") >= F.lit(start_date)))
            .alias("retail_rate"),
            F.col("interval_date").between(F.col("from_date"), F.col("to_date")) &
            (F.col("retail_rate.period_id") == F.col("calendar.period_id")) &
            (F.col("retail_rate.day_type") == F.col("calendar.day_type")) &
            (F.col("retail_rate.jurisdiction_id") == F.col("calendar.jurisdiction_id"))
        )
        .select(
            F.col("retail_rate.product_id").cast("short"),
            "retail_rate.jurisdiction_id",
            "interval_date",
            F.col("calendar.period_id").cast("short"),
            F.col("rate").cast("float")
        )
    ).cache()
    return output

def product_profiles():
    profiles_table = spark.table("exploration.denise_ng.sim_daytypes_profile")
    
    flat_products = (
        spark.table("prod.bronze.etrm_sim_products")
        .filter(F.col("schedule").isin(["Flat", "Base"]))
        .select(F.col("id").alias("product_id"))
        .distinct()
    )

    output = (
        profiles_table
        .select(
            F.col("PRODUCT_ID").cast("short").alias("product_id"),
            F.col("INTERVAL_DATE").alias("interval_date"),
            F.col("PERIOD_5MIN").cast("short").alias("period_id"),
            F.col("PROFILE_MW").alias("profile_mw"),
            F.col("PROFILE_STRIKE").alias("profile_strike")
        )
        .join(flat_products, "product_id", "left_anti")
    )

    return output

def lgc_rates():
    rate_source_table = spark.table("exploration.denise_ng.mtlf_hh")

    output = (
        rate_source_table
        .select(
            F.col("productid").cast("short").alias("product_id"),
            F.col("date").alias("interval_date"),
            F.col("green_rate").cast("double").alias("lgc_price"),
            F.col("green_percentage").cast("double").alias("lgc_percentage")
        )
        .dropDuplicates(subset=["product_id", "interval_date"])
    )
    return output






