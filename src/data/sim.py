import pandas as pd

from datetime import date
from pyspark.sql import functions as F, Window as W, DataFrame
from src import spark

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
            F.col("deals.id").alias("deal_id"),
            F.col("deals.name").alias("deal_name"),
            F.row_number().over(
                W.partitionBy("deals.id").orderBy("legs.transferdate")
            ).alias("leg_number"),
            F.col("products.id").alias("product_id"),
            F.col("products.name").alias("product_name"),
            F.col("deals.buysell").alias("buy_sell"),
            F.col("deals.dealdate").alias("deal_date"),
            F.col("deals.status").alias("status"),
            F.col("products.schedule").alias("product_schedule"),
            F.col("products.startdate").alias("product_start_date"),
            F.col("products.enddate").alias("product_end_date"),
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
            F.col("legs.transferdate").alias("transfer_date"),
            F.col("legs.paymentdate").alias("payment_date")
        )
    )
    return output

def calendar() -> DataFrame:
    jurisdiction_region_id = spark.createDataFrame(pd.DataFrame({
        "jurisdiction": ["SA", "VIC", "NSW", "ACT", "QLD"],
        "regionid": ["SA1", "VIC1", "NSW1", "NSW1", "QLD1"]
    }))

    output = (
        spark.table("prod.silver.calendar_datetime")
        .crossJoin(jurisdiction_region_id.select("jurisdiction").distinct())
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
            F.col("jurisdiction"),
            F.col("datetime").alias("interval_date_time"),
            F.col("period").alias("period_id"),
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

def rates() -> DataFrame:
    """
    Pulls rates from sim profiles table.
    """

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
        .select(
            F.col("productid").alias("product_id"),
            F.col("fromdate").alias("from_date"),
            F.col("todate").alias("to_date"),
            F.col("daytype").alias("day_type"),
            F.col("period").alias("period_id"),
            F.col("value").alias("rate")
        )
    )

    return output

def rate_calendar(
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
            rates()
            .filter(
                (F.col("from_date") <= F.lit(end_date)) &
                (F.col("to_date") >= F.lit(start_date)))
            .alias("rate"),
            F.col("interval_date").between(F.col("from_date"), F.col("to_date")) &
            (F.col("rate.period_id") == F.col("calendar.period_id")) &
            (F.col("rate.day_type") == F.col("calendar.day_type"))
        )
        .select(
            "product_id",
            "jurisdiction",
            "interval_date_time",
            "interval_date",
            "calendar.period_id",
            "rate"
        )
    ).cache()
    return output