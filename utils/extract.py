from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W

ETRM_SOURCE_MAP = {
    "sim": {
        "deals": "prod.bronze.etrm_sim_deals",
        "legs": "prod.bronze.etrm_sim_legs",
        "products": "prod.bronze.etrm_sim_products",
        "books": "prod.bronze.etrm_sim_books",
        "strategies": "prod.bronze.etrm_sim_strategies",
        "companies": "prod.bronze.etrm_sim_companies",
        "calendars": "prod.bronze.etrm_sim_calendars",
        "instruments": "prod.bronze.etrm_sim_instruments",
        "markets": "prod.bronze.etrm_sim_markets",
        "nodes": "prod.bronze.etrm_sim_nodes",
    }
}


def extract_deal_info(env: str = "sim"):
    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    t = ETRM_SOURCE_MAP[env]

    df = (
        spark.table(t["legs"])
        .alias("legs")
        .join(
            spark.table(t["deals"]).alias("deals"),
            F.col("legs.dealid") == F.col("deals.id"),
            "left_outer",
        )
        .join(
            spark.table(t["products"]).alias("products"),
            F.col("legs.productid") == F.col("products.id"),
            "left_outer",
        )
        .join(
            spark.table(t["books"]).alias("books"),
            F.col("deals.bookid") == F.col("books.id"),
            "left_outer",
        )
        .join(
            spark.table(t["strategies"]).alias("strategies"),
            F.col("deals.strategyid") == F.col("strategies.id"),
            "left_outer",
        )
        .join(
            spark.table(t["companies"]).alias("companies"),
            F.col("deals.companyid") == F.col("companies.id"),
            "left_outer",
        )
        .join(
            spark.table(t["companies"]).alias("entities"),
            F.col("deals.tradingentityid") == F.col("entities.id"),
            "left_outer",
        )
        .join(
            spark.table(t["calendars"]).alias("calendars"),
            F.col("deals.calendarid") == F.col("calendars.id"),
            "left_outer",
        )
        .join(
            spark.table(t["companies"]).alias("brokers"),
            F.col("deals.brokerid") == F.col("brokers.id"),
            "left_outer",
        )
        .join(
            spark.table(t["companies"]).alias("clearers"),
            F.col("deals.clearerid") == F.col("clearers.id"),
            "left_outer",
        )
        .join(
            spark.table(t["instruments"]).alias("instruments"),
            F.col("products.instrumentid") == F.col("instruments.id"),
            "left_outer",
        )
        .join(
            spark.table(t["markets"]).alias("markets"),
            F.col("instruments.marketid") == F.col("markets.id"),
            "left_outer",
        )
        .join(
            spark.table(t["nodes"]).alias("nodes"),
            F.col("products.nodeid") == F.col("nodes.id"),
            "left_outer",
        )
        .filter(F.col("books.active") == "Y")
        .select(
            F.col("deals.id").alias("deal_id"),
            F.col("deals.name").alias("deal_name"),
            F.row_number()
            .over(W.partitionBy("deals.id").orderBy("legs.transferdate"))
            .alias("leg_number"),
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
                F.col("nodes.code"), 1, F.length(F.col("nodes.code")) - 1
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
            F.col("legs.paymentdate").alias("payment_date"),
        )
    )

    return df.cache()