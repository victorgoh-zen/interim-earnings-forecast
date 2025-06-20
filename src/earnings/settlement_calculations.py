import polars as pl

def ppa_energy(
    deal_info: pl.DataFrame,
    generation_profiles: pl.DataFrame,
    spot_prices: pl.DataFrame
) -> pl.DataFrame:
    """
    Args:
        deal_info: pl.DataFrame
            - deal_id: int16
            - product_id: int16
            - start_date: date
            - end_date: date
            - region_number: int8
            - floor: float64
            - price: float64
            - quantity_factor: float64
        generation_profiles: pl.DataFrame
            - product_id: int16
            - interval_date: date
            - period_id: int16
            - generation_mwh: float32
        spot_prices: pl.DataFrame
            - interval_date: date
            - period_id: int16
            - region_number: int8
            - rrp: float32
    
    Returns: pl.DataFrame
        - deal_id: int16
        - product_id: int16
        - interval_date: date
        - period_id: int16
        - region_number: int8
        - volume_mwh: float32
        - buy_income: float32
        - sell_income: float32
    """

    result = (
        deal_info
        .join(
            generation_profiles,
            ["product_id", "interval_date", "period_id"],
            how="inner"
        )
        .filter(pl.col("interval_date").is_between(pl.col("start_date"), pl.col("end_date")))
        .join(
            spot_prices,
            ["interval_date", "period_id", "region_number"],
            "inner"
        )
        .with_columns(
            (
                pl.col("generation_mwh")
                * pl.col("quantity_factor")
            ).cast(pl.Float32()).alias("volume_mwh")
        )
        .select(
            "deal_id",
            "product_id",
            "interval_date",
            "period_id",
            "region_number",
            "volume_mwh",
            (
                pl.col("rrp").clip(pl.col("floor"), None)
                * pl.col("volume_mwh")
            ).cast(pl.Float32()).alias("buy_income"),
            (
                pl.col("price")
                * pl.col("volume_mwh")
            ).cast(pl.Float32()).alias("sell_income")
        )
    )

    return result

def asset_toll_energy(
    deal_info: pl.DataFrame,
    generation_profiles: pl.DataFrame,
    spot_prices: pl.DataFrame,
    intervals_per_day: int=288
) -> pl.DataFrame:
    """
    Args:
        deal_info: pl.DataFrame
            - deal_id: int16
            - product_id: int16
            - start_date: date
            - end_date: date
            - region_number: int8
            - tolling_fee: float64 (daily???)
            - quantity_factor: float64
        generation_profiles: pl.DataFrame
            - product_id: int16
            - interval_date: date
            - period_id: int16
            - generation_mwh: float32
        spot_prices: pl.DataFrame
            - interval_date: date
            - period_id: int16
            - region_number: int8
            - rrp: float32
    
    Returns: pl.DataFrame
        - deal_id: int16
        - product_id: int16
        - interval_date: date
        - period_id: int16
        - region_number: int8
        - volume_mwh: float32
        - buy_income: float32
        - sell_income: float32
    """

    result = (
        deal_info
        .join(
            generation_profiles,
            ["product_id", "interval_date", "period_id"],
            how="inner"
        )
        .filter(pl.col("interval_date").is_between(pl.col("start_date"), pl.col("end_date")))
        .join(
            spot_prices,
            ["interval_date", "period_id", "region_number"],
            "inner"
        )
        .with_columns(
            (
                pl.col("generation_mwh")
                * pl.col("quantity_factor")
            ).cast(pl.Float32()).alias("volume_mwh")
        )
        .select(
            "deal_id",
            "product_id",
            "interval_date",
            "period_id",
            "region_number",
            "volume_mwh",
            (
                pl.col("rrp").clip(pl.col("floor"), None)
                * pl.col("volume_mwh")
            ).cast(pl.Float32()).alias("buy_income"),
            (
                pl.col("tolling_fee") / pl.lit(intervals_per_day)
            ).cast(pl.Float32()).alias("sell_income")
        )
    )

    return result

def retail_energy(
    deal_info: pl.DataFrame,
    load_profiles: pl.DataFrame,
    spot_prices: pl.DataFrame,
    rate_calendar: pl.DataFrame
) -> pl.DataFrame:
    """
    Args:
        deal_info: pl.DataFrame
            - deal_id: int16
            - product_id: int16
            - start_date: date
            - end_date: date
        load_profiles: pl.DataFrame
            - product_id: int16
            - jurisdiction_id: int8
            - region_number: int8
            - interval_date: date
            - period_id: int16
            - load_mwh
        spot_prices: pl.DataFrame
            - interval_date: date
            - period_id: int16
            - region_number: int8
            - rrp: float32
        rate_calendar: pl.DataFrame
            - product_id: int16
            - jurisdiction_id: int8
            - interval_date: date
            - period_id: int16
            - rate: float32

    Returns: pl.DataFrame
        - deal_id: int16
        - product_id: int16
        - region_number: int8
        - interval_date: date
        - period_id: int16
        - volume_mwh: float32
        - buy_income: float32
        - sell_income: float32
    Note:
        May need to output keep breakdown by jurisdiction for reporting???
        But I don't think anyone really cares tbh
    """
    result = (
        deal_info
        .join(
            load_profiles,
            "product_id"
        )
        .filter(pl.col("interval_date").is_between(pl.col("start_date"), pl.col("end_date")))
        .join(
            rate_calendar,
            ["product_id", "jurisdiction_id", "interval_date", "period_id"]
        )
        .join(
            spot_prices,
            ["interval_date", "period_id", "region_number"]
        )
        .group_by([
            "deal_id",
            "product_id",
            "region_number",
            "interval_date",
            "period_id",
        ])
        .agg(
            pl.col("load_mwh").sum().cast(pl.Float32()).alias("volume_mwh"),
            (
                pl.col("load_mwh") * pl.col("rrp")
            ).cast(pl.Float32()).sum().alias("buy_income"),
            (
                pl.col("load_mwh") * pl.col("rate")
            ).cast(pl.Float32()).sum().alias("sell_income")
        )
    )

    return result

def flat_energy_swap(
    deal_info: pl.DataFrame,
    spot_price: pl.DataFrame
) -> pl.DataFrame:
    """
    Args:
        deal_info: pl.DataFrame
            - deal_id: int16
            - product_id: int16
            - start_date: date
            - end_date: date
            - region_number: int8
            - quantity_mw: int16
            - price: float64
        spot_price: pl.DataFrame
            - interval_date: date
            - period_id: int16
            - region_number: int8
            - rrp: float32
    
    Returns: pl.DataFrame
        - deal_id: int16
        - product_id: int16
        - interval_date: date
        - period_id: int16
        - region_number: int8
        - volume_mwh: float32
        - buy_income: float32
        - sell_income: float32
    """
    result = (
        deal_info
        .join(
            spot_price,
            ["region_number"]
        )
        .filter(pl.col("interval_date").is_between(pl.col("start_date"), pl.col("end_date")))
        .with_columns(
            (
                pl.col("quantity_mw") / pl.lit(12)
            ).cast(pl.Float32()).alias("volume_mwh")
        )
        .select(
            "deal_id",
            "product_id",
            "interval_date",
            "period_id",
            "region_number",
            "volume_mwh",
            (
                pl.col("volume_mwh") * pl.col("rrp")
            ).alias("buy_income"),
            (
                pl.col("volume_mwh") * pl.col("price")
            ).alias("sell_income")
        )
    )

    return result

def flat_energy_cap(
    deal_info: pl.DataFrame,
    spot_price: pl.DataFrame
) -> pl.DataFrame:
    """
    Args:
        deal_info: pl.DataFrame
            - deal_id: int16
            - product_id: int16
            - start_date: date
            - end_date: date
            - region_number: int8
            - quantity_mw: int16
            - price: float64
            - strike: float64
        spot_price: pl.DataFrame
            - interval_date: date
            - period_id: int16
            - region_number: int8
            - rrp: float32
    
    Returns: pl.DataFrame
        - deal_id: int16
        - product_id: int16
        - interval_date: date
        - period_id: int16
        - region_number: int8
        - volume_mwh: float32
        - buy_income: float32
        - sell_income: float32
    """
    result = (
        deal_info
        .join(
            spot_price,
            ["region_number"]
        )
        .filter(pl.col("interval_date").is_between(pl.col("start_date"), pl.col("end_date")))
        .with_columns(
            (
                pl.col("quantity_mw") / pl.lit(12)
            ).cast(pl.Float32()).alias("volume_mwh")
        )
        .select(
            "deal_id",
            "product_id",
            "interval_date",
            "period_id",
            "region_number",
            "volume_mwh",
            (
                (pl.col("rrp") - pl.col("strike")).clip(0)
                * pl.col("volume_mwh")
            ).cast(pl.Float32()).alias("buy_income"),
            (
                pl.col("volume_mwh") * pl.col("price")
            ).cast(pl.Float32()).alias("sell_income")
        )
    )

    return result

def profiled_energy_swap(
    deal_info: pl.DataFrame,
    product_profies: pl.DataFrame,
    spot_price: pl.DataFrame
) -> pl.DataFrame:
    """
    Args:
        deal_info: pl.DataFrame
            - deal_id: int16
            - product_id: int16
            - start_date: date
            - end_date: date
            - region_number: int8
            - quantity_mw: int16
            - price: float64
        product_profiles: pl.DataFrame
            - product_id: int16
            - interval_date: date
            - period_id: int16
            - volume_factor: float64
            - price_factor: float64
        spot_price: pl.DataFrame
            - interval_date: date
            - period_id: int16
            - region_number: int8
            - rrp: float32
    
    Returns: pl.DataFrame
        - deal_id: int16
        - product_id: int16
        - interval_date: date
        - period_id: int16
        - region_number: int8
        - volume_mwh: float32
        - buy_income: float32
        - sell_income: float32
    """
    result = (
        deal_info
        .join(
            product_profiles,
            ["product_id"]
        )
        .filter(pl.col("interval_date").is_between(pl.col("start_date"), pl.col("end_date")))
        .join(
            spot_price,
            ["interval_date", "period_id", "region_number"]
        )
        .with_columns(
            (
                pl.col("quantity_mw") / pl.lit(12)
                * pl.col("volume_factor")
            ).cast(pl.Float32()).alias("volume_mwh")
        )
        .select(
            "deal_id",
            "product_id",
            "interval_date",
            "period_id",
            "region_number",
            "volume_mwh",
            (
                pl.col("rrp") * pl.col("volume_mwh")
            ).cast(pl.Float32()).alias("buy_income"),
        )
    )

def profiled_energy_cap(
    deal_info: pl.DataFrame,
    product_profies: pl.DataFrame,
    spot_price: pl.DataFrame
) -> pl.DataFrame:
    """
    Args:
        deal_info: pl.DataFrame
            - deal_id: int16
            - product_id: int16
            - start_date: date
            - end_date: date
            - region_number: int8
            - quantity_mw: int16
            - price: float64
        product_profiles: pl.DataFrame
            - product_id: int16
            - interval_date: date
            - period_id: int16
            - volume_factor: float64
            - price_factor: float64
            - strike: float64
        spot_price: pl.DataFrame
            - interval_date: date
            - period_id: int16
            - region_number: int8
            - rrp: float32
    
    Returns: pl.DataFrame
        - deal_id: int16
        - product_id: int16
        - interval_date: date
        - period_id: int16
        - region_number: int8
        - volume_mwh: float32
        - buy_income: float32
        - sell_income: float32
    """
    result = (
        deal_info
        .join(
            product_profiles,
            ["product_id"]
        )
        .filter(pl.col("interval_date").is_between(pl.col("start_date"), pl.col("end_date")))
        .join(
            spot_price,
            ["interval_date", "period_id", "region_number"]
        )
        .with_columns(
            (
                pl.col("quantity_mw") / 12
                * pl.col("volume_factor")
            ).cast(pl.Float32()).alias("volume_mwh")
        )
        .select(
            "deal_id",
            "product_id",
            "interval_date",
            "period_id",
            "region_number",
            "volume_mwh",
            (
                (pl.col("rrp") - pl.col("strike")).clip(0)
                * pl.col("volume_mwh")
            ).cast(pl.Float32()).alias("buy_income"),
            (
                pl.col("volume_mwh") * pl.col("price")
            ).cast(pl.Float32()).alias("sell_income")
        )
    )

    return result

def ppa_lgc(
    deal_info: pl.DataFrame,
    generation_profiles: pl.DataFrame,
    lgc_prices: pl.DataFrame
) -> pl.DataFrame:
    """
    Args:
        deal_info: pl.DataFrame
            - deal_id: int16
            - product_id: int16
            - start_date: date
            - end_date: date
            - region_number: int8
            - price: float64
            - quantity_factor: float64
        generation_profiles: pl.DataFrame
            - product_id: int16
            - interval_date: date
            - period_id: int16
            - generation_mwh: float32
        lgc_prices: pl.DataFrame
            - interval_date: date
            - lgc_price: float64
    
    Returns: pl.DataFrame
        - deal_id: int16
        - product_id: int16
        - interval_date: date
        - period_id: int16
        - region_number: int8
        - volume_mwh: float32
        - buy_income: float32
        - sell_income: float32
    """
    result = (
        deal_info
        .join(
            generation_profiles,
            ["product_id"]
        )
        .filter(pl.col("interval_date").is_between(pl.col("start_date"), pl.col("end_date")))
        .join(
            lgc_prices,
            "interval_date"
        )
        .with_column(
            (
                pl.col("generation_mwh") * pl.col("quantity_factor")
            ).cast(pl.Float32()).alias("volume_mwh")
        )
        .select(
            "deal_id",
            "product_id",
            "interval_date",
            "period_id",
            "region_number",
            "volume_mwh",
            (
                pl.col("volume_mwh") * pl.col("lgc_price")
            ).cast(pl.Float32()).alias("buy_income"),
            (
                pl.col("volume_mwh") * pl.col("price")
            ).cast(pl.Float32()).alias("sell_income")
        )
    )

    return result

def retail_lgc(
    deal_info: pl.DataFrame,
    load_profiles: pl.DataFrame,
    lgc_price: pl.DataFrame
) -> pl.DataFrame:
    """
    Args:
        deal_info: pl.DataFrame
            - deal_id: int16
            - product_id: int16
            - start_date: date
            - end_date: date
            - lgc_percentage: float64
            - price: date
        load_profiles: pl.DataFrame
            - product_id: int16
            - jurisdiction_id: int8
            - region_number: int8
            - interval_date: date
            - period_id: int16
            - load_mwh
        lgc_price: pl.DataFrame
            - interval_date: date
            - lgc_price: float64

    Returns: pl.DataFrame
        - deal_id: int16
        - product_id: int16
        - region_number: int8
        - interval_date: date
        - period_id: int16
        - volume_mwh: float32
        - buy_income: float32
        - sell_income: float32
    """
    result = (
        deal_info
        .join(
            load_profiles,
            "product_id"
        )
        .filter(pl.col("interval_date").is_between(pl.col("start_date"), pl.col("end_date")))
        .join(
            lgc_price,
            "interval_date"
        )
        .with_columns(
            (
                pl.col("load_mwh") * pl.col("lgc_percentage")
            ).cast(pl.Float32()).alias("volume_mwh")
        )
        .select(
            "deal_id",
            "product_id",
            "region_number",
            "interval_date",
            "period_id",
            "volume_mwh",
            (
                pl.col("volume_mwh") * pl.col("lgc_price")
            ).cast(pl.Float32()).alias("buy_income"),
            (
                pl.col("volume_mwh") * pl.col("price")
            ).cast(pl.Float32()).alias("sell_income")
        )
    )

    return result









