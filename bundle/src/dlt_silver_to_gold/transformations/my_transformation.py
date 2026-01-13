from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, to_date, date_trunc

@dp.table(
    name="gold.daily_agg_a16",
    comment="Daily aggregation of silver_A16 by position_timestamp"
)
def daily_agg_a16():
    return (
        spark.read.table("data.silver.silver_A16")
        .withColumn("position_timestamp", to_date(col("position_timestamp")))
        .groupBy("position_timestamp")
        .agg(
            sum("quantity").alias("quantity")
        )
    )

@dp.table(
    name="gold.monthly_agg_a16",
    comment="Monthly aggregation of silver_A16 by position_timestamp"
)
def monthly_agg_a16():
    return (
        spark.read.table("data.silver.silver_A16")
        .withColumn("position_timestamp", date_trunc("month", col("position_timestamp")))
        .groupBy("position_timestamp")
        .agg(
            sum("quantity").alias("quantity")
        )
    )

    from pyspark import pipelines as dp

from pyspark import pipelines as dp

@dp.table(
    name="gold.load_generation_joined",
    comment="Inner join of silver_A75 and silver_A16 on position_timestamp"
)
def load_generation_joined():
    df_a75 = spark.read.table("data.silver.silver_A75")
    df_a16 = (
        spark.read.table("data.silver.silver_A16")
        .select(
            "position_timestamp",  # join key
            "quantity"             # add other non-conflicting columns as needed
        )
    )
    return (
        df_a75.join(
            df_a16,
            on="position_timestamp",
            how="inner"
        )
    )


cols_to_agg = [
    "B01", "B04", "B05", "B11", "B14", "B16", "B17", "B18", "B19", "B20", "quantity"
]
@dp.table(
    name="gold.monthly_agg_combined",
    comment="Monthly aggregation of silver_A75 and silver_A16 by position_timestamp"
)
def monthly_agg_combined():
    return (
        spark.read.table("gold.load_generation_joined")
        .withColumn(
            "position_timestamp",
            date_trunc("month", col("position_timestamp"))
        )
        .groupBy("position_timestamp")
        .agg(
            *[sum(c).alias(c) for c in cols_to_agg]
        )
    )

@dp.table(
    name="gold.weather_data_with_temp_c",
    comment="Weather data with recalculated temperature (T/10 as temp_celsius)"
)
def weather_data_with_temp_c():
    return (
        spark.read.table("data.silver.weather_data_silver")
        .withColumn("temp_celsius", col("T") / 10)
    )


@dp.table(
    name="gold.load_weather_joined",
    comment="Inner join of load_generation_joined and weather_data_with_temp_c on position_timestamp = YYYYMMDD_HH"
)
def load_weather_joined():
    df_load = spark.read.table("gold.load_generation_joined")
    df_weather = spark.read.table("gold.weather_data_with_temp_c")
    return (
        df_load.join(
            df_weather,
            df_load["position_timestamp"] == df_weather["YYYYMMDD_HH"],
            how="inner"
        )
    )
    