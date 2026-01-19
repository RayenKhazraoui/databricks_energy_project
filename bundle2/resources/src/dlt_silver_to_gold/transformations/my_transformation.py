
import dlt as dp
from pyspark.sql.functions import col, sum as F_sum, to_date, date_trunc, to_timestamp

# ---------------------------
# Silver → Gold aggregations
# ---------------------------

@dp.table(
    name="daily_agg_a16",
    comment="Daily aggregation of silver_A16 by position_timestamp (date-granularity)."
)
def daily_agg_a16():
    return (
        spark.table("data.silver.silver_A16")
        # Normalize to DATE granularity while keeping the column name
        .withColumn("position_timestamp", to_date(col("position_timestamp")))
        .groupBy("position_timestamp")
        .agg(F_sum("quantity").alias("quantity"))
    )


@dp.table(
    name="monthly_agg_a16",
    comment="Monthly aggregation of silver_A16 by position_timestamp (month-granularity)."
)
def monthly_agg_a16():
    return (
        spark.table("data.silver.silver_A16")
        # Make sure we have a timestamp for date_trunc; cast if needed
        .withColumn("position_timestamp", date_trunc("month", to_timestamp(col("position_timestamp"))))
        .groupBy("position_timestamp")
        .agg(F_sum("quantity").alias("quantity"))
    )


# ---------------------------
# Join A75 with A16 (load generation)
# ---------------------------

@dp.table(
    name="load_generation_joined",
    comment="Inner join of silver_A75 and selected columns of silver_A16 on position_timestamp."
)
def load_generation_joined():
    df_a75 = spark.table("data.silver.silver_A75")
    df_a16 = (
        spark.table("data.silver.silver_A16")
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


# ---------------------------
# Monthly aggregation over the joined load data
# ---------------------------

cols_to_agg = [
    "B01", "B04", "B05", "B11", "B14", "B16", "B17", "B18", "B19", "B20",
    "quantity"
]

@dp.table(
    name="monthly_agg_combined",
    comment="Monthly aggregation of the load_generation_joined table by position_timestamp."
)
def monthly_agg_combined():
    return (
        # IMPORTANT: reference the DLT table by name using dlt.read, not spark.read.table("gold....")
        dp.read("load_generation_joined")
        .withColumn("position_timestamp", date_trunc("month", to_timestamp(col("position_timestamp"))))
        .groupBy("position_timestamp")
        .agg(*[F_sum(c).alias(c) for c in cols_to_agg])
    )


# ---------------------------
# Weather transformations
# ---------------------------

@dp.table(
    name="weather_data_with_temp_c",
    comment="Weather data with recalculated temperature (T/10 as temp_celsius)."
)
def weather_data_with_temp_c():
    return (
        spark.table("data.silver.weather_data_silver")
        .withColumn("temp_celsius", col("T") / 10.0)
    )


@dp.table(
    name="load_weather_joined",
    comment="Inner join of load_generation_joined and weather_data_with_temp_c on position_timestamp = YYYYMMDD_HH."
)
def load_weather_joined():
    df_load = dp.read("load_generation_joined")
    df_weather = dp.read("weather_data_with_temp_c")

    return (
        df_load.join(
            df_weather,
            df_load["position_timestamp"] == df_weather["YYYYMMDD_HH"],
            how="inner"
        )
    )
