# Import the libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, month, year

from pyspark.sql.functions import unix_timestamp, col, year, month, to_timestamp
from pyspark.sql.functions import concat_ws

from pyspark.sql import functions as F
import os
import sys 

# Create a SparkSession
spark = SparkSession.builder \
    .appName("TaxiData") \
    .getOrCreate()

taxi_color = sys.argv[1]

data_files = sorted(os.listdir(f'datasets/{taxi_color}_taxi_data'))

output_folder_path = f'outputs/{taxi_color}_taxi/avg_amounts_speed_time_passenger_counts'
os.makedirs(output_folder_path, exist_ok=True)


def rename_column(df, old_name, new_name):
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)
    return df
    
for i in data_files:
    df_path = f'datasets/{taxi_color}_taxi_data/{i}'

    df = spark.read.parquet(df_path)

    df = rename_column(df, "lpep_pickup_datetime", "pickup_datetime")
    df = rename_column(df, "lpep_dropoff_datetime", "dropoff_datetime")
    df = rename_column(df, "tpep_pickup_datetime", "pickup_datetime")
    df = rename_column(df, "tpep_dropoff_datetime", "dropoff_datetime")

    # Convert datetime fields to timestamp type
    datetime_fields = ["pickup_datetime", "dropoff_datetime"]
    for field in datetime_fields:
        df = df.withColumn(field, to_timestamp(col(field)))

    df = df.withColumn("date", to_date(col("pickup_datetime")))

    selected_columns = [
    "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID",
    "trip_distance", "RatecodeID", "total_amount", "tip_amount", "fare_amount", "passenger_count"]

    df = df.select(*selected_columns)

    df = df.na.drop()
    
    # Define the start and end dates
    start_date = "2017-01-01"
    end_date = "2023-12-31"

    # Filter the DataFrame to keep rows between these dates
    df = df.filter((col("pickup_datetime") >= start_date) & (col("pickup_datetime") <= end_date) &
                            (col("dropoff_datetime") >= start_date) & (col("dropoff_datetime") <= end_date))

    # Convert the timestamp to a date
    df = df.withColumn("date", to_date(col("pickup_datetime")))

    # Extracting year and month from the date
    df = df.withColumn("year", year(col("date")))
    df = df.withColumn("month", month(col("date")))

    # Concatenating year and month into a single column
    df = df.withColumn("month_year", concat_ws("-", col("month"), col("year")))
    
    # Getting the time difference between drop off and pick up time
    df = df.withColumn("pickup_timestamp", unix_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("dropoff_timestamp", unix_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))
    
    df = df.withColumn("time_diff", col("dropoff_timestamp") - col("pickup_timestamp"))
    
    columns_to_drop = ['pickup_datetime', 'dropoff_datetime', 'pickup_timestamp', 'dropoff_timestamp']

    df = df.drop(*columns_to_drop)

    df = df.withColumn("speed", F.when(
    (F.col("time_diff").isNotNull()) & (F.col("time_diff") != 0),
    F.col("trip_distance") / (F.col("time_diff") / 3600)
    ).otherwise(None))
    
    # filer the speed value
    df = df.filter(df['speed'] < 100).filter(df['speed'] > 0)
    
    avg_fare_total_and_tip_amounts_df = df.groupBy("month_year").agg(F.mean("total_amount").alias("avg_total_amount"),
                                                                     F.mean("fare_amount").alias("avg_fare_amount"),
                                                                     F.mean("tip_amount").alias("avg_tip_amount"),
                                                                     F.round(F.sum("passenger_count"), 2).alias("total_passenger_count"),
                                                                     F.mean("time_diff").alias("avg_time_diff"),
                                                                     F.mean("speed").alias("avg_speed"))
    
    avg_fare_total_and_tip_amounts_df = avg_fare_total_and_tip_amounts_df.na.drop()

    avg_fare_total_and_tip_amounts_pd = avg_fare_total_and_tip_amounts_df.toPandas()
    avg_fare_total_and_tip_amounts_pd.to_csv(f'{output_folder_path}/{i}.csv'.replace(".parquet", ""), index = False)

    print(f'Saved: {i}.csv'.replace(".parquet", ""))

df = spark.read.csv(output_folder_path, header=True, inferSchema=True)
df = df.groupBy("month_year").agg(F.mean("avg_total_amount").alias("avg_total_amount"),
                                  F.mean("avg_fare_amount").alias("avg_fare_amount"),
                                  F.mean("avg_tip_amount").alias("avg_tip_amount"),
                                  F.round(F.sum("total_passenger_count"), 2).alias("total_passenger_count"),
                                 F.round(F.sum("avg_time_diff"), 2).alias("avg_time_diff"),
                                 F.round(F.sum("avg_speed"), 2).alias("avg_speed"))

# Ordering the result by month_year
df = df.orderBy("month_year")

final_output_folder_path = f'{output_folder_path}/final_result'
os.makedirs(final_output_folder_path, exist_ok=True)

final_pd = df.toPandas()
final_pd.to_csv(f'{final_output_folder_path}/final_result.csv'.replace(".parquet", ""), index = False)