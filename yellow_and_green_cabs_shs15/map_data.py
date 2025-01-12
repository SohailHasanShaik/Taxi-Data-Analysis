# Import the libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, month, year

from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import isnan, when, count, sum
from pyspark.sql import functions as F


import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import datetime


from pyspark.sql.types import IntegerType, StringType
import os

import geopandas as gpd
import sys


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Maps") \
    .getOrCreate()

taxi_color = sys.argv[1]
start_year = sys.argv[2]
end_year = sys.argv[3]

taxi_zone_lookup = spark.read.option("header", "true").csv("datasets/map/taxi_zone_lookup.csv")
zone_shp = gpd.read_file('datasets/map/taxi_zones/taxi_zones.shp')
data_files = os.listdir(f'datasets/{taxi_color}_taxi_data')

# Define the start and end periods
start_period = f'{start_year}-01'
end_period = f'{end_year}-12'

output_folder_path = f'outputs/{taxi_color}_taxi/map'
os.makedirs(output_folder_path, exist_ok=True)

try:
    # List all files in the directory
    files_in_directory = os.listdir(output_folder_path)
    
    # Loop through the files and remove those ending with .csv
    for file in files_in_directory:
        if file.endswith('.csv'):
            os.remove(os.path.join(output_folder_path, file))
except:
    pass
            
def extract_date(filename):
    # Extracts the date part from the filename and converts it to a datetime object
    date_part = filename.split('_')[-1].replace('.parquet', '')
    return datetime.strptime(date_part, '%Y-%m')

# Filter the list based on the start and end dates
filtered_files = sorted([file for file in data_files if start_period <= extract_date(file).strftime('%Y-%m') <= end_period])

def filter_dataframe_by_date(dataframe, start_date, end_date):

    try:
        return dataframe.filter(
            (col("lpep_pickup_datetime") >= start_date) & (col("lpep_pickup_datetime") <= end_date) &
            (col("lpep_dropoff_datetime") >= start_date) & (col("lpep_dropoff_datetime") <= end_date)
        )
    except:
        return dataframe.filter(
            (col("tpep_pickup_datetime") >= start_date) & (col("tpep_pickup_datetime") <= end_date) &
            (col("tpep_dropoff_datetime") >= start_date) & (col("tpep_dropoff_datetime") <= end_date)
        )

def add_time_difference(df):
    # Convert the timestamp to a date
    try:
        df = df.withColumn("date", to_date(col("lpep_pickup_datetime")))
        df = df.withColumn("pickup_timestamp", unix_timestamp(col("lpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("dropoff_timestamp", unix_timestamp(col("lpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))
    except:
        df = df.withColumn("date", to_date(col("tpep_pickup_datetime")))
        df = df.withColumn("pickup_timestamp", unix_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("dropoff_timestamp", unix_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))

    df = df.withColumn("time_diff", col("dropoff_timestamp") - col("pickup_timestamp"))

    # Drop unnecessary columns
    try:
        columns_to_drop = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'pickup_timestamp', 'dropoff_timestamp']
    except:
        columns_to_drop = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pickup_timestamp', 'dropoff_timestamp']
    df = df.drop(*columns_to_drop)

    return df

def calculate_speed_and_filter(df):
    # Calculate speed
    df = df.withColumn("speed", when(
        (col("time_diff").isNotNull()) & (col("time_diff") != 0),
        col("trip_distance") / (col("time_diff") / 3600)
    ).otherwise(None))

    # Filter for speeds less than 100 and greater than 0
    df = df.filter((col("speed") < 100) & (col("speed") > 0))

    return df

# Clean the taxi data
def clean_taxi_data(df):
    # Convert VendorID to string
    df = df.withColumn("VendorID", F.col("VendorID").cast(StringType()))

    # Filter for positive trip_distance, fare_amount, total_amount, and passenger_count
    df = df.where(F.col("trip_distance") > 0)
    df = df.where(F.col("fare_amount") > 0)
    df = df.where(F.col("total_amount") > 0)
    df = df.where(F.col("passenger_count") > 0)
    df = df.where(F.col("tip_amount") > 0)

    df = df.groupby('PULocationID').agg(\
                                        # F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
                                        F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount"),
                                        # F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
                                        # F.round(F.avg("speed"), 2).alias("avg_speed"),
                                        F.round(F.avg("time_diff"), 2).alias("avg_time_diff"),
                                        # F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance")
                                        )

    return df

def merge_taxi_with_zones(taxi_df, zones_df):
    # Merge with pickup zone
    merged_df = taxi_df.join(zones_df, taxi_df["PULocationID"] == zones_df["LocationID"], "left_outer")
    merged_df = merged_df.drop("LocationID")
    merged_df = merged_df.withColumnRenamed("Borough", "PU_borough").withColumnRenamed("Zone", "PU_zone").withColumnRenamed("service_zone", "PU_service_zone")

    # Merge with dropoff zone
    merged_df = merged_df.join(zones_df, merged_df["DOLocationID"] == zones_df["LocationID"], "left_outer")
    merged_df = merged_df.drop("LocationID")
    merged_df = merged_df.withColumnRenamed("Borough", "DO_borough").withColumnRenamed("Zone", "DO_zone").withColumnRenamed("service_zone", "DO_service_zone")
    merged_df = clean_taxi_data(merged_df)
    return merged_df


for i in filtered_files:
    df_path = f'datasets/{taxi_color}_taxi_data/{i}'

    df = spark.read.parquet(df_path)

    # Define the start and end dates
    start_date = "2019-01-01"
    end_date = "2023-12-31"

    df = filter_dataframe_by_date(df, start_date, end_date)

    # Transform the DataFrame
    df = add_time_difference(df)

    # Calculate speed and filter the speed
    df = calculate_speed_and_filter(df)

    # Define UDFs for converting datetime columns
    pickup_to_datetime = F.udf(lambda x: pd.to_datetime(x), StringType())
    dropoff_to_datetime = F.udf(lambda x: pd.to_datetime(x), StringType())

    zones_df = merge_taxi_with_zones(df, taxi_zone_lookup)
    zones_pd = zones_df.toPandas()
    zones_pd['PULocationID'] = zones_pd['PULocationID'].astype(int)
    zones_shape = pd.merge(zones_pd, zone_shp, left_on='PULocationID', right_on='LocationID')
    zones_shape = gpd.GeoDataFrame(zones_shape, geometry=zones_shape.geometry)
    
    zones_shape.to_csv(f'{output_folder_path}/{i}.csv'.replace('.parquet', ''), index=False)
    print(f'Saved: {i}.csv'.replace('.parquet', ''))

final_output_folder_path = f'{output_folder_path}/final_result'
os.makedirs(final_output_folder_path, exist_ok=True)

df = spark.read.csv(output_folder_path, header=True, inferSchema=True)

# Group by 'PULocationID' and 'geometry', then calculate the mean
df = (df.groupBy("PULocationID", "geometry").agg(
    F.mean("avg_tip_amount").alias("avg_tip_amount"),
    F.mean("avg_time_diff").alias("avg_time_diff")
))

final_pd = df.toPandas()
final_pd.to_csv(f'{final_output_folder_path}/final_result.csv', index = False)