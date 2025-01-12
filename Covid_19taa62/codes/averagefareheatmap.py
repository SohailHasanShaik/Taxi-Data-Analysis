from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, mean
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def create_heatmap(dataframe, title, filename):
    """
    Function to create a heatmap using Seaborn and Matplotlib.
    :param dataframe: Pandas DataFrame to plot.
    :param title: Title of the heatmap.
    :param filename: Filename to save the heatmap.
    """
    plt.figure(figsize=(10, 8))
    sns.heatmap(dataframe, annot=True, fmt=".2f", cmap="YlGnBu")
    plt.title(title)
    plt.xticks(rotation=45)
    plt.yticks(rotation=0)
    plt.savefig(filename)
    plt.show()

# Initialize Spark Session
spark = SparkSession.builder.appName("Time-Based Fare Analysis").getOrCreate()

# Read the Parquet file
df = spark.read.parquet("/mnt/d/project/testdata/yellow_tripdata_2021-04.parquet")

# Data preprocessing
df = df.withColumn("hour", hour("tpep_pickup_datetime"))
df = df.withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))

# Time-Based Analysis for Heatmap
hour_day_stats = df.groupBy("hour", "day_of_week").agg(
    mean("fare_amount").alias("avg_fare")
).orderBy("day_of_week", "hour")


# ... [previous code remains unchanged]

# Convert to Pandas DataFrame for visualization
hour_day_stats_pd = hour_day_stats.toPandas()

# Correcting the pivot method usage
hour_day_stats_pivot = hour_day_stats_pd.pivot(index='hour', columns='day_of_week', values='avg_fare')

# Create and save the heatmap
create_heatmap(hour_day_stats_pivot, "Average Fare Heatmap by Hour and Day of Week", "/mnt/d/project/output6/avg_fare_heatmap.png")



# Create and save the heatmap
create_heatmap(hour_day_stats_pivot, "Average Fare Heatmap by Hour and Day of Week", "/mnt/d/project/output6/avg_fare_heatmap2.png")
plot_path = '/mnt/d/project/testdata/avg_fare_plot2.png'
plt.savefig(plot_path, format='png')
# Close the Spark session
spark.stop()
