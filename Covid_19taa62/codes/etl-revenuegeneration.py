from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession  # Import SparkSession
import sys
import os  # Import os module
from datetime import datetime

def get_files(input_folder):
    base_path = '/mnt/d/project/testdata/'  # Absolute path to the directory
    return [os.path.join(base_path, file) for file in os.listdir(base_path) if file.endswith('.parquet')]

def analyze_trips(spark, file_path, pre_pandemic_date, post_pandemic_date):
    # Read Parquet file
    df = spark.read.parquet(file_path)

    # Filter pre-pandemic and post-pandemic data
    pre_pandemic_data = df.filter(df['tpep_pickup_datetime'] < pre_pandemic_date)
    post_pandemic_data = df.filter(df['tpep_pickup_datetime'] >= post_pandemic_date)

    # Count the number of trips
    pre_pandemic_count = pre_pandemic_data.count()
    post_pandemic_count = post_pandemic_data.count()

    return pre_pandemic_count, post_pandemic_count

def main(input_folder, output_folder):
    # Setting up Spark
    conf = SparkConf().setAppName("TaxiTripAnalysis")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)  # Create SparkSession from SparkContext

    # Define the date which we consider as the start of the pandemic
    pandemic_start_date = datetime(2020, 3, 1)

    # Get the list of Parquet files
    available_files = get_files(input_folder)

    total_pre_pandemic = 0
    total_post_pandemic = 0

    for file_path in available_files:
        print(f"Processing file: {file_path}")
        pre_pandemic, post_pandemic = analyze_trips(spark, file_path, pandemic_start_date, pandemic_start_date)
        total_pre_pandemic += pre_pandemic
        total_post_pandemic += post_pandemic

    print(f"Total trips before pandemic: {total_pre_pandemic}")
    print(f"Total trips after pandemic: {total_post_pandemic}")

    # Save the results
    results = spark.createDataFrame([(pandemic_start_date, total_pre_pandemic, total_post_pandemic)], ["Pandemic Start Date", "Pre-Pandemic Trips", "Post-Pandemic Trips"])
    results.write.mode("overwrite").csv(output_folder)

    # Cleaning up
    sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit this_script.py <input_folder> <output_folder>", file=sys.stderr)
        sys.exit(-1)

    input_folder = sys.argv[1]
    output_folder = sys.argv[2]
    main(input_folder, output_folder)


