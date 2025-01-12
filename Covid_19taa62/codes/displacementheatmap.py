from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, hour, to_timestamp, avg
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Taxi Demand Displacement Analysis") \
    .getOrCreate()

# Function to load and combine Parquet data for each month
def load_monthly_data(base_path, year_month_pairs):
    df_list = []
    for (year, month) in year_month_pairs:
        file_path = f"{base_path}/yellow_tripdata_{year}-{month:02d}.parquet"
        df_month = spark.read.parquet(file_path)
        df_list.append(df_month)
    return df_list

# Function to preprocess data
def preprocess_data(df):
    df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    df = df.withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime")))
    df = df.withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    return df

# Function to calculate relative demand
def calculate_relative_demand(df):
    df_grouped = df.groupBy("day_of_week", "hour_of_day").count()
    total_trips = df_grouped.groupBy().sum("count").collect()[0][0]
    df_relative_demand = df_grouped.withColumn("relative_demand", col("count") / total_trips)
    return df_relative_demand

# Function to calculate displacement of relative demand
def calculate_displacement(df_2019, df_2020):
    df_2020 = df_2020.withColumnRenamed("count", "count_2020") \
                      .withColumnRenamed("relative_demand", "relative_demand_2020")
    df_2019 = df_2019.withColumnRenamed("count", "count_2019") \
                      .withColumnRenamed("relative_demand", "relative_demand_2019")
    displacement_df = df_2020.join(df_2019, ["day_of_week", "hour_of_day"], "outer")
    displacement_df = displacement_df.withColumn("displacement", col("relative_demand_2020") - col("relative_demand_2019"))
    return displacement_df

# Function to analyze demand changes for a list of DataFrames
def analyze_demand_changes(df_list):
    combined_df = df_list[0]
    for df in df_list[1:]:
        combined_df = combined_df.unionByName(df)
    return calculate_relative_demand(preprocess_data(combined_df))

# Function to calculate total trips and average passengers for a list of DataFrames
def calculate_stats(df_list):
    combined_df = df_list[0]
    for df in df_list[1:]:
        combined_df = combined_df.union(df)
    total_trips = combined_df.count()
    avg_passengers = combined_df.agg(avg(col("Passenger_count"))).first()[0]
    single_person_trips = combined_df.filter(col("Passenger_count") == 1).count()
    return total_trips, avg_passengers, single_person_trips

# Main analysis function
def main():
    try:
        base_path = '/mnt/d/project/testdata'  # Update this path to your data folder location
        year_month_2019 = [(2019, 3), (2019, 4), (2019, 5)]
        year_month_2020 = [(2020, 3), (2020, 4), (2020, 5)]

        # Load and preprocess data for both periods
        df_list_2019 = load_monthly_data(base_path, year_month_2019)
        df_list_2020 = load_monthly_data(base_path, year_month_2020)

        # Calculate total trips and average passengers
        total_trips_2019, avg_passengers_2019, single_person_trips_2019 = calculate_stats(df_list_2019)
        total_trips_2020, avg_passengers_2020, single_person_trips_2020 = calculate_stats(df_list_2020)

        # Calculate percent reduction in trips and single-person trips
        reduction_percentage = (total_trips_2019 - total_trips_2020) / total_trips_2019 * 100
        single_trip_percentage_2019 = single_person_trips_2019 / total_trips_2019 * 100
        single_trip_percentage_2020 = single_person_trips_2020 / total_trips_2020 * 100

        # Print out the calculated statistics
        print(f"Total trips 2019: {total_trips_2019}, Average passengers 2019: {avg_passengers_2019}, Single-person trips 2019: {single_trip_percentage_2019}%")
        print(f"Total trips 2020: {total_trips_2020}, Average passengers 2020: {avg_passengers_2020}, Single-person trips 2020: {single_trip_percentage_2020}%")
        print(f"Reduction in trips: {reduction_percentage}%")

        # Analyze demand changes for both periods
        df_relative_demand_2019 = analyze_demand_changes(df_list_2019)
        df_relative_demand_2020 = analyze_demand_changes(df_list_2020)

        # Calculate displacement of relative demand
        displacement_df = calculate_displacement(df_relative_demand_2019, df_relative_demand_2020)

        # Collect the data for visualization
        displacement_pd = displacement_df.toPandas()

        # Show raw counts for debugging
        print("Showing raw counts for debugging:")
        displacement_df.select('day_of_week', 'hour_of_day', 'count_2019', 'count_2020').show()

        # Show relative demand for debugging
        print("Showing relative demand for debugging:")
        displacement_df.select('day_of_week', 'hour_of_day', 'relative_demand_2019', 'relative_demand_2020').show()

        # Convert the data to a pivot table for heatmap plotting
        pivot_table = displacement_pd.pivot(index='day_of_week', columns='hour_of_day', values='displacement')

        # Plotting the heatmap
        plt.figure(figsize=(15, 6))  # Adjust the size as needed
        sns.heatmap(pivot_table, annot=True, fmt=".3f", cmap='coolwarm', center=0, cbar_kws={'label': 'Displacement of Relative Demand'})
        plt.title('Displacement of Relative Demand by Day of Week and Hour of Day')
        plt.xlabel('Hour of the Day')
        plt.ylabel('Day of the Week')
        plt.tight_layout()  # Adjust the layout so everything fits without overlapping

        # Save the heatmap as a PNG file
        plt.savefig('/mnt/d/project/testdata/displacement_heatmap.png')
        print("Heatmap saved")

    except Exception as e:
        print(f"An exception occurred: {e}")

if __name__ == "__main__":
    main()
