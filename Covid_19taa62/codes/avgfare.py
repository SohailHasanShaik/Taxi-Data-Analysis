from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg
import matplotlib.pyplot as plt

# Initialize Spark Session
spark = SparkSession.builder.appName("Fare Amount Analysis").getOrCreate()

# Define the path to the directory containing your Parquet files
base_path = "/mnt/d/project/testdata"

# Load the datasets for the relevant years
years = [2019, 2020, 2021, 2022, 2023]  # Adjust the years as per your data availability
fare_amount_data = {}

for yr in years:
    # Read the dataset for each year
    df = spark.read.parquet(f"{base_path}/yellow_tripdata_{yr}-*.parquet")

    # Calculate the average fare amount for the year
    avg_fare = df.groupBy(year("tpep_pickup_datetime").alias("year")).agg(avg("Fare_amount").alias("avg_fare")).orderBy("year")

    # Collect the result and store in a dictionary
    fare_amount_data[yr] = avg_fare.collect()[0]["avg_fare"]

# Plotting
plt.figure(figsize=(10, 6))
years = list(fare_amount_data.keys())
avg_fares = list(fare_amount_data.values())

plt.bar(years, avg_fares, color='skyblue')
plt.xlabel('Year')
plt.ylabel('Average Fare Amount ($)')
plt.title('Average Taxi Fare Amount Per Trip (2019-2023)')
plt.xticks(years)

# Save the plot
plot_path = '/mnt/d/project/testdata/avg_fare_plot.png'  # Change this to your desired path
plt.savefig(plot_path, format='png')

# Display the plot
plt.show()

# Close the Spark session
spark.stop()
