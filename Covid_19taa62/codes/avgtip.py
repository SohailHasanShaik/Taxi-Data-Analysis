from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg
import matplotlib.pyplot as plt

# Initialize Spark Session
spark = SparkSession.builder.appName("Tip Amount Analysis").getOrCreate()

# Define the path to the directory containing your Parquet files
base_path = "/mnt/d/project/testdata"

# Define the years for analysis - adjust these as per your data
years = [2019, 2020, 2021, 2022, 2023]
tip_amount_data = {}

for yr in years:
    # Load the dataset for each year
    df = spark.read.parquet(f"{base_path}/yellow_tripdata_{yr}-*.parquet")

    # Calculate the average tip amount for the year
    avg_tip = df.groupBy(year("tpep_pickup_datetime").alias("year")).agg(avg("tip_amount").alias("avg_tip")).orderBy("year")

    # Collect the result and store in a dictionary
    tip_amount_data[yr] = avg_tip.collect()[0]["avg_tip"]

# Plotting
plt.figure(figsize=(10, 6))
years = list(tip_amount_data.keys())
avg_tips = list(tip_amount_data.values())

plt.bar(years, avg_tips, color='green')
plt.xlabel('Year')
plt.ylabel('Average Tip Amount ($)')
plt.title('Average Tip Amount Per Trip (2019-2023)')
plt.xticks(years)

# Save the plot
plot_path = '/mnt/d/project/testdata/avg_tip_plot.png'  # Change this to your desired path
plt.savefig(plot_path, format='png')

# Display the plot
plt.show()

# Close the Spark session
spark.stop()
