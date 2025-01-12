import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import month

# Initialize Spark Session
spark = SparkSession.builder.appName("Taxi Trip Analysis").getOrCreate()

# Define the path to the directory containing your Parquet files in WSL format
base_path = "/mnt/d/project/testdata"

# Load the datasets
df_2019 = spark.read.parquet(f"{base_path}/yellow_tripdata_2019-*.parquet")
df_2020 = spark.read.parquet(f"{base_path}/yellow_tripdata_2020-*.parquet")
df_2021 = spark.read.parquet(f"{base_path}/yellow_tripdata_2021-*.parquet")
df_2022 = spark.read.parquet(f"{base_path}/yellow_tripdata_2022-*.parquet")
df_2023 = spark.read.parquet(f"{base_path}/yellow_tripdata_2023-*.parquet")

# Aggregate the data by month
monthly_trips_2019 = df_2019.groupBy(month("tpep_pickup_datetime").alias("month")).count().orderBy("month")
monthly_trips_2020 = df_2020.groupBy(month("tpep_pickup_datetime").alias("month")).count().orderBy("month")
monthly_trips_2021 = df_2021.groupBy(month("tpep_pickup_datetime").alias("month")).count().orderBy("month")
monthly_trips_2022 = df_2022.groupBy(month("tpep_pickup_datetime").alias("month")).count().orderBy("month")
monthly_trips_2023 = df_2023.groupBy(month("tpep_pickup_datetime").alias("month")).count().orderBy("month")


# Convert to Pandas dataframe for visualization
monthly_trips_2019_pd = monthly_trips_2019.toPandas().set_index('month').sort_index()
monthly_trips_2020_pd = monthly_trips_2020.toPandas().set_index('month').sort_index()
monthly_trips_2021_pd = monthly_trips_2021.toPandas().set_index('month').sort_index()
monthly_trips_2022_pd = monthly_trips_2022.toPandas().set_index('month').sort_index()
monthly_trips_2023_pd = monthly_trips_2023.toPandas().set_index('month').sort_index()

# ... (After converting to Pandas dataframe for visualization)

# Plot the data with adjusted figure size to prevent x-axis label overlap
plt.figure(figsize=(15, 7))  # Adjust the figure size as needed

# Calculate bar positions
total_width = 0.8
num_bars = 5
bar_width = total_width / num_bars
positions = range(1, 13)  # Assuming 12 months

# Plotting the bar charts for each year with adjusted positions
plt.bar([p - bar_width * 2 for p in positions], monthly_trips_2019_pd['count'], width=bar_width, label='2019')
plt.bar([p - bar_width for p in positions], monthly_trips_2020_pd['count'], width=bar_width, label='2020')
plt.bar(positions, monthly_trips_2021_pd['count'], width=bar_width, label='2021')
plt.bar([p + bar_width for p in positions], monthly_trips_2022_pd['count'], width=bar_width, label='2022')
plt.bar([p + bar_width * 2 for p in positions], monthly_trips_2023_pd['count'], width=bar_width, label='2023')

plt.title('Monthly Taxi Trips from 2019 to 2023')
plt.xlabel('Month')
plt.ylabel('Number of Trips')

# Fix the x-axis labels
months = ['Jan', 'Feb', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
plt.xticks([p for p in positions], months, rotation=45)  # Rotate labels for better readability

# Move the legend to the side
plt.legend(loc='upper right')

# Use tight layout to fit everything nicely
plt.tight_layout()

# Display the plot, or save it to a file
# plt.show()  # Uncomment if you want to show the plot
plt.savefig('/mnt/d/project/testdata/trip_comparison.png')

# Stop the Spark session
spark.stop()


# Plot the data
#plt.figure(figsize=(10, 6))
# Plotting the bar chart for 2019
#plt.bar(monthly_trips_2019_pd.index - 0.2, monthly_trips_2019_pd['count'], width=0.4, label='2019', align='center')

# Plotting the bar chart for 2020
#plt.bar(monthly_trips_2020_pd.index + 0.2, monthly_trips_2020_pd['count'], width=0.4, label='2020', align='center')
#plt.bar(monthly_trips_2021_pd.index + 0.4, monthly_trips_2021_pd['count'], width=0.2, label='2021', align='center')
#plt.bar(monthly_trips_2022_pd.index + 0.4, monthly_trips_2022_pd['count'], width=0.2, label='2021', align='center')
#plt.bar(monthly_trips_2023_pd.index + 0.6, monthly_trips_2023_pd['count'], width=0.2, label='2023', align='center')
#plt.title('Monthly Taxi Trips from 2019 to 2023')
#plt.xlabel('Month')
#plt.ylabel('Number of Trips')
#plt.xticks([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], ['Jan', 'Feb', 'March', 'April', 'May', 'June' 'July', 'September', 'August', 'September', 
                                               #'October', 'November', 'December'])
#plt.legend()
#plt.tight_layout()
#plt.show()
#plt.savefig('/mnt/d/project/testdata/trip_comparison.png')

# Stop the Spark session
#spark.stop()
