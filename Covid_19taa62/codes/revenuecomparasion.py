from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, to_date
import matplotlib.pyplot as plt

# Initialize Spark Session
spark = SparkSession.builder.appName("Taxi Trip Revenue Analysis").getOrCreate()

# Path to the directory containing your Parquet files in WSL format
base_path = "/mnt/d/project/testdata"
years = ['2019', '2020', '2021', '2022', '2023']
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

# Read all the parquet files
dfs = []
for year in years:
    for month in months:
        file_path = f"{base_path}/yellow_tripdata_{year}-{month}.parquet"
        try:
            df = spark.read.parquet(file_path)
            dfs.append(df)
        except Exception as e:  # Catching a generic exception for simplicity
            print(f"Could not read file: {file_path} due to {e}")

# Check if any dataframes have been read successfully
if dfs:
    # Union all the dataframes into one
    all_trips_df = dfs[0]
    for df in dfs[1:]:
        all_trips_df = all_trips_df.union(df)

    # Convert the pickup datetime to a date format
    all_trips_df = all_trips_df.withColumn("pickup_date", to_date("tpep_pickup_datetime"))

    # Define the pandemic start date
    pandemic_start_date = "2020-03-01"

    # Filter data for pre-pandemic and post-pandemic periods
    pre_pandemic_df = all_trips_df.filter(all_trips_df["pickup_date"] < pandemic_start_date)
    post_pandemic_df = all_trips_df.filter(all_trips_df["pickup_date"] >= pandemic_start_date)

    # Calculate total revenue for each period
    total_revenue_pre_pandemic = pre_pandemic_df.agg(_sum("total_amount")).collect()[0][0]
    total_revenue_post_pandemic = post_pandemic_df.agg(_sum("total_amount")).collect()[0][0]

    # Revenue data
revenues = [total_revenue_pre_pandemic, total_revenue_post_pandemic]
labels = ['Pre-Pandemic', 'Post-Pandemic']

# Create bar plot
plt.figure(figsize=(10, 6))
bars = plt.bar(labels, revenues, color=['blue', 'green'])

# Main title and subtitle
plt.suptitle('Taxi Trip Revenue Analysis', fontsize=16)
plt.title('Comparison of Total Revenue Before and After the COVID-19 Pandemic', fontsize=10)

# X and Y labels
plt.xlabel('Period', fontsize=12)
plt.ylabel('Total Revenue (in billions)', fontsize=12)

# Adding the text labels on the bars
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval, f'${yval:,.2f}', ha='center', va='bottom', fontsize=10)

# Additional descriptions or annotations
plt.text(1, total_revenue_pre_pandemic / 2, 'Significant drop in revenue due to pandemic', ha='center', color='white', fontsize=8)
plt.text(1, total_revenue_post_pandemic / 2, 'Recovery phase with gradual increase in trips', ha='center', color='white', fontsize=8)

# Set the y-axis to show numbers as billions
plt.gca().get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x/1e9))))

# Save the figure
output_path = '/mnt/d/project/testdata/taxi_revenue_comparison.png'
plt.savefig(output_path, bbox_inches='tight')

# Show the plot if desired
plt.show()

# Close the plot to free up memory
plt.close()

# Stop the Spark session
spark.stop()
