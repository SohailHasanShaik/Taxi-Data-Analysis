from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year, mean, to_date, col, sum as _sum, corr
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder.appName("Covid19 Taxi Fare Correlation").getOrCreate()

# Base directory containing the taxi datasets
taxi_data_directory = "/mnt/d/project/testdata/"


# Initialize an empty DataFrame before the loop
taxi_data_aggregated = pd.DataFrame()

# Inside your loop, after calculating monthly_avg_fare
monthly_avg_fare = ...  # Your existing code to calculate monthly_avg_fare
taxi_data_aggregated = taxi_data_aggregated.append(monthly_avg_fare, ignore_index=True)

# Load and preprocess the taxi data
taxi_data_aggregated = pd.DataFrame()

# Assuming data is available from 2019 to 2023
for year_value in range(2019, 2024):
    for month_value in range(1, 13):
        file_path = f"{taxi_data_directory}yellow_tripdata_{year_value}-{str(month_value).zfill(2)}.parquet"
        monthly_taxi_data = spark.read.parquet(file_path)
        
        # Calculate the average fare per month
        monthly_avg_fare = monthly_taxi_data.groupBy(month("tpep_pickup_datetime").alias("month"),
                                                     year("tpep_pickup_datetime").alias("year")) \
                                           .agg(mean("total_amount").alias("avg_fare")).toPandas()
        
        taxi_data_aggregated = taxi_data_aggregated.append(monthly_avg_fare, ignore_index=True)

# Load and preprocess the COVID-19 cases data
covid_data_path = "/mnt/data/cases-by-day.csv"
covid_df = spark.read.csv(covid_data_path, header=True, inferSchema=True) \
                     .withColumn("date", to_date(col("date_of_interest"), "MM/dd/yyyy")) \
                     .groupBy(month("date").alias("month"),
                              year("date").alias("year")) \
                     .agg(_sum("CASE_COUNT").alias("monthly_cases")).toPandas()

# Merge the datasets
combined_df = pd.merge(taxi_data_aggregated, covid_df, on=["month", "year"])

# Perform correlation analysis
correlation = combined_df['avg_fare'].corr(combined_df['monthly_cases'])

# Visualization with scatter plot
plt.figure(figsize=(10, 6))
sns.scatterplot(data=combined_df, x='monthly_cases', y='avg_fare', hue='year', palette='viridis')
plt.title('Correlation between Monthly COVID-19 Cases and Average Taxi Fare')
plt.xlabel('Monthly COVID-19 Cases')
plt.ylabel('Average Taxi Fare ($)')
plt.legend(title='Year')
plt.grid(True)
plt.tight_layout()
plt.show()

# Visualization with heatmap
heatmap_data = combined_df.pivot_table(index='month', columns='year', values='avg_fare')
plt.figure(figsize=(12, 8))
sns.heatmap(heatmap_data, annot=True, cmap='coolwarm', fmt=".2f")
plt.title('Heatmap of Average Taxi Fare Over Months and Years')
plt.xlabel('Year')
plt.ylabel('Month')
plt.show()

# Write the analysis
print(f"The correlation coefficient between average taxi fare and monthly COVID-19 cases is: {correlation}")
print("This coefficient suggests that there is a [positive/negative/no] correlation between the number of COVID-19 cases and the average taxi fare.")
print("From the scatter plot, we can observe that [...]")
print("The heatmap shows the variation in average taxi fare over different months and years, providing a visual representation of [...]")

