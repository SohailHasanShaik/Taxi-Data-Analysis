from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
import os
import sys

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ML_Model") \
    .getOrCreate()

taxi_color = sys.argv[1]

# output folder to save the result
output_folder_path = f'outputs/{taxi_color}_taxi/average_total_amount_rfm'
os.makedirs(output_folder_path, exist_ok=True)

# read the datasets
df = spark.read.csv(f'outputs/{taxi_color}_taxi/avg_amounts_speed_time_passenger_counts/final_result', header = True)

# convert the values to float type
df = df.withColumn("avg_time_diff", F.col("avg_time_diff").cast(FloatType()))
df = df.withColumn("avg_tip_amount", F.col("avg_tip_amount").cast(FloatType()))
df = df.withColumn("avg_total_amount", F.col("avg_total_amount").cast(FloatType()))
df = df.withColumn("total_passenger_count", F.col("total_passenger_count").cast(FloatType()))
df = df.withColumn("avg_speed", F.col("avg_speed").cast(FloatType()))

df = df.withColumn("month_year_date", F.to_date(F.concat_ws("-", F.split(df["month_year"], "-")[1], 
                                                             F.split(df["month_year"], "-")[0], 
                                                             F.lit("01")), "yyyy-M-dd"))

# Create the training set (from 1 January 2017 to 31 December 2019)
train_df = df.filter((F.col("month_year_date") >= F.lit("2017-01-01")) & (F.col("month_year_date") <= F.lit("2019-12-31")))

# Create the test set (from 1 January 2020 to 31 December 2023)
test_df = df.filter((F.col("month_year_date") >= F.lit("2020-01-01")) & (F.col("month_year_date") <= F.lit("2023-12-31")))

# First, use VectorAssembler to combine these columns into a single feature vector column
assembler = VectorAssembler(inputCols=["avg_time_diff", "avg_tip_amount", "total_passenger_count", "avg_speed"], outputCol="features")

# Apply the StandardScaler
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

# Create a pipeline with the assembler and scaler
pipeline = Pipeline(stages=[assembler, scaler])

# Fit the pipeline to the data
pipelineModel = pipeline.fit(train_df)

# Transform the data
train_df = pipelineModel.transform(train_df)
test_df = pipelineModel.transform(test_df)

# Define the model
rf = RandomForestRegressor(featuresCol="scaledFeatures", labelCol="avg_total_amount")

# Fit the model on the training data
model = rf.fit(train_df)

# make predictions and save the results
predictions_on_train = model.transform(train_df)
predicted_train_data = predictions_on_train.select("month_year", "avg_total_amount", "prediction").toPandas()
predicted_train_data.to_csv(f'{output_folder_path}/average_total_amount_prediction_for_train_set.csv', index = False)

predictions_on_test = model.transform(test_df)
predicted_test_data = predictions_on_test.select("month_year", "avg_total_amount", "prediction").toPandas()
predicted_test_data.to_csv(f'{output_folder_path}/average_total_amount_prediction_for_test.csv', index = False)