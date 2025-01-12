#This code generates number of shared trips per month per company
#input : Output from etl-vendor
import sys
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions as fun, Row, types
from pyspark.sql.functions import col, avg, count, sum
from functools import reduce


#Filtered out values
license_number = ['HVOOO2','HV0003','HV0004','HV0005']
Vendor_license = {'HVOOO2':'juno','HV0003':'uber','HV0004':'via','HV0005':'lyft'}
#General parameters
input_file = "{v}_tripdata_{y}-{m}.parquet.gzip"
years_taken = range(2019,2024)

trip_count_schema = types.StructType([
    types.StructField('company', types.StringType()),
    types.StructField('month', types.IntegerType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('pickup_count', types.LongType())
    
])

def get_files(company):
    for year in years_taken:
        for month in range(1, 13):
            yield input_file.format(v=company,y=year, m="%02d" % month), year,month

def main(input_folder, output_folder):
    companies = Vendor_license.values()
    data_frames = []

    for company in companies:
        available_files = get_files(company)

        for file, year, month in available_files:
            path = os.path.join(input_folder, company, file)

            if os.path.exists(path):
                data = spark.read.parquet(path)
                shared_count = data.where((col('shared_request_flag') == 'Y') | (col('shared_match_flag') == 'Y')).count()
                new_row = [company, month, year, shared_count]
                data_frames.append(spark.createDataFrame([new_row], schema=trip_count_schema))
                print("Got your file", file)
            else:
                print("File not found", file)

    # Perform a single union for all DataFrames
    final_result = data_frames[0] if data_frames else None

    if len(data_frames) > 1:
        final_result = reduce(lambda df1, df2: df1.unionByName(df2), data_frames[1:])

    # Write the final result to CSV
    if final_result is not None:
        output_path_shared_count = os.path.join(output_folder, 'shared_trip_count')
        print("Writing file to:", output_path_shared_count)
        final_result.repartition(1).write.option("header", "true").csv(output_path_shared_count, mode='overwrite')

if __name__ == '__main__':
    input_folder = sys.argv[1]
    output_folder = sys.argv[2]
    spark = SparkSession.builder.appName('ETL inserting date time data.').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_folder, output_folder)
    spark.stop()