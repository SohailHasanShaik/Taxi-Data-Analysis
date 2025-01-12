import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions as fun
from pyspark.sql.functions import col

import numpy as np

#Filtered out values
license_number = ['HVOOO2','HV0003','HV0004','HV0005']

#General parameters
input_file = "fhvhv_tripdata_{y}-{m}.parquet.gzip"
years_taken = range(2019,2024)

def get_files():
    for year in years_taken:
        for month in range(1, 13):
            yield input_file.format(y=year, m="%02d" % month)

def main(input_folder, output_folder):
    available_files = get_files()
    for file  in available_files:        
        output_file = 'time_analysis_'+file
        path = os.path.join(input_folder, file)
        if os.path.exists(path): 
            data = spark.read.parquet(path)
            #Dropping 'wav_match_flag','wav_request_flag','tolls'
            #'bcf','sales_tax','orginating_base_num'
            data = data.withColumn('year', fun.year(col('pickup_datetime')))\
                .withColumn('month', fun.month(col('pickup_datetime')))\
                .withColumn('monthday', fun.dayofmonth(col('pickup_datetime')))\
                .withColumn('weekday', fun.dayofweek(col('pickup_datetime')))\
                .withColumn('hour', fun.hour(col('pickup_datetime')))\
                .withColumn('date', fun.to_date(col('pickup_datetime')))
            
            output_path = os.path.join(output_folder,output_file)
            print("Wrote your file")
            data.show(10)
            data.repartition(1).write.option("header", "true").option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").parquet(output_path, compression='gzip', mode='overwrite')
            #data.write.parquet(output_path, compression='gzip', mode='overwrite')
        else:
            print("file not found", file)
                   
if __name__ == '__main__':
    input_folder = sys.argv[1]
    output_folder = sys.argv[2]
    spark = SparkSession.builder.appName('ETL inserting date time data.').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_folder, output_folder)
    spark.stop()