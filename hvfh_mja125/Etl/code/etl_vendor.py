#This code is for generating vendor specfic data.
#There are 4 vendors for app based taxi's in Newyork
# Juno, Uber. Via and Lyft

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions as fun
from pyspark.sql.functions import col


#Filtered out values
license_number = ['HVOOO2','HV0003','HV0004','HV0005']
Vendor_license = {'HVOOO2':'juno','HV0003':'uber','HV0004':'via','HV0005':'lyft'}
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
        
        path = os.path.join(input_folder, file)
        if os.path.exists(path): 
            file = file.replace('fhvhv',"")
            output_file_1 = 'juno'+file
            output_file_2 = 'uber'+file
            output_file_3 = 'via'+file
            output_file_4 = 'lyft'+file
            data = spark.read.parquet(path)
            #Dropping 'wav_match_flag','wav_request_flag','tolls'
            #'bcf','sales_tax','orginating_base_num'
            juno_data = data.filter(col('vendor_id') == license_number[0])
            uber_data = data.filter(col('vendor_id') == license_number[1])
            via_data = data.filter(col('vendor_id') == license_number[2])
            lyft_data = data.filter(col('vendor_id') == license_number[3])
            output_path_juno = os.path.join(output_folder+'/juno', output_file_1)
            output_path_uber = os.path.join(output_folder+'/uber', output_file_2)
            output_path_via = os.path.join(output_folder+'/via', output_file_3)
            output_path_lyft = os.path.join(output_folder+'/lyft', output_file_4)
            os.makedirs(output_path_juno, exist_ok=True)
            os.makedirs(output_path_uber, exist_ok=True)
            os.makedirs(output_path_via, exist_ok=True)
            os.makedirs(output_path_lyft, exist_ok=True)

            print("Wrote your file")
            juno_data.repartition(1).write.option("header", "true").option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").parquet(output_path_juno, compression='gzip', mode='overwrite')
            uber_data.repartition(1).write.option("header", "true").option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").parquet(output_path_uber, compression='gzip', mode='overwrite')
            via_data.repartition(1).write.option("header", "true").option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").parquet(output_path_via, compression='gzip', mode='overwrite')
            lyft_data.repartition(1).write.option("header", "true").option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").parquet(output_path_lyft, compression='gzip', mode='overwrite')
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