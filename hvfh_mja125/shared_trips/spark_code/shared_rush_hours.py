#This code will generate a CSV file for shared_trip in each hour for a week 
#Output contains weekday, hours and count
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions as fun, Row, types
from pyspark.sql.functions import col, avg, count, sum


#Filtered out values
license_number = ['HVOOO2','HV0003','HV0004','HV0005']
Vendor_license = {'HVOOO2':'juno','HV0003':'uber','HV0004':'via','HV0005':'lyft'}
#General parameters
input_file = "{v}_tripdata_{y}-{m}.parquet.gzip"
years_taken = range(2019,2024)

trip_count_schema = types.StructType([
    types.StructField('weekday', types.IntegerType()),
    types.StructField('hour', types.IntegerType()),
    types.StructField('count', types.LongType())
    
])

def get_files(company):
    for year in years_taken:
        for month in range(1, 13):
            yield input_file.format(v=company,y=year, m="%02d" % month), year,month

def main(input_folder, output_folder):
    companies = Vendor_license.values()
    final_result = spark.createDataFrame([],schema = trip_count_schema)
    for company in companies:  
        
        available_files = get_files(company)
        for file, year, month  in available_files:              
            path = os.path.join(input_folder+'/'+company, file)            
            if os.path.exists(path): 
                data = spark.read.parquet(path)
                data  = data.where((col('shared_request_flag') == 'Y') | (col('shared_match_flag') == 'Y')).select('pickup_datetime','dropoff_datetime')
                data = data.withColumn('weekday', fun.dayofweek(col('pickup_datetime')))\
                .withColumn('hour', fun.hour(col('pickup_datetime')))
                data = data.groupBy('weekday','hour').agg((count('*')/4.3).alias('count'))
                final_result = final_result.union(data)
                print("Wrote your file")
                
            else:
                print("file not found", file)
    final_result = final_result.groupBy('weekday','hour').agg(avg('count').alias('count'))

    output_path_shared_rush_hours = os.path.join(output_folder, 'shared_rush_hours')
    print("writng file to :", output_path_shared_rush_hours)
    final_result.repartition(1).write.option("header", "true").csv(output_path_shared_rush_hours, mode='overwrite')

 #Give input directory as the folder containing the directory of uber, juno, via and Lyft

if __name__ == '__main__':
    input_folder = sys.argv[1]
    #vendor_name = sys.argv[2]
    output_folder = sys.argv[2]
    spark = SparkSession.builder.appName('ETL inserting date time data.').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_folder, output_folder)
    spark.stop()