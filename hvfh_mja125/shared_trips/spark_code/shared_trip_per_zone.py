#This code will generate a CSV file for shared_trip_per_zone for each company
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
    types.StructField('drop_zone', types.IntegerType()),
    types.StructField('vendor_id', types.StringType()),
    types.StructField('drop_count', types.LongType())
    
])

def get_files(company):
    for year in years_taken:
        for month in range(1, 13):
            yield input_file.format(v=company,y=year, m="%02d" % month), year,month

def main(input_folder, output_folder):
    number_months = 0
    companies = Vendor_license.values()
    final_result = spark.createDataFrame([],schema = trip_count_schema)
    for company in companies:  
        
        available_files = get_files(company)
        for file, year, month  in available_files:              
            path = os.path.join(input_folder+'/'+company, file)            
            if os.path.exists(path): 
                number_months +=1
                data = spark.read.parquet(path)
                data  = data.where((col('shared_request_flag') == 'Y') | (col('shared_match_flag') == 'Y')).select('drop_zone','vendor_id','pickup_datetime',)
                final_result = final_result.union(data.groupBy('drop_zone','vendor_id').agg(count('*').alias('drop_count')))
                print("Wrote your file")
                
            else:
                print("file not found", file)
    final_result = final_result.groupBy('drop_zone','vendor_id').agg((avg('drop_count')/(30.4)).alias('drop_count'))


    output_path_shared_trip_zone = os.path.join(output_folder, 'shared_drop_per_zone')
    print("writng file to :", output_path_shared_trip_zone)
    final_result.repartition(1).write.option("header", "true").csv(output_path_shared_trip_zone, mode='overwrite')

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