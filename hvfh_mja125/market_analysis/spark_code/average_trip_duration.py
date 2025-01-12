#This code will generate a CSV file for average trip duration
#One file for average trip distance in each zones.
#File for average trip for each of the vendor 
#Only considering trips greater than 300s
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

trip_duration_schema = types.StructType([
    types.StructField('start_zone', types.IntegerType()),
    types.StructField('vendor_id', types.StringType()),
    types.StructField('trip_time', types.FloatType()),
    
])

def get_files(company):
    for year in years_taken:
        for month in range(1, 13):
            yield input_file.format(v=company,y=year, m="%02d" % month), year,month

def main(input_folder, output_folder):
    companies = Vendor_license.values()
    final_result = spark.createDataFrame([],schema = trip_duration_schema)
    for company in companies:  
        
        available_files = get_files(company)
        for file, year, month  in available_files:              
            path = os.path.join(input_folder+'/'+company, file)            
            if os.path.exists(path): 
                data = spark.read.parquet(path)
                data2  = data.select('start_zone','vendor_id','trip_time').filter(col('trip_time')>300)
                final_result = final_result.union(data2.groupBy('start_zone','vendor_id').agg(avg('trip_time').alias('trip_time')))
                data.unpersist()
                data2.unpersist()        
                print("Wrote your file")
                
            else:
                print("file not found", file)
    final_result = final_result.groupBy('start_zone','vendor_id').agg(avg('trip_time').alias('trip_time'))

 
    output_path_revenue_pertrip = os.path.join(output_folder, 'trip_duration')
    print("writng file to :", output_path_revenue_pertrip)
    final_result.repartition(1).write.option("header", "true").csv(output_path_revenue_pertrip, mode='overwrite')

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