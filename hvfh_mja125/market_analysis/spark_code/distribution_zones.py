#This code will generate a CSV file which will have vendor_id, 
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
    types.StructField('start_zone', types.IntegerType()),
    types.StructField('vendor_id', types.StringType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
    types.StructField('pickup_count', types.LongType())
    
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
                data  = data.select('start_zone','vendor_id','pickup_datetime').withColumn('year', fun.year(col('pickup_datetime')))\
                .withColumn('month', fun.month(col('pickup_datetime')))
                final_result = final_result.union(data.groupBy('start_zone','vendor_id','year','month').agg(count('*').alias('pickup_count')))
                print("Wrote your file")
                
            else:
                print("file not found", file)
    final_result = final_result.groupBy('start_zone','vendor_id','year','month').agg(sum('pickup_count').alias('pickup_count'))

    # output_per_trip_revenue = os.path.join(output_folder, output_file_1)
    # output_trip_count = os.path.join(output_folder+'/uber', output_file_2)
    output_path_revenue_pertrip = os.path.join(output_folder, 'pickup_per_zone')
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