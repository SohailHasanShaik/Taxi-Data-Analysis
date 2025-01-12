#Number trips without tip and with tip
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
    types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
    types.StructField('tip_count', types.LongType()),
    types.StructField('without_tip_count', types.LongType())
    
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
                tip_count  = data.filter(col('tips')>0).count()
                no_tip_count = data.filter(col('tips')==0).count()
                new_row = [year,month,tip_count,no_tip_count]
                final_result = final_result.union(spark.createDataFrame([new_row], schema=trip_count_schema))
                print("Wrote your file")
                
            else:
                print("file not found", file)
    
    output_path_tips_trend = os.path.join(output_folder, 'tips_trend')
    print("writng file to :", output_path_tips_trend)
    final_result.repartition(1).write.option("header", "true").csv(output_path_tips_trend, mode='overwrite')

 #Give input directory as the folder containing the directory of uber, juno, via and Lyft

if __name__ == '__main__':
    input_folder = sys.argv[1]
    output_folder = sys.argv[2]
    spark = SparkSession.builder.appName('ETL inserting date time data.').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_folder, output_folder)
    spark.stop()