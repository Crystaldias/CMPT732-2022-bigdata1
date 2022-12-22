from itertools import groupby
from os import sep
import sys
from time import time
from typing import final


assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types

@functions.udf(returnType=types.StringType())
def get_date_hour(file):
    time_stamp = file.split('/')[-1]
    date = time_stamp.split('-')[1]
    hour = time_stamp.split('-')[2][:2]
    return date+'-'+hour

def main(inputs, output):
    # main logic starts here
    observation_schema = types.StructType([ 
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()), ])

    weather = spark.read.csv(inputs, schema=observation_schema)
    full_table = weather.filter(weather.qflag.isNull()).cache()

    t_max_table = full_table.where(full_table.observation == "TMAX").withColumnRenamed("value","max_value")
    t_min_table = full_table.where(full_table.observation == "TMIN").withColumnRenamed("value","min_value")
    
    range_table = t_max_table.join(t_min_table,["date","station"]) \
                            .withColumn("range",functions.round((t_max_table.max_value/10)-(t_min_table.min_value/10),2)) \
                            .withColumnRenamed("station","range_station")
                            
    max_range_table = range_table.groupBy("date").agg(functions.max("range").alias("max_range"))

    final_result = functions.broadcast(max_range_table)\
                            .join(range_table, (range_table["date"]==max_range_table["date"]) & (range_table["range"]==max_range_table["max_range"]) )\
                            .select(max_range_table.date,range_table.range_station,max_range_table.max_range)
                            
    final_result = final_result.orderBy("date","range_station")
    final_result = final_result.withColumnRenamed("max_range","range").withColumnRenamed("range_station","station")
    final_result.write.csv(output,mode="overwrite")
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output) 