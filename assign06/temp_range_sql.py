from itertools import groupby
from os import sep
import sys
from time import time


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
    weather.createOrReplaceTempView("weather")
    
    full_table = spark.sql("SELECT * FROM weather WHERE qflag is NULL")
    full_table.createOrReplaceTempView("full_table")
    
    t_max_table = spark.sql("SELECT date, station, value as max_value FROM full_table WHERE observation == 'TMAX' ")
    t_max_table.createOrReplaceTempView("t_max_table")
    
    t_min_table = spark.sql("SELECT  date, station, value as min_value  FROM full_table WHERE observation == 'TMIN' ")
    t_min_table.createOrReplaceTempView("t_min_table")
    
    min_max_table = spark.sql("SELECT t_max_table.date, t_max_table.station, t_max_table.max_value, t_min_table.min_value \
                            FROM t_max_table \
                            INNER JOIN t_min_table \
                            ON t_max_table.date == t_min_table.date \
                            AND t_max_table.station == t_min_table.station")
    min_max_table.createOrReplaceTempView("min_max_table")
    
    range_table = spark.sql("SELECT date, station as range_station, round(max_value/10-min_value/10,2) as range FROM min_max_table")
    range_table.createOrReplaceTempView("range_table")

    max_range_table = spark.sql("SELECT date, max(range) as max_range FROM range_table GROUP BY date")
    max_range_table.createOrReplaceTempView("max_range_table")

    final_result = spark.sql("SELECT max_range_table.date, range_table.range_station as station, max_range_table.max_range \
                            FROM max_range_table \
                            INNER JOIN range_table \
                            ON range_table.date == max_range_table.date \
                            AND range_table.range == max_range_table.max_range \
                            ")
    final_result.createOrReplaceTempView("final_result")
    
    final_result = spark.sql("SELECT * FROM final_result ORDER BY date ASC, station ASC")
    final_result.createOrReplaceTempView("final_result")
    
    final_result.write.csv(output,mode="overwrite")
    
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikipedia_popular_sql').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output) 