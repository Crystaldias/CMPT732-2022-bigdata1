import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

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
    #the records we care about
    etl_data = weather.filter(weather.qflag.isNull())
    etl_data = etl_data.filter(etl_data.station.startswith('CA'))
    etl_data = etl_data.where(etl_data.observation == 'TMAX')
    etl_data = etl_data.withColumn("tmax", etl_data.value/10)
    etl_data = etl_data.select(etl_data.station, etl_data.date, etl_data.tmax)
    etl_data = etl_data.write.json(output, compression='gzip', mode='overwrite')
    
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)