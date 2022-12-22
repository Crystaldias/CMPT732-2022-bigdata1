import re
import math
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
def get_key_value(data):
    if data != None:
        host_name, datetime, requested_path, number_of_bytes = data.groups()
        return Row(host_name, int(number_of_bytes))
    # else:
        # return Row("no", 0)
def main(inputs, output):
    # main logic starts here
    in_data = sc.textFile(inputs)
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    regex_result = in_data.map(lambda x: re.search(line_re,x))
    key_value = regex_result.map(get_key_value)
    key_value = key_value.filter(lambda x: x != None)
    correlation_schema = types.StructType([
    types.StructField('host_name', types.StringType(), False),
    types.StructField('number_of_bytes', types.LongType(),False),])
    correlation_df = spark.createDataFrame(key_value, schema = correlation_schema)
    correlation_df = correlation_df.groupBy("host_name").agg(functions.count("host_name").alias("xi"), \
                                                             functions.sum("number_of_bytes").alias("yi"))
    
    correlation_df = correlation_df.withColumn("n",functions.lit(1))\
                                    .withColumn("xi2",correlation_df["xi"]**2)\
                                    .withColumn("yi2",correlation_df["yi"]**2)\
                                    .withColumn("xiyi",correlation_df["xi"]*correlation_df["yi"])
    
    all_sums = correlation_df.groupBy().sum().collect() 
    print(all_sums)                               
    sum_xi, sum_yi, sum_n, sum_xi2, sum_yi2, sum_xiyi = all_sums[0]
    r = (sum_n*sum_xiyi - sum_xi*sum_yi)/(math.sqrt(sum_n*sum_xi2 - sum_xi**2)*math.sqrt(sum_n*sum_yi2 - sum_yi**2))
    r2 = r**2
    print("r = ",r)
    print("r2 = ",r2)
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
