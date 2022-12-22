'''
to run:  
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions correlate_logs_cassandra.py cda78 nasalogs

'''
import re
import math
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(keyspace, table_name):
    
    correlation_df = spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table=table_name, keyspace=keyspace).load()
            
    correlation_df = correlation_df.groupBy("host").agg(functions.count("host").alias("xi"), \
                                                             functions.sum("bytes").alias("yi"))
    
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
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Correlate Logs Cassandra') \
            .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    keyspace = sys.argv[1]
    table_name = sys.argv[2]
    main( keyspace, table_name)