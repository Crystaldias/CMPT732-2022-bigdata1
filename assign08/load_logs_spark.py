'''
to run:  
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions load_logs_spark.py /courses/732/nasa-logs-2 cda78 nasalogs

'''
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
import os, re, gzip, sys, uuid
import datetime

def get_info(data):
    if data != None:
        host_name, date_time, requested_path, number_of_bytes = data.groups()
        date_time = datetime.datetime.strptime(date_time, '%d/%b/%Y:%H:%M:%S')
        id = str(uuid.uuid1())
        return Row(host_name, date_time, requested_path, int(number_of_bytes), id)
    
def main(inputs, key_space, table_name):
    in_data = sc.textFile(inputs)
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    regex_result = in_data.map(lambda x: re.search(line_re,x))
    nasa_info = regex_result.map(get_info)
    nasa_info = nasa_info.filter(lambda x: x != None)
    nasa_info = nasa_info.repartition(128)
    nasa_schema = types.StructType([
    types.StructField('host', types.StringType(), False),
    types.StructField('datetime', types.TimestampType(), False),
    types.StructField('path', types.StringType(),False),
    types.StructField('bytes', types.IntegerType(),False),
    types.StructField('id', types.StringType(),False),
    ])
    nasa_df = spark.createDataFrame(nasa_info, schema = nasa_schema)
    nasa_df.write.format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=key_space).save(mode="append")
    

if __name__ == '__main__':
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Load Spark example') \
            .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(inputs, keyspace, table_name)