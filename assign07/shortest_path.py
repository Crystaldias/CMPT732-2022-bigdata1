from pickle import TRUE
import sys
import re
from typing import final
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types

def get_key_value(line):
    node,connected_nodes = line.split(':')
    for i in re.split(r'\s',connected_nodes):
        if i != '':
            yield(node,i)

def get_new_path(x):
    new_paths=[]
    for node in x[1][1]:
        new_path=(node,[x[0],int(x[1][0][1])+1])
        new_paths.append(new_path)
    return new_paths

def get_shortest_path(a,b):
    if a[1]<b[1]:
        return a
    else:
        return b

def main(inputs, output, source, destination):
    
    in_data=sc.textFile(inputs)
    key_value=in_data.flatMap(get_key_value).cache()
    known_paths=sc.parallelize([(source,['-','0'])])

    for i in range(6):
        inter_path=known_paths.join(key_value)
        new_paths=inter_path.flatMap(get_new_path)
        known_paths=known_paths.union(new_paths)
        known_paths=known_paths.reduceByKey(get_shortest_path)
        known_paths.saveAsTextFile(output + '/iter-' + str(i))
        if known_paths.lookup(destination):
            break

    known_paths.cache()
    path=[destination]
    for i in range(known_paths.lookup(destination)[0][1]):
        required_node=known_paths.lookup(destination)[0][0]
        path.insert(0,required_node)
        destination=required_node

    final_result=sc.parallelize(path)
    final_result.saveAsTextFile(output + '/path')

if __name__ == '__main__':
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    main(inputs, output, source, destination)