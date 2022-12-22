from re import A, MULTILINE

from pyspark import SparkConf, SparkContext
import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
# add more functions as necessary

def get_key_value(in_data):
    key,  value = in_data["subreddit"] , (1, in_data["score"])
    return key, value

def add_pairs (x,y):
    a, b = x[0], x[1]
    c, d = y[0], y[1]
    return a+c, b+d

def get_key(kv):
    return kv[0]

def get_value(kv):
    return kv[1]

def get_output_format(kv):
    # WRONGCODEtuple_info = added_value_pair.collect()
    k,v = kv
    print(kv)
    average = v[1]/v[0]
    return '[%s %s]' % (k,str(average))


def main(inputs, output):
    # main logic starts here
    # WRONG CODEin_data = sc.parallelize([json.loads(line) for line in open(inputs,'r')])
    in_data = sc.textFile(inputs)
    in_data = in_data.map(json.loads)
    key_value_pair = in_data.map(get_key_value)
    added_value_pair = key_value_pair.reduceByKey(add_pairs)
    # WRONG CODEaverage = get_output_format(added_value_pair)
    # with open(output, 'w') as outfile:
    #     json.dump(average, outfile)
    outdata = added_value_pair.sortBy(get_key).map(get_output_format)
    outdata.saveAsTextFile(output)
    
if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit Average Spark')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)