from traceback import print_tb
from xml.etree.ElementTree import Comment

from pyspark import SparkConf, SparkContext
import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
# add more functions as necessary

def get_key_value(in_data):
    key,  value = in_data["subreddit"] , (1, in_data["score"])
    yield key, value

def add_pairs (x,y):
    a, b = x[0], x[1]
    c, d = y[0], y[1]
    return a+c, b+d

def get_key(kv):
    return kv[0]

def get_value(kv):
    return kv[1]

def get_output_format_average(added_value_pair):
    k,v = added_value_pair
    average = v[1]/v[0]
    return(k,average)
    
def get_relative_scores(average_and_comment_info):
    key, value = average_and_comment_info
    avg, comment = value 
    #if you send the entire comment info, instead of sending just what's needed   
    # score = comment['score']
    # author = comment['author']
    score = comment[0]
    author = comment[1]
    return (score/avg,author)

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
    # average logic starts here
    in_data = sc.textFile(inputs)
    in_data = in_data.map(json.loads).cache()
    key_value_pair = in_data.map(lambda x: ( x["subreddit"], (1, x["score"]) ))
    # key_value_pair = in_data.flatMap(get_key_value)
    
    added_value_pair = key_value_pair.reduceByKey(add_pairs)
    average = added_value_pair.map(get_output_format_average)
    average = average.filter(lambda x: x[1] > 0)
    
    # entire comment information
    
    #if you send the entire comment info, instead of sending just what's needed, takes 40s more time
    # comment_info = in_data.map(lambda x: (x['subreddit'], x))
    comment_info = in_data.map(lambda x: (x['subreddit'], (x['score'],x['author'])))
    
    average_and_comment_info = average.join(comment_info)
    
    relative_score = average_and_comment_info.map(get_relative_scores).sortBy(get_key, False)
    # print(relative_score.take(10))
    # print(average_and_comment_info.take(2))
    relative_score.saveAsTextFile(output)
    
if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit Average Spark')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)