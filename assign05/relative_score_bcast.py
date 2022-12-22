from traceback import print_tb
from xml.etree.ElementTree import Comment

from pyspark import SparkConf, SparkContext
import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
# add more functions as necessary
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
    return k,average
    
def get_relative_scores(comment_info, broadcast_average):
    comment = comment_info
    reddit_average_dict = broadcast_average.value
    avg = reddit_average_dict[comment[0]]
    score, author = comment[1]
    return (score/avg,author)

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
    # average logic starts here
    in_data = sc.textFile(inputs)
    in_data = in_data.map(json.loads).cache()
    key_value_pair = in_data.map(lambda x: ( x["subreddit"], (1, x["score"]) ))
    added_value_pair = key_value_pair.sortBy(get_key).reduceByKey(add_pairs)
    average = added_value_pair.map(get_output_format_average)
    average = average.filter(lambda x: x[1] > 0)
    average = dict(average.collect())
    # entire comment information
    broadcast_average = sc.broadcast(average)
    comment_info = in_data.map(lambda x: (x['subreddit'], (x['score'],x['author'])))
    relative_score = comment_info.map(lambda x: get_relative_scores(x, broadcast_average)).sortBy(get_key, False)
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