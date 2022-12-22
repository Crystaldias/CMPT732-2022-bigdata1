from re import A, MULTILINE
from pyspark import SparkConf, SparkContext
import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json

def get_select_fields(in_data):
    return {'subreddit':in_data['subreddit'],'score':in_data['score'], 'author':in_data['author']}

def main(inputs, output):
    # main logic starts here
    in_data = sc.textFile(inputs)
    in_data = in_data.map(json.loads)
    select_fields = in_data.map(get_select_fields)
    only_with_e = select_fields.filter(lambda x:x['subreddit'].find('e') != -1).cache()
    # only_with_e = select_fields.filter(lambda x:x['subreddit'].find('e') != -1)
    positive_reddits = only_with_e.filter(lambda x:x['score']>0)
    positive_reddits.map(json.dumps).saveAsTextFile(output + '/positive')
    negative_reddits = only_with_e.filter(lambda x:x['score']<=0)
    negative_reddits.map(json.dumps).saveAsTextFile(output + '/negative')
    # print(p.take(10))
    
if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit Average Spark')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)