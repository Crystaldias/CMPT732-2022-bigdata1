from pyspark import SparkConf, SparkContext
import sys, re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def words_once(line):
    wordsep = re.split(r'[%s\s]+' % re.escape(string.punctuation),line)
    # words = line.split()
    for w in wordsep:
    # for w in wordsep:
        yield (w.lower(), 1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)




# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs).repartition(10) 
    # text = sc.textFile(inputs).coalesce(10) 
    
    # text = sc.textFile(inputs)
    words = text.flatMap(words_once)
    filtered_words = words.filter(lambda x: x[0] != "")
    wordcount = filtered_words.reduceByKey(add)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Go Bigger')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)