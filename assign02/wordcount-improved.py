from pyspark import SparkConf, SparkContext
import sys, re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

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

text = sc.textFile(inputs)
words = text.flatMap(words_once)
filtered_words = words.filter(lambda x: x[0] != "")
wordcount = filtered_words.reduceByKey(add)

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)