from pyspark import SparkConf, SparkContext
import sys

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wiki popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def components_of_records(line):
    date, lang, title, requests, size = line.split() 
    # for w in line.split():
    yield (date, lang, title, int(requests), size)
    
def get_key_value_pair(records):
    date, lang, title, requests, size = records
    return (date, (requests,title))

def find_max(x, y):
    if y[0] > x[0]:
        return y
    elif y[0]==x[0]:
        return (x[0],x[1]+', '+y[1])
    else:
        return x
        

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t(%s,%s)" % (kv[0], kv[1][0], kv[1][1])


max = 0
#Read the input file(s) in as lines (as in the word count).
text = sc.textFile(inputs)

#Break each line up into a tuple of five things (by splitting around spaces). 
#This would be a good time to convert he view count to an integer. (.map())
records = text.flatMap(components_of_records)

#Remove the records we don't want to consider. (.filter())
filtered_records = records.filter(lambda x: x[1] == "en")
filtered_records = filtered_records.filter(lambda x: x[2] != "Main_Page")
filtered_records = filtered_records.filter(lambda x: x[2].startswith("Special:") != True)

#Create an RDD of key-value pairs. (.map())
rdd_of_records = filtered_records.map(get_key_value_pair)

max_requests = rdd_of_records.sortBy(get_key).reduceByKey(find_max)

#Save as text output (see note below).
max_requests.map(tab_separated).saveAsTextFile(output)
# max_requests.saveAsTextFile(output)


