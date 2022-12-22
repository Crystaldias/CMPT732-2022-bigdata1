from itertools import groupby
from os import sep
import sys
from time import time
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types

@functions.udf(returnType=types.StringType())
def get_date_hour(file):
    time_stamp = file.split('/')[-1]
    date = time_stamp.split('-')[1]
    hour = time_stamp.split('-')[2][:2]
    return date+'-'+hour

def main(inputs, output):
    # main logic starts here
    wikipedia_schema = types.StructType([ 
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.IntegerType()),
    types.StructField('size', types.LongType()),
    ])

    full_wiki_page = spark.read.csv(inputs, schema=wikipedia_schema, sep = ' ').withColumn('hour', get_date_hour(functions.input_file_name()))
    
    wiki_page = full_wiki_page.filter(full_wiki_page.language == 'en').filter(full_wiki_page.title.startswith('Special:')==False).filter(full_wiki_page.title!='Main_Page').cache()
    
    popular_page = wiki_page.groupBy('hour').agg(functions.max(wiki_page["views"]).alias("popular_views")) \
                            .withColumnRenamed("hour","popular_hour")

    max_joined_wiki_page = wiki_page \
                                    .join(popular_page, [(wiki_page.views==popular_page.popular_views) & (wiki_page.hour==popular_page.popular_hour)], "inner") \
                                    .select(wiki_page.hour,wiki_page.title,wiki_page.views) \
                                    .orderBy(wiki_page.hour,wiki_page.title)
    max_joined_wiki_page.explain()
    max_joined_wiki_page.write.json(output,mode="overwrite")
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikipedia_popular_df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)