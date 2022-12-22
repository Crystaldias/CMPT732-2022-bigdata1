import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types


def main(topic):

    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))
    
    
    df = values.select(functions.split(values['value'], ' ').getItem(0).alias('x'),
                       functions.split(values['value'], ' ').getItem(1).alias('y'))
    
    df = df.select(df['x'], df['y'], 
                   (df['x'] * df['y']).alias('xy'), 
                   (df['x'] ** 2).alias('x^2'), 
                   functions.lit(1).alias('n'))
    
    intermediate_df = df.agg(functions.sum(df['x']).alias('x'),
                            functions.sum(df['y']).alias('y'),
                            functions.sum(df['xy']).alias('xy'),
                            functions.sum(df['x^2']).alias('x^2'),
                            functions.sum(df['n']).alias('n'))
    
    slope_df = intermediate_df.withColumn('slope', (intermediate_df['xy'] - (1/intermediate_df['n'] * intermediate_df['x'] * intermediate_df['y'])) /
                                      (intermediate_df['x^2'] - (1/intermediate_df['n'] * (intermediate_df['x'] ** 2))))
    intercept_df = slope_df.withColumn('intercept', (slope_df['y'] / slope_df['n']) - (slope_df['slope'] * (slope_df['x'] / slope_df['n'])))
    results_df = intercept_df.select(intercept_df['slope'], intercept_df['intercept'])
    stream = results_df.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(100)
    
if __name__ == '__main__':
    topic = sys.argv[1]
    spark = SparkSession.builder.appName('read stream').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(topic)