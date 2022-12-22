import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

from pyspark.ml.feature import VectorAssembler, SQLTransformer, Binarizer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator


def create_data():
    data = spark.range(100000)
    data = data.select(
        (functions.rand()*100 - 50).alias('length'),
        (functions.rand()*100 - 50).alias('width'),
        (functions.rand()*100 - 50).alias('height'),
    )
    data = data.withColumn('volume', data['length']*data['width']*data['height'])
    training, validation = data.randomSplit([0.75, 0.25])
    return training, validation


def regression():
    training, validation = create_data()
    
    assemble_features = VectorAssembler(inputCols=['length', 'width', 'height'], outputCol='features')
    regressor = GBTRegressor(featuresCol='features', labelCol='volume', maxIter=100, maxDepth=5)
    pipeline = Pipeline(stages=[assemble_features, regressor])
    
    model = pipeline.fit(training)
    predictions = model.transform(validation)
    predictions.show()
    
    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='volume',
        metricName='r2'
    )
    r2 = r2_evaluator.evaluate(predictions)
    print(r2)

    
def classification():
    training, validation = create_data()

    to_categorical = SQLTransformer(statement='''
        SELECT
            length>0 AS l_sign,
            width>0 AS w_sign,
            height>0 AS h_sign,
            volume
        FROM __THIS__
    ''')
    binarizer = Binarizer(inputCol='volume', outputCol='v_sign', threshold=0.0)
    assemble_features = VectorAssembler(inputCols=['l_sign', 'w_sign', 'h_sign'], outputCol='features')
    classifier = GBTClassifier(featuresCol='features', labelCol='v_sign', maxIter=100, maxDepth=5)
    pipeline = Pipeline(stages=[to_categorical, binarizer, assemble_features, classifier])
    
    model = pipeline.fit(training)
    predictions = model.transform(validation)
    predictions.show()
    
    r2_evaluator = BinaryClassificationEvaluator(
        rawPredictionCol='prediction', labelCol='volume',
    )
    r2 = r2_evaluator.evaluate(predictions)
    print(r2)

    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('ML demo').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    
    regression()
    classification()