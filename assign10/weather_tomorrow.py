import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.ml import PipelineModel
from datetime import datetime


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def test_model(model_file):
    # get the data
    test_tmax = spark.createDataFrame(
        [
            ("SECB", datetime.strptime("2022-11-17", "%Y-%m-%d"), 49.2771, -122.9146, 330.0, 12.0),
            ("SECB", datetime.strptime("2022-11-18", "%Y-%m-%d"), 49.2771, -122.9146, 330.0, 12.0)
        ], schema=tmax_schema)

    # load the model
    model = PipelineModel.load(model_file)

    # use the model to make predictions
    predictions = model.transform(test_tmax)
    print(predictions.show())

    prediction = predictions.collect()[0][7]
    print('Predicted tmax tomorrow:', prediction)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    # print(model.stages[-1].featureImportances)


if __name__ == '__main__':
    model_file = sys.argv[1]
    spark = SparkSession.builder.appName('weather tomorrow').getOrCreate()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    test_model(model_file)