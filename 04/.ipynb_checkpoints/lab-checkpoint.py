import os
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession
from pysprak.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("LR-pyspark").getOrCreate()

data [
        (3.5, 5.8),
        (6.7, 9.7),
        (9.8, 11,2),
        (14.12, 16.8),
        (19.0, 25.0)
    ]

df = spark.createDataFrame(data, schema = ["x", "y"])

assembler= VectorAssembler(inputcols = ['x'], coutputCol = "features")
df_features = assesmbler.transform(df).select("features", 'x', 'y')

df_features.show()



