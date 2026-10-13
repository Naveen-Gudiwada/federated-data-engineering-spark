import os, sys, time, csv
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from ingest import ingest_all
from job import clean

spark = (SparkSession.builder.master("local[4]").appName("ml")
         .config("spark.driver.memory", "6g")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())

c = ingest_all(spark)
df = c["A"].unionByName(c["B"]).unionByName(c["C"])
df = (clean(df)
      .withColumn("hour", F.hour("tpep_pickup_datetime"))
      .withColumn("dow", F.dayofweek("tpep_pickup_datetime"))
      .select("trip_distance", "passenger_count", "hour", "dow", "fare_amount")
      .sample(0.1, seed=42))

feats = ["trip_distance", "passenger_count", "hour", "dow"]
data = (VectorAssembler(inputCols=feats, outputCol="features").transform(df)
        .select("features", F.col("fare_amount").alias("label")))
train, test = data.randomSplit([0.8, 0.2], seed=42)
train.cache()
print("Train rows:", train.count(), "| Test rows:", test.count())

models = {"LinearRegression": LinearRegression(),
          "GBTRegressor": GBTRegressor(maxIter=20, maxDepth=5, seed=42)}
rows = []
for name, algo in models.items():
    t = time.time()
    pred = algo.fit(train).transform(test)
    rmse = RegressionEvaluator(metricName="rmse").evaluate(pred)
    r2 = RegressionEvaluator(metricName="r2").evaluate(pred)
    secs = round(time.time() - t, 2)
    rows.append([name, round(rmse, 3), round(r2, 4), secs])
    print(name, "| RMSE:", round(rmse, 3), "| R2:", round(r2, 4), "| seconds:", secs)

with open("ml_results.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["model", "rmse", "r2", "seconds"])
    w.writerows(rows)
spark.stop()
