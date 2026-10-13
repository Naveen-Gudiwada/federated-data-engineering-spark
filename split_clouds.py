import os, sys
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .master("local[4]")
         .appName("split_clouds")
         .config("spark.driver.memory", "6g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

df = spark.read.parquet("data/raw/*.parquet")
df = df.withColumn("cloud", F.col("PULocationID") % 3)

df.filter("cloud = 0").drop("cloud").write.mode("overwrite").parquet("cloud_A/trips")
df.filter("cloud = 1").drop("cloud").write.mode("overwrite").option("header", True).csv("cloud_B/trips")
df.filter("cloud = 2").drop("cloud").write.mode("overwrite").json("cloud_C/trips")

spark.stop()
