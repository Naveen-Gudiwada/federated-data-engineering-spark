import os, sys
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local[4]")
         .appName("test")
         .config("spark.driver.memory", "6g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

spark.range(1000000).selectExpr("sum(id)").show()
spark.stop()
