import os, sys
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F

COLS = {
    "VendorID": "int", "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp", "passenger_count": "int",
    "trip_distance": "double", "PULocationID": "int",
    "DOLocationID": "int", "fare_amount": "double",
    "tip_amount": "double", "total_amount": "double",
}

def normalize(df, cloud_name):
    df = df.select([F.col(c).cast(t).alias(c) for c, t in COLS.items()])
    return df.withColumn("source_cloud", F.lit(cloud_name))

def ingest_all(spark):
    a = spark.read.parquet("cloud_A/trips")
    b = spark.read.option("header", True).csv("cloud_B/trips")
    c = spark.read.json("cloud_C/trips")
    return {"A": normalize(a, "A"), "B": normalize(b, "B"), "C": normalize(c, "C")}

if __name__ == "__main__":
    spark = (SparkSession.builder.master("local[4]").appName("ingest")
             .config("spark.driver.memory", "6g")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    clouds = ingest_all(spark)
    for name, df in clouds.items():
        print("Cloud", name, "rows:", df.count())
        df.printSchema()
    spark.stop()
