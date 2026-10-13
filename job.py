import os, sys, time, shutil
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F
from ingest import ingest_all

def clean(df):
    return df.filter((F.col("trip_distance") > 0) & (F.col("trip_distance") < 100)
                     & (F.col("fare_amount") > 0) & (F.col("fare_amount") < 500)
                     & (F.col("passenger_count") > 0))

def run_federated(spark):
    parts = []
    for name, df in ingest_all(spark).items():
        p = (clean(df).groupBy(F.hour("tpep_pickup_datetime").alias("hour"))
             .agg(F.count("*").alias("trips"), F.sum("fare_amount").alias("fare_sum")))
        parts.append(p)
    allp = parts[0].unionByName(parts[1]).unionByName(parts[2])
    return (allp.groupBy("hour")
            .agg(F.sum("trips").alias("trips"), F.sum("fare_sum").alias("fare_sum"))
            .withColumn("avg_fare", F.round(F.col("fare_sum") / F.col("trips"), 4))
            .orderBy("hour").collect())

def run_centralized(spark):
    clouds = ingest_all(spark)
    shutil.rmtree("central", ignore_errors=True)
    merged = clouds["A"].unionByName(clouds["B"]).unionByName(clouds["C"])
    merged.write.mode("overwrite").parquet("central/trips")   # the copy step
    central = spark.read.parquet("central/trips")
    return (clean(central).groupBy(F.hour("tpep_pickup_datetime").alias("hour"))
            .agg(F.count("*").alias("trips"), F.sum("fare_amount").alias("fare_sum"))
            .withColumn("avg_fare", F.round(F.col("fare_sum") / F.col("trips"), 4))
            .orderBy("hour").collect())

def timed(fn, spark):
    t = time.time()
    rows = fn(spark)
    return rows, round(time.time() - t, 2)

if __name__ == "__main__":
    spark = (SparkSession.builder.master("local[4]").appName("job")
             .config("spark.driver.memory", "6g")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    fed, t_fed = timed(run_federated, spark)
    cen, t_cen = timed(run_centralized, spark)
    same = [(r.hour, r.trips) for r in fed] == [(r.hour, r.trips) for r in cen]
    print("Federated seconds  :", t_fed)
    print("Centralized seconds:", t_cen)
    print("Same results       :", same)
    for r in fed[:5]:
        print(r.hour, r.trips, r.avg_fare)
    spark.stop()
