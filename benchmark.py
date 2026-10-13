import os, sys, time, csv
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from job import run_federated, run_centralized

cores = sys.argv[1]
spark = (SparkSession.builder.master(f"local[{cores}]").appName("benchmark")
         .config("spark.driver.memory", "6g")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())

run_federated(spark)  # warm-up run, result discarded

new = not os.path.exists("benchmark_results.csv")
with open("benchmark_results.csv", "a", newline="") as f:
    w = csv.writer(f)
    if new:
        w.writerow(["cores", "mode", "run", "seconds"])
    for run in (1, 2):
        for mode, fn in (("federated", run_federated), ("centralized", run_centralized)):
            t = time.time()
            fn(spark)
            s = round(time.time() - t, 2)
            w.writerow([cores, mode, run, s])
            f.flush()
            print("cores", cores, mode, "run", run, ":", s, "s")
spark.stop()
