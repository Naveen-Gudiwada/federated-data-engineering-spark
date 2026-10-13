import os, sys, csv, datetime
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F
from ingest import ingest_all

SALT = "fed-spark-demo-salt"
HASH_COLS = ["PULocationID", "DOLocationID"]
POLICY = {
    "admin":   {"hash": [],        "drop": []},
    "analyst": {"hash": HASH_COLS, "drop": ["tip_amount"]},
}
AUDIT_FILE = "audit_log.csv"

def apply_policy(df, role):
    p = POLICY[role]
    for c in p["hash"]:
        df = df.withColumn(c, F.sha2(F.concat_ws("|", F.lit(SALT), F.col(c).cast("string")), 256))
    return df.drop(*p["drop"])

def audit(role, cloud, action, columns):
    new = not os.path.exists(AUDIT_FILE)
    with open(AUDIT_FILE, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["timestamp", "role", "cloud", "action", "columns"])
        w.writerow([datetime.datetime.now().isoformat(timespec="seconds"),
                    role, cloud, action, ";".join(columns)])

if __name__ == "__main__":
    spark = (SparkSession.builder.master("local[4]").appName("governance")
             .config("spark.driver.memory", "6g")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    clouds = ingest_all(spark)
    for role in ["admin", "analyst"]:
        for name, df in clouds.items():
            view = apply_policy(df, role)
            audit(role, name, "READ", view.columns)
            print("ROLE:", role, "| CLOUD:", name)
            view.select("PULocationID", "fare_amount", "source_cloud").show(3, truncate=20)
    spark.stop()
