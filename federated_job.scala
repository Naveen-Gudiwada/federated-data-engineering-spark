import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val cols = Seq("VendorID" -> "int", "tpep_pickup_datetime" -> "timestamp",
  "tpep_dropoff_datetime" -> "timestamp", "passenger_count" -> "int",
  "trip_distance" -> "double", "PULocationID" -> "int", "DOLocationID" -> "int",
  "fare_amount" -> "double", "tip_amount" -> "double", "total_amount" -> "double")

def normalize(df: DataFrame): DataFrame =
  df.select(cols.map { case (c, t) => col(c).cast(t).alias(c) }: _*)

def clean(df: DataFrame): DataFrame =
  df.filter(col("trip_distance") > 0 && col("trip_distance") < 100 &&
    col("fare_amount") > 0 && col("fare_amount") < 500 && col("passenger_count") > 0)

def part(df: DataFrame): DataFrame =
  clean(normalize(df)).groupBy(hour(col("tpep_pickup_datetime")).alias("hour"))
    .agg(count("*").alias("trips"), sum("fare_amount").alias("fare_sum"))

def run() = {
  val a = spark.read.parquet("cloud_A/trips")
  val b = spark.read.option("header", "true").csv("cloud_B/trips")
  val c = spark.read.json("cloud_C/trips")
  part(a).unionByName(part(b)).unionByName(part(c))
    .groupBy("hour").agg(sum("trips").alias("trips"), sum("fare_sum").alias("fare_sum"))
    .withColumn("avg_fare", round(col("fare_sum") / col("trips"), 4))
    .orderBy("hour").collect()
}

run()  // warm-up run, discarded
val times = (1 to 2).map { _ =>
  val t = System.nanoTime(); val r = run(); (System.nanoTime() - t) / 1e9
}
println("SCALA federated runs (s): " + times.map(x => f"$x%.2f").mkString(", "))
val first = run().head
println("SCALA first row: hour=" + first.getInt(0) + " trips=" + first.getLong(1))
