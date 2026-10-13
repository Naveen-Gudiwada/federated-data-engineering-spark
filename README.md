# Federated Data Engineering with PySpark and Scala (Simulated Multi-Cloud)

A working prototype of a **federated data engineering pipeline**: instead of copying all data into one place, each "cloud" processes its own data in place and only small partial results are combined, under role-based governance with an audit trail. Built with **Python (PySpark)**, **Scala** and **Apache Spark 3.5.1** on NYC taxi trip data (9.55 million rows).

> **Note on scope:** the three "clouds" are three local folders, each storing the data in a different format (Parquet, CSV, JSON). This simulates multi-cloud heterogeneity on one laptop; no real AWS, Azure or GCP services were used.

## Highlights

- Ingests **three heterogeneous sources** (Parquet, CSV, JSON) into one enforced schema, with a lineage column (`source_cloud`).
- **Federated processing returned identical results to centralized processing** while taking about **37-40% less time**.
- **Governance layer**: role-based column policies (SHA-256 salted hashing of location IDs, hiding tip amounts for the `analyst` role) with a CSV **audit log** of every access.
- The same federated job implemented in **both PySpark and Scala**, producing identical output.
- A **Spark MLlib** fare-prediction step (Linear Regression vs Gradient-Boosted Trees).
- Scalability benchmark on 1, 2 and 4 cores, with 600 DPI figures.

## Architecture
Cloud A (Parquet) Cloud B (CSV) Cloud C (JSON)
| | |
+------- Ingestion + schema normalization (+ source_cloud lineage) -------+
|
Governance (role policy + audit log)
|
Federated job: clean + aggregate IN each cloud, combine small results
|
Analytics (hourly trip stats) and ML (fare prediction)

## Dataset

NYC TLC Yellow Taxi trip records, January to March 2024 (Parquet), split deterministically into three clouds using `PULocationID % 3`:

| Cloud | Format | Rows | Size on disk |
|---|---|---|---|
| A | Parquet | 3,745,627 | about 77 MB |
| B | CSV | 2,187,047 | about 228 MB |
| C | JSON | 3,622,104 | about 1.4 GB |
| **Total** | | **9,554,778** | |

The same kind of data takes about 18x more space as JSON than as Parquet.

## Results

### Federated vs centralized processing time (average of 2 timed runs, after a warm-up run)

| Cores | Federated (s) | Centralized (s) | Federated time saved |
|---|---|---|---|
| 1 | 35.16 | 56.16 | 37% |
| 2 | 21.34 | 34.47 | 38% |
| 4 | 18.02 | 30.09 | 40% |

Both methods produced identical per-hour trip counts (`Same results: True`).

![Performance benchmark](figures/Fig%2021%20Performance%20Benchmark.png)
![Scalability](figures/Fig%2022%20Scalability%20Evaluation.png)

### Scalability
Federated speedup relative to 1 core: **1.65x on 2 cores and 1.95x on 4 cores**. Scaling is real but sub-linear: on one laptop with about 9.5M rows, the job is limited by disk reads (especially the large JSON cloud) and fixed Spark overhead.

### Python vs Scala (federated job, 4 cores)

| Language | Run 1 (s) | Run 2 (s) | Average (s) |
|---|---|---|---|
| PySpark | 20.89 | 15.15 | 18.02 |
| Scala | 17.79 | 15.31 | 16.55 |

The ranges overlap, so the honest conclusion is that performance is comparable. DataFrame code in PySpark runs on the same JVM engine as Scala.

### ML: fare prediction (10% sample; 678,826 train / 169,967 test rows)

| Model | RMSE (USD) | R2 | Training time (s) |
|---|---|---|---|
| Linear Regression | 5.556 | 0.8948 | 18.97 |
| Gradient-Boosted Trees | 5.917 | 0.8807 | 55.17 |

The simpler linear model was both more accurate and about 3x faster to train with the settings used (GBT: 20 iterations, depth 5, no tuning).

## Governance

| Role | Location IDs | Tip amount |
|---|---|---|
| `admin` | visible | visible |
| `analyst` | SHA-256 hashed with a salt | hidden |

Every read is appended to `audit_log.csv` (timestamp, role, cloud, action, columns visible). Hashing is deterministic, so analysts can still group by location without seeing the real ID.

## Repository structure

| File | Purpose |
|---|---|
| `split_clouds.py` | Splits raw data into Cloud A/B/C in three formats |
| `ingest.py` | Reads each cloud and normalizes to one schema |
| `governance.py` | Role policies, hashing and audit log |
| `job.py` | Federated vs centralized job |
| `benchmark.py` | Timed benchmark (`python benchmark.py <cores>`) |
| `federated_job.scala` | The federated job in Scala |
| `ml_model.py` | MLlib models |
| `make_charts.py` | Generates the 600 DPI figures |
| `run_demo.ps1` | Runs the main stages end to end |
| `*_results.csv`, `audit_log.csv` | Measured outputs |
| `figures/`, `Screenshots/` | Charts and execution evidence |

## How to reproduce (Windows)

Requirements: Python 3.11, Java 17 (Temurin), and `winutils.exe` + `hadoop.dll` (Hadoop 3.3.x) in `C:\hadoop\bin`.

```powershell
py -3.11 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17..."   # your Java 17 path
$env:HADOOP_HOME = "C:\hadoop"
# Download yellow_tripdata_2024-01..03.parquet from the NYC TLC site into data\raw
python split_clouds.py
python ingest.py
python governance.py
python job.py
python benchmark.py 4
python make_charts.py
```

The Scala job runs with PySpark's bundled shell:
`.\venv\Lib\site-packages\pyspark\bin\spark-shell.cmd --master "local[4]" -i federated_job.scala`

Windows notes: set `SPARK_LOCAL_HOSTNAME=localhost` (already set inside the scripts) if your computer name contains an underscore, and pin `numpy<2`, `pandas<2.3`, `pyarrow<17` for PySpark 3.5.1 (already in `requirements.txt`).

## Limitations

- The multi-cloud setup is **simulated** on one machine. The "centralized" baseline copies data to local disk, so it does not include real network transfer or cloud egress costs; a real deployment would likely widen the gap, but that was not measured.
- Timings come from **2 runs per setting** on one 16 GB laptop, so small differences (such as Python vs Scala) are not statistically conclusive.
- Salted hashing of location IDs is **pseudonymization**, not anonymization, and the salt is hard-coded for demonstration. A real system would keep secrets in a key vault.
- Orchestration (for example Airflow), real cloud storage and a true multi-node Spark cluster are left as future work.

## Data availability

The raw data used are the public NYC TLC Yellow Taxi trip records (January to March 2024), available from the NYC Taxi and Limousine Commission "Trip Record Data" page. The three-cloud split is regenerated from them by `split_clouds.py`. All measured results are included in this repository.

## Tech stack

Python 3.11, PySpark 3.5.1, Scala 2.12, Apache Spark MLlib, Java 17, pandas, matplotlib, PowerShell.
