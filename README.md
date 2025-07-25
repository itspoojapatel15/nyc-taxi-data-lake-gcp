# NYC Taxi Data Lake on GCP

![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark)
![GCP](https://img.shields.io/badge/GCP-Cloud-4285F4?logo=googlecloud)
![BigQuery](https://img.shields.io/badge/BigQuery-Analytics-4285F4?logo=googlebigquery)
![dbt](https://img.shields.io/badge/dbt-1.7+-orange?logo=dbt)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?logo=terraform)
![License](https://img.shields.io/badge/License-MIT-green)

Processes the NYC TLC Trip Record Dataset (50M+ rows/month) with Dataproc + PySpark, writes optimized Parquet to GCS with Hive-style partitioning, serves via BigQuery external tables, and transforms with dbt.

## Demo

![Project Demo](screenshots/project-demo.png)

PySpark job output processing 54M+ trips with BigQuery analytics showing trip duration and fare metrics by time of day.

## Architecture

```
+--------------------+
| NYC TLC Open Data  | (Yellow, Green, FHV trips)
+---------+----------+
          |
          v
+--------------------+     +---------------------------+
|   GCS Raw Zone     |---->| Dataproc + PySpark        |
| gs://taxi-raw/     |     | - Clean & enforce schema  |
+--------------------+     | - Derived columns         |
                           | - Hive partitioning       |
                           | - Parquet + Snappy        |
                           +----------+----------------+
                                      |
                                      v
                           +---------------------------+
                           | GCS Processed Zone        |
                           | /year=/month=/zone=/      |
                           +----------+----------------+
                                      |
                                      v
                           +---------------------------+
                           | BigQuery External Tables  |
                           | (partition pruning)       |
                           +----------+----------------+
                                      |
                                      v
                           +---------------------------+
                           | dbt -> Looker Studio      |
                           +---------------------------+
```

## Key Insights

1. NYC Transportation Patterns: Identify peak demand hours, top pickup/dropoff zones, and fare trends across boroughs.
2. Big Data at Scale: Processing billions of rows with Spark demonstrates credible production experience with performance optimization.

## Setup

```bash
cd terraform && terraform init && terraform apply
python scripts/download_tlc_data.py
bash scripts/submit_dataproc_job.sh
```

## Test Results

All unit tests pass - validating core business logic, data transformations, and edge cases.

![Test Results](screenshots/test-results.png)

14 tests passed across 5 test suites:
- TestTripDuration - normal/short/negative duration handling
- TestAvgSpeed - speed calculation, zero-duration edge case
- TestFarePerMile - fare normalization, zero-distance edge case
- TestTipPercentage - tip calculation, no-tip case
- TestTimeOfDay - morning rush/midday/evening rush/night/late night

## Maintainer

This project is maintained by Pooja Patel, a Data Science professional specializing in large-scale data processing, statistical analysis, and predictive modeling.

- Email: patel.pooja81599@gmail.com
- Skills: Python, R, SQL, Pandas, NumPy, ggplot2

## License

MIT