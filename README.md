# Home Credit Default Risk — End-to-End Big Data Engineering Pipeline


**An end-to-end Big Data Engineering pipeline that takes the raw, relational **Home Credit Default Risk** dataset all the way from a MySQL operational database to a production-style Data Warehouse on Hadoop/Hive, and finally into a Machine Learning model that predicts loan default risk.**

Data Link : https://www.kaggle.com/c/home-credit-default-risk
---

## Why This Project?

The Home Credit dataset presents the same kind of challenges Data Engineers face in the real world: several large, normalized relational tables (millions of rows each) that need to be transformed into something a data warehouse and a machine learning model can actually consume. Rather than treating this as a single flat CSV-to-model exercise, this project was built as a full production-style pipeline — starting from raw operational data sitting in a MySQL database, moving it through a distributed Hadoop ecosystem (HDFS, Sqoop, PySpark, Hive), and only then handing off a clean, feature-rich, one-row-per-customer dataset to the modeling stage. The goal throughout was to mirror how a real Big Data / MLOps team would design this: strict schema enforcement, idempotent and repeatable ingestion jobs, grain-safe aggregations that avoid data explosion, an optimized columnar storage layer, and a final predictive model built on top of engineered, business-meaningful features — all so the resulting dataset can serve both Machine Learning (default-risk scoring) and Business Intelligence (dashboards and reporting) use cases from a single, trustworthy source of truth.

---

## Pipeline Architecture

```
MySQL (Docker)  →  Apache Sqoop  →  HDFS (raw zone)  →  PySpark  →  Staging Layer (Parquet)
                                                                          │
                                                                          ▼
                                                        Feature Engineering (grouped aggregations)
                                                                          │
                                                                          ▼
                                              Hive Star Schema — Dim_Customer & Fact_Loan (Parquet + Snappy)
                                                                          │
                                                                          ▼
                                                 Predictive Modeling (Python / scikit-learn / XGBoost)
```

The pipeline is organized into eight sequential phases, each with strict multi-user permission separation and data-integrity checks across the cluster:

1. **Data Modeling & Star Schema Design** — designing a Dimension table (`Dim_Customer`) and a Fact table (`Fact_Loan`) to avoid Cartesian joins and cluster memory overload.
2. **Database Setup & Bulk Ingestion** — loading the raw CSVs (`application_train`, `bureau`, `previous_application`, `installments_payments`) into a MySQL instance running in Docker, using strict DDL and high-throughput `LOAD DATA LOCAL INFILE`.
3. **Big Data Ingestion (HDFS & Apache Sqoop)** — extracting the MySQL tables into HDFS as Parquet via Sqoop MapReduce jobs, bridging the operational database host and the Hadoop cluster over the network.
4. **Distributed Processing & Data Selection (PySpark)** — initializing a Spark session with Hive support and projecting only the columns needed, with strict type casting.
5. **Data Cleaning & Staging Layer** — dynamic empty-string-to-NULL conversion, deduplication, referential-integrity filtering, and business-logic/anomaly correction, exported to a Parquet staging layer.
6. **Distributed Feature Engineering & Aggregation** — customer-grain-safe `groupBy` aggregations across the historical tables (previous applications, installments, bureau) plus cross-table composite features.
7. **Data Warehouse Serving Layer (Star Schema)** — splitting the unified dataset into `Dim_Customer` and `Fact_Loan`, writing to HDFS as partitioned, Snappy-compressed Parquet, and registering both tables in the Hive Metastore.
8. **Predictive Modeling & Machine Learning** — training and evaluating a default-risk classifier on the engineered warehouse tables.

---

## Tech Stack

| Layer | Technologies |
|---|---|
| **Operational Database** | MySQL (containerized with Docker) |
| **Big Data Ingestion** | Apache Sqoop (MapReduce-based extraction) |
| **Distributed Storage** | Hadoop HDFS |
| **Distributed Processing** | Apache Spark (PySpark) |
| **Data Warehouse** | Apache Hive (Metastore, Star Schema, HiveQL/DDL) |
| **Storage Format** | Apache Parquet with Snappy compression, Hive-style partitioning |
| **Machine Learning** | Python, pandas, NumPy, scikit-learn, XGBoost |
| **Visualization / Evaluation** | Matplotlib, Seaborn |
| **Environment** | Docker, Linux, YARN (cluster resource management) |

---

## Data Model — Star Schema

The engineered dataset is served as a Star Schema with strict `SK_ID_CURR`-grain enforcement (one row per customer):

- **`Dim_Customer`** — descriptive/categorical attributes (gender, education, family status, housing type, income type, etc.) used for BI slicing and dashboards.
- **`Fact_Loan`** — the numerical feature matrix used for Machine Learning, combining raw application fields with engineered features such as debt-to-income ratio, loan-to-value ratio, historical approval/refusal ratios, repayment-discipline metrics, and external bureau credit exposure.

Every historical table (previous applications, installment payments, bureau records) is aggregated to the customer grain **before** joining onto the main fact table, which prevents the Cartesian-product "data explosion" that a naive direct join would cause.

---

## Machine Learning

The final phase consumes the Hive-partitioned warehouse tables to train a default-risk classifier:

- Data is loaded directly from the partitioned Parquet warehouse (Hive-style `NAME_CONTRACT_TYPE=*` partitions), deduplicated, and merged into a single customer-level DataFrame.
- Categorical features are one-hot encoded; numerical features are standardized on the training split only, to avoid leakage.
- Because defaults make up only a small fraction of the data, the class imbalance is addressed explicitly (cost-sensitive weighting / resampling) rather than being ignored.
- The model is evaluated with metrics suited to imbalanced classification — ROC-AUC, confusion matrix, and a full classification report — rather than relying on plain accuracy.

---