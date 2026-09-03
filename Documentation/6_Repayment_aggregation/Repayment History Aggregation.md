# 56. Repayment History Aggregation

## Overview

This stage processes the cleaned `installments_payments` dataset and transforms detailed repayment history into loan-level repayment features.

The main objective is to analyze the repayment behavior of each loan and generate features required for the final `Fact_Loan` feature set.

---

## Input

### DataFrame

```text
clean_inst
```

### Key Columns

| Column | Description |
|---|---|
| `SK_ID_PREV` | Loan / previous application identifier |
| `DAYS_INSTALMENT` | Scheduled installment payment day |
| `DAYS_ENTRY_PAYMENT` | Actual payment day |

---

# Processing Steps

## 1. Calculate Days Past Due (DPD)

For each installment, the number of days past due is calculated by comparing the actual payment day with the scheduled installment day.

Early or on-time payments are assigned a `0` value.

The resulting feature is:

```text
days_past_due
```

### PySpark Implementation

```python
from pyspark.sql import functions as F

repayment_data = clean_inst.withColumn(
    "days_past_due",
    F.when(
        F.col("DAYS_ENTRY_PAYMENT") > F.col("DAYS_INSTALMENT"),
        F.col("DAYS_ENTRY_PAYMENT") - F.col("DAYS_INSTALMENT")
    ).otherwise(0)
)
```

---

## 2. Aggregate Repayment History

The repayment records are grouped by `SK_ID_PREV` to maintain a loan-level grain.

Two repayment features are generated:

- `total_days_past_due`
- `num_late_payments`

### PySpark Implementation

```python
repayment_features = repayment_data.groupBy(
    "SK_ID_PREV"
).agg(
    F.sum("days_past_due").alias("total_days_past_due"),

    F.sum(
        F.when(
            F.col("days_past_due") > 0,
            1
        ).otherwise(0)
    ).alias("num_late_payments")
)
```

---

# Complete PySpark Code

```python
from pyspark.sql import functions as F

# Step 1: Calculate Days Past Due
repayment_data = clean_inst.withColumn(
    "days_past_due",
    F.when(
        F.col("DAYS_ENTRY_PAYMENT") > F.col("DAYS_INSTALMENT"),
        F.col("DAYS_ENTRY_PAYMENT") - F.col("DAYS_INSTALMENT")
    ).otherwise(0)
)

# Step 2: Aggregate repayment history at loan level
repayment_features = repayment_data.groupBy(
    "SK_ID_PREV"
).agg(
    F.sum("days_past_due").alias(
        "total_days_past_due"
    ),

    F.sum(
        F.when(
            F.col("days_past_due") > 0,
            1
        ).otherwise(0)
    ).alias(
        "num_late_payments"
    )
)

# Display results
repayment_features.show(10)

# Display schema
repayment_features.printSchema()
```

---

# Output

The stage produces the following DataFrame:

```text
repayment_features
```

| Column | Description |
|---|---|
| `SK_ID_PREV` | Loan identifier |
| `total_days_past_due` | Total accumulated days past due across all installments |
| `num_late_payments` | Number of late payments |

---

# Data Flow

```text
clean_inst
    │
    ▼
Calculate Days Past Due
    │
    ▼
days_past_due
    │
    ▼
Group By SK_ID_PREV
    │
    ├── total_days_past_due
    │
    └── num_late_payments
    │
    ▼
repayment_features
```

---

# Example Feature Output

```text
+----------+---------------------+------------------+
|SK_ID_PREV|total_days_past_due  |num_late_payments |
+----------+---------------------+------------------+
|1000001   |15                   |2                 |
|1000002   |0                    |0                 |
|1000003   |8                    |1                 |
+----------+---------------------+------------------+
```

---

# Technologies

- Apache Spark
- PySpark
- Spark DataFrame API
- Spark SQL Functions
- Hadoop HDFS
- Parquet

---

# Stage Output

The resulting DataFrame:

```text
repayment_features
```

is passed to the subsequent ETL stage, where repayment behavior features are integrated into the final:

```text
Fact_Loan
```

feature set.

---

## Project Pipeline Position

```text
Raw Data
    │
    ▼
Data Cleaning
    │
    ▼
clean_inst
    │
    ▼
Repayment History Aggregation
    │
    ▼
repayment_features
    │
    ▼
Feature Engineering
    │
    ▼
Fact_Loan
    │
    ▼
Machine Learning
```