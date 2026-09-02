# Loan Default Prediction project steps

## 1. Data Exploration and Selection

The pipeline design began by analyzing the `HomeCredit_columns_description.csv` provided in the Kaggle competition to map the relational structure.

Out of the available datasets, we restricted the scope to exactly four core files:

- `application_train.csv`
- `previous_application.csv`
- `installments_payments.csv`
- `bureau.csv`

Start download them, then added to the Shared directory, to be added the VM

## 2. Pre-Ingestion Data Filtering

Before loading data into the Hadoop ecosystem, created a local Python script to filter the raw CSV files down to only the explicitly required columns.

**Justification:** Spinning up a distributed Spark context to trim columns from local, flat CSV files introduces unnecessary JVM overhead. Filtering the data locally with Pandas before ingestion reduces the file size, saves disk space in the MySQL staging database, reduces network I/O during the Sqoop transfer, and minimizes the final HDFS storage footprint.

Create a file named `filter_data.py` in the shared directory with the following code:

```python
import pandas as pd

# Map each file to the exact columns defined in the Star Schema
files_to_filter = {
    "application_train.csv": [
        "SK_ID_CURR", "TARGET", "NAME_CONTRACT_TYPE", "DAYS_BIRTH", 
        "OCCUPATION_TYPE", "NAME_EDUCATION_TYPE", "NAME_FAMILY_STATUS", 
        "NAME_HOUSING_TYPE", "NAME_INCOME_TYPE", "FLAG_OWN_REALTY", 
        "AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY", "AMT_GOODS_PRICE", "DAYS_EMPLOYED"
    ],
    "previous_application.csv": [
        "SK_ID_CURR", "SK_ID_PREV", "NAME_CONTRACT_STATUS", "AMT_CREDIT"
    ],
    "installments_payments.csv": [
        "SK_ID_CURR", "SK_ID_PREV", "DAYS_ENTRY_PAYMENT", "DAYS_INSTALMENT", "AMT_INSTALMENT", "AMT_PAYMENT"
    ],
    "bureau.csv": [
        "SK_ID_CURR", "SK_ID_PREV", "CREDIT_ACTIVE", "AMT_CREDIT_SUM_DEBT", "AMT_CREDIT_SUM_OVERDUE", 
        "AMT_CREDIT_MAX_OVERDUE", "AMT_CREDIT_SUM"
    ]
}

for filename, columns in files_to_filter.items():
    print(f"Reading {filename}...")
    # usecols reads only the specified columns, saving memory
    df = pd.read_csv(filename, usecols=columns)
    
    new_filename = f"filtered_{filename}"
    df.to_csv(new_filename, index=False)
    print(f"Successfully saved {new_filename} with {len(df.columns)} columns.\n")
```

### Execution Command

Open a terminal, navigate to the directory containing your raw datasets, and run the script (change the directory name if different):

```bash
cd /media/sf_shared-files
python3 filter_data.py
```

## 3. Relational Database Staging (MySQL)

The filtered CSV files are loaded into MySQL to simulate a standard enterprise OLTP system.

Open a terminal and log into the MySQL shell:

```bash
mysql -u student -p
```

Execute the following SQL commands to create the schema and load the CSV data(change the directory name if different):

```sql
CREATE DATABASE home_credit;
USE home_credit;

-- 1. application_train
CREATE TABLE application_train (
    SK_ID_CURR INT,
    TARGET INT,
    NAME_CONTRACT_TYPE VARCHAR(255),
    DAYS_BIRTH INT,
    OCCUPATION_TYPE VARCHAR(255),
    NAME_EDUCATION_TYPE VARCHAR(255),
    NAME_FAMILY_STATUS VARCHAR(255),
    NAME_HOUSING_TYPE VARCHAR(255),
    NAME_INCOME_TYPE VARCHAR(255),
    FLAG_OWN_REALTY VARCHAR(255),
    AMT_INCOME_TOTAL DOUBLE,
    AMT_CREDIT DOUBLE,
    AMT_ANNUITY DOUBLE,
    AMT_GOODS_PRICE DOUBLE,
    DAYS_EMPLOYED INT
);
-- 2. Create previous_application table
CREATE TABLE previous_application (
    SK_ID_PREV INT PRIMARY KEY,
    SK_ID_CURR INT,
    NAME_CONTRACT_STATUS VARCHAR(50),
    AMT_CREDIT DOUBLE
);

-- 3. Create installments_payments table
CREATE TABLE installments_payments (
    SK_ID_PREV INT,
    SK_ID_CURR INT,
    DAYS_INSTALMENT DOUBLE,
    DAYS_ENTRY_PAYMENT DOUBLE,
    AMT_INSTALMENT DOUBLE,
    AMT_PAYMENT DOUBLE
);

-- 4. Create bureau table
CREATE TABLE bureau (
    SK_ID_CURR INT,
    CREDIT_ACTIVE VARCHAR(50),
    AMT_CREDIT_SUM_DEBT DOUBLE,
    AMT_CREDIT_SUM_OVERDUE DOUBLE,
    AMT_CREDIT_MAX_OVERDUE DOUBLE,
    AMT_CREDIT_SUM DOUBLE
);

LOAD DATA INFILE '/media/sf_shared-files/filtered_application_train.csv'
INTO TABLE application_train
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 2. previous_application
CREATE TABLE previous_application (
    SK_ID_PREV INT,
    SK_ID_CURR INT,
    NAME_CONTRACT_STATUS VARCHAR(255),
    AMT_CREDIT DOUBLE
);

LOAD DATA INFILE '/media/sf_shared-files/filtered_previous_application.csv'
INTO TABLE previous_application
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 3. installments_payments
CREATE TABLE installments_payments (
    SK_ID_PREV INT,
    SK_ID_CURR INT,
    DAYS_INSTALMENT DOUBLE,
    DAYS_ENTRY_PAYMENT DOUBLE,
    AMT_INSTALMENT DOUBLE,
    AMT_PAYMENT DOUBLE
);

LOAD DATA INFILE '/media/sf_shared-files/filtered_installments_payments.csv'
INTO TABLE installments_payments
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 4. bureau
CREATE TABLE bureau (
    SK_ID_CURR INT,
    CREDIT_ACTIVE VARCHAR(255),
    AMT_CREDIT_SUM_DEBT DOUBLE,
    AMT_CREDIT_SUM_OVERDUE DOUBLE,
    AMT_CREDIT_MAX_OVERDUE DOUBLE,
    AMT_CREDIT_SUM DOUBLE
);

LOAD DATA INFILE '/media/sf_shared-files/filtered_bureau.csv'
INTO TABLE bureau
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
```

## 4. Big Data Ingestion (HDFS & Apache Sqoop)

Open a new terminal to create the raw data landing zones in the Hadoop Distributed File System (HDFS):

```bash
hdfs dfs -mkdir -p /user/student/home_credit/raw/application_train
hdfs dfs -mkdir -p /user/student/home_credit/raw/previous_application
hdfs dfs -mkdir -p /user/student/home_credit/raw/installments_payments
hdfs dfs -mkdir -p /user/student/home_credit/raw/bureau
```

Use Sqoop to trigger MapReduce jobs that import the MySQL tables into HDFS. Execute these four commands in your terminal:

```bash
sqoop import --connect jdbc:mysql://localhost/home_credit --username student --password student --table application_train --target-dir /user/student/home_credit/raw/application_train -m 1

sqoop import --connect jdbc:mysql://localhost/home_credit --username student --password student --table previous_application --target-dir /user/student/home_credit/raw/previous_application -m 1

sqoop import --connect jdbc:mysql://localhost/home_credit --username student --password student --table installments_payments --target-dir /user/student/home_credit/raw/installments_payments -m 1

sqoop import --connect jdbc:mysql://localhost/home_credit --username student --password student --table bureau --target-dir /user/student/home_credit/raw/bureau -m 1
```

Verify the files successfully landed in the cluster:

```bash
hdfs dfs -ls /user/student/home_credit/raw/application_train
```

## 5. Distributed Processing & EDA (PySpark)

Launch Jupyter Notebook from your terminal:

```bash
jupyter notebook
```

jupyter notebook will open in Firefox, create a new Python 3 notebook, and execute the following cells in order.

### 5.1 Initialization and Loading

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

%matplotlib inline
sns.set_theme(style="whitegrid")
spark = SparkSession.builder.appName("HomeCredit_Jupyter_EDA").getOrCreate()

schema_app = StructType([
    StructField("SK_ID_CURR", IntegerType(), True), StructField("TARGET", IntegerType(), True),
    StructField("NAME_CONTRACT_TYPE", StringType(), True), StructField("DAYS_BIRTH", IntegerType(), True),
    StructField("OCCUPATION_TYPE", StringType(), True), StructField("NAME_EDUCATION_TYPE", StringType(), True),
    StructField("NAME_FAMILY_STATUS", StringType(), True), StructField("NAME_HOUSING_TYPE", StringType(), True),
    StructField("NAME_INCOME_TYPE", StringType(), True), StructField("FLAG_OWN_REALTY", StringType(), True),
    StructField("AMT_INCOME_TOTAL", DoubleType(), True), StructField("AMT_CREDIT", DoubleType(), True),
    StructField("AMT_ANNUITY", DoubleType(), True), StructField("AMT_GOODS_PRICE", DoubleType(), True),
    StructField("DAYS_EMPLOYED", IntegerType(), True)
])

schema_prev = StructType([
    StructField("SK_ID_PREV", IntegerType(), True), StructField("SK_ID_CURR", IntegerType(), True),
    StructField("NAME_CONTRACT_STATUS", StringType(), True), StructField("AMT_CREDIT", DoubleType(), True)
])

schema_inst = StructType([
    StructField("SK_ID_PREV", IntegerType(), True), StructField("SK_ID_CURR", IntegerType(), True),
    StructField("DAYS_INSTALMENT", DoubleType(), True), StructField("DAYS_ENTRY_PAYMENT", DoubleType(), True),
    StructField("AMT_INSTALMENT", DoubleType(), True), StructField("AMT_PAYMENT", DoubleType(), True)
])

schema_bureau = StructType([
    StructField("SK_ID_CURR", IntegerType(), True), StructField("CREDIT_ACTIVE", StringType(), True),
    StructField("AMT_CREDIT_SUM_DEBT", DoubleType(), True), StructField("AMT_CREDIT_SUM_OVERDUE", DoubleType(), True),
    StructField("AMT_CREDIT_MAX_OVERDUE", DoubleType(), True), StructField("AMT_CREDIT_SUM", DoubleType(), True)
])

df_app = spark.read.schema(schema_app).csv("/user/student/home_credit/raw/application_train/*")
df_prev = spark.read.schema(schema_prev).csv("/user/student/home_credit/raw/previous_application/*")
df_inst = spark.read.schema(schema_inst).csv("/user/student/home_credit/raw/installments_payments/*")
df_bureau = spark.read.schema(schema_bureau).csv("/user/student/home_credit/raw/bureau/*")
```

### 5.2 Data Cleaning

Remove duplicate records and drop rows missing the primary key (`SK_ID_CURR`).

```python
clean_app = df_app.dropDuplicates(["SK_ID_CURR"]).filter(F.col("SK_ID_CURR").isNotNull())
clean_prev = df_prev.dropDuplicates(["SK_ID_PREV"]).filter(F.col("SK_ID_CURR").isNotNull())
clean_inst = df_inst.dropDuplicates().filter(F.col("SK_ID_CURR").isNotNull())
clean_bureau = df_bureau.dropDuplicates().filter(F.col("SK_ID_CURR").isNotNull())
```

### 5.3 Visual EDA & Project Insights

```python
target_counts = clean_app.groupBy("TARGET").count().toPandas()
target_counts["TARGET_LABEL"] = target_counts["TARGET"].map({0: "Non-Default (0)", 1: "Default (1)"})

plt.figure(figsize=(7, 4))
sns.barplot(x="TARGET_LABEL", y="count", data=target_counts, palette="viridis")
plt.title("Home Credit Default Risk - Target Distribution")
plt.xlabel("Status")
plt.ylabel("Number of Applicants")
plt.show()
```

**Key Insight:** The visualization reveals a severe class imbalance. Approximately 92% of applicants belong to Class 0 (Non-Default), while only ~8% belong to Class 1 (Default). Feature engineering and machine learning models must utilize techniques such as SMOTE, algorithmic class weighting, or ROC-AUC evaluation, as standard accuracy metrics will yield false confidence in predicting actual credit risk.

### 5.4 Data Staging (Parquet Export)

The cleaned DataFrames are written back to HDFS in Parquet format for highly optimized read speeds during downstream Feature Engineering.

```python
clean_app.write.mode("overwrite").parquet("/user/student/home_credit/staging/application_train")
clean_prev.write.mode("overwrite").parquet("/user/student/home_credit/staging/previous_application")
clean_inst.write.mode("overwrite").parquet("/user/student/home_credit/staging/installments_payments")
clean_bureau.write.mode("overwrite").parquet("/user/student/home_credit/staging/bureau")
```
