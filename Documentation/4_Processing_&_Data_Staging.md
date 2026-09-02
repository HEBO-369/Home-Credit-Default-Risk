## 5. Distributed Processing & Data Staging (PySpark)

### Overview
After successfully ingesting the raw relational tables from MySQL to HDFS via Apache Sqoop, we establish proper multi-user permission separation and leverage PySpark on the Hadoop/YARN cluster to initialize schemas, clean the raw records, and export them into a high-performance **Staging Layer (Parquet format)**. This provides a clean, optimized foundation for the team to perform downstream Feature Engineering and Star Schema modeling without repeatedly reading raw CSV files.

---

### Step 5.1: Cluster Permissions & Workspace Preparation (Terminal)
Before launching Jupyter, we ensure that system service accounts and client permissions are properly configured to prevent any permission or directory access errors during PySpark execution.

```bash
# 1. Fix ownership and permissions for Hadoop daemon logs
sudo chown -R hadoop:hadoop /home/hadoop/hadoop/logs
sudo chmod 775 /home/hadoop/hadoop/logs

# 2. Switch to hadoop service user to ensure cluster daemons and staging directories are properly provisioned
su - hadoop
start-dfs.sh
start-yarn.sh

# Create the staging zone on HDFS and grant full access to the student user
hdfs dfs -mkdir -p /user/student/home_credit/staging
hdfs dfs -chown -R student:student /user/student/home_credit/staging

# Exit back to the standard student user
exit
```

---

### Step 5.2: Spark Session Initialization & Schema Definition
From the Jupyter Notebook, we initialize a SparkSession and define strict schema definitions for each raw table to ensure type safety and prevent schema-inference overhead.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("HomeCredit_Data_Staging") \
    .getOrCreate()

# 1. Main Application Schema
schema_app = StructType([
    StructField("SK_ID_CURR", IntegerType(), True), 
    StructField("TARGET", IntegerType(), True),
    StructField("NAME_CONTRACT_TYPE", StringType(), True), 
    StructField("DAYS_BIRTH", IntegerType(), True),
    StructField("OCCUPATION_TYPE", StringType(), True), 
    StructField("NAME_EDUCATION_TYPE", StringType(), True),
    StructField("NAME_FAMILY_STATUS", StringType(), True), 
    StructField("NAME_HOUSING_TYPE", StringType(), True),
    StructField("NAME_INCOME_TYPE", StringType(), True), 
    StructField("FLAG_OWN_REALTY", StringType(), True),
    StructField("AMT_INCOME_TOTAL", DoubleType(), True), 
    StructField("AMT_CREDIT", DoubleType(), True),
    StructField("AMT_ANNUITY", DoubleType(), True), 
    StructField("AMT_GOODS_PRICE", DoubleType(), True),
    StructField("DAYS_EMPLOYED", IntegerType(), True)
])

# 2. Previous Applications Schema
schema_prev = StructType([
    StructField("SK_ID_PREV", IntegerType(), True), 
    StructField("SK_ID_CURR", IntegerType(), True),
    StructField("NAME_CONTRACT_STATUS", StringType(), True), 
    StructField("AMT_CREDIT", DoubleType(), True)
])

# 3. Installments Payments Schema
schema_inst = StructType([
    StructField("SK_ID_PREV", IntegerType(), True), 
    StructField("SK_ID_CURR", IntegerType(), True),
    StructField("DAYS_INSTALMENT", DoubleType(), True), 
    StructField("DAYS_ENTRY_PAYMENT", DoubleType(), True),
    StructField("AMT_INSTALMENT", DoubleType(), True), 
    StructField("AMT_PAYMENT", DoubleType(), True)
])

# 4. Bureau Data Schema
schema_bureau = StructType([
    StructField("SK_ID_CURR", IntegerType(), True), 
    StructField("CREDIT_ACTIVE", StringType(), True),
    StructField("AMT_CREDIT_SUM_DEBT", DoubleType(), True), 
    StructField("AMT_CREDIT_SUM_OVERDUE", DoubleType(), True),
    StructField("AMT_CREDIT_MAX_OVERDUE", DoubleType(), True), 
    StructField("AMT_CREDIT_SUM", DoubleType(), True)
])
```

---

### Step 5.3: Reading Raw Data & Data Cleaning
We load the raw CSV files from HDFS using the defined schemas, remove duplicate records based on primary keys, and filter out null primary keys to maintain data integrity.

```python
# Load raw data from HDFS
df_app = spark.read.schema(schema_app).csv("/user/student/home_credit/raw/application_train/*")
df_prev = spark.read.schema(schema_prev).csv("/user/student/home_credit/raw/previous_application/*")
df_inst = spark.read.schema(schema_inst).csv("/user/student/home_credit/raw/installments_payments/*")
df_bureau = spark.read.schema(schema_bureau).csv("/user/student/home_credit/raw/bureau/*")

# Data Cleaning (Drop duplicates & ensure primary keys are not null)
clean_app = df_app.dropDuplicates(["SK_ID_CURR"]).filter(F.col("SK_ID_CURR").isNotNull())
clean_prev = df_prev.dropDuplicates(["SK_ID_PREV"]).filter(F.col("SK_ID_CURR").isNotNull())
clean_inst = df_inst.dropDuplicates().filter(F.col("SK_ID_CURR").isNotNull())
clean_bureau = df_bureau.dropDuplicates().filter(F.col("SK_ID_CURR").isNotNull())
```

---

### Step 5.4: Staging Layer Export (Parquet Format)
The cleaned DataFrames are written back to HDFS in Parquet format under the staging zone. Parquet provides columnar compression and fast read performance, acting as the clean handoff point for the Feature Engineering team.

```python
clean_app.write.mode("overwrite").parquet("/user/student/home_credit/staging/application_train")
clean_prev.write.mode("overwrite").parquet("/user/student/home_credit/staging/previous_application")
clean_inst.write.mode("overwrite").parquet("/user/student/home_credit/staging/installments_payments")
clean_bureau.write.mode("overwrite").parquet("/user/student/home_credit/staging/bureau")
```

> **Note for teammates:** Subsequent feature engineering pipelines can directly load from `/user/student/home_credit/staging/` without processing raw CSVs.
