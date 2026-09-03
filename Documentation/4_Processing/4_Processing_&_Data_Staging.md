## 5. Distributed Processing & Data Selection (PySpark)

### Overview
After successfully ingesting the raw relational tables from MySQL to HDFS via Apache Sqoop, we establish proper multi-user permission separation and leverage PySpark on the Hadoop/YARN cluster to initialize schemas. We then load the raw records and explicitly select the required columns to prepare them for the cleaning phase.

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

### Step 5.2: Spark Session Initialization 
From the Jupyter Notebook, we initialize a SparkSession.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("HomeCredit_Serving_Layer") \
    .enableHiveSupport() \
    .getOrCreate()
```

---

### Step 5.3: Reading Raw Data & Column Selection
We load the raw Parquet files from HDFS (which natively retain the original MySQL schema). We then explicitly select and cast the required columns to ensure machine learning and BI readiness, remove duplicate records, and filter out null primary keys to maintain data integrity.
```python
# Load raw data from HDFS
raw_app = spark.read.parquet("/user/student/home_credit/raw/application_train")
raw_prev = spark.read.parquet("/user/student/home_credit/raw/previous_application")
raw_inst = spark.read.parquet("/user/student/home_credit/raw/installments_payments")
raw_bureau = spark.read.parquet("/user/student/home_credit/raw/bureau")

# Select required columns and enforce strict analytical data types

# 1. Application Train
df_app = raw_app.select(
    F.col("SK_ID_CURR").cast("int"),
    F.col("TARGET").cast("int"),
    F.col("NAME_CONTRACT_TYPE").cast("string"),
    F.col("DAYS_BIRTH").cast("int"),
    F.col("OCCUPATION_TYPE").cast("string"),
    F.col("NAME_EDUCATION_TYPE").cast("string"),
    F.col("NAME_FAMILY_STATUS").cast("string"),
    F.col("NAME_HOUSING_TYPE").cast("string"),
    F.col("NAME_INCOME_TYPE").cast("string"),
    F.col("FLAG_OWN_REALTY").cast("string"),
    F.col("AMT_INCOME_TOTAL").cast("double"),
    F.col("AMT_CREDIT").cast("double"),
    F.col("AMT_ANNUITY").cast("double"),
    F.col("AMT_GOODS_PRICE").cast("double"),
    F.col("DAYS_EMPLOYED").cast("int")
)

# 2. Previous Applications
df_prev = raw_prev.select(
    F.col("SK_ID_PREV").cast("int"),
    F.col("SK_ID_CURR").cast("int"),
    F.col("NAME_CONTRACT_STATUS").cast("string"),
    F.col("AMT_CREDIT").cast("double")
)

# 3. Installments Payments
df_inst = raw_inst.select(
    F.col("SK_ID_PREV").cast("int"),
    F.col("SK_ID_CURR").cast("int"),
    F.col("DAYS_INSTALMENT").cast("double"),
    F.col("DAYS_ENTRY_PAYMENT").cast("double"),
    F.col("AMT_INSTALMENT").cast("double"),
    F.col("AMT_PAYMENT").cast("double")
)

# 4. Bureau Data
df_bureau = raw_bureau.select(
    F.col("SK_ID_CURR").cast("int"),
    F.col("CREDIT_ACTIVE").cast("string"),
    F.col("AMT_CREDIT_SUM_DEBT").cast("double"),
    F.col("AMT_CREDIT_SUM_OVERDUE").cast("double"),
    F.col("AMT_CREDIT_MAX_OVERDUE").cast("double"),
    F.col("AMT_CREDIT_SUM").cast("double")
)


```
