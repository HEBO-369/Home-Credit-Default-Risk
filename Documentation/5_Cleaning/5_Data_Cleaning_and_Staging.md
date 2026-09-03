## 6. Data Cleaning & Staging Layer

### Overview
In this phase, we apply rigorous data quality checks and transformations to our Spark DataFrames. We handle missing string values, remove duplicates, filter out missing Primary Keys, and correct business logic errors (such as anomalous days and invalid financial values). Finally, the cleaned DataFrames are exported to a high-performance Staging Layer in Parquet format.

---

### Step 6.1: Dynamic Empty String Cleaning
SQL exports often represent missing string values as empty strings (`""`). The following function dynamically loops through all string columns and converts these empty strings to proper `NULL` values to ensure accurate Machine Learning processing.

```python
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

def clean_empty_strings(df):
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    
    for c in string_cols:
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)))
        
    return df

df_app = clean_empty_strings(df_app)
df_prev = clean_empty_strings(df_prev)
df_inst = clean_empty_strings(df_inst)
df_bureau = clean_empty_strings(df_bureau)
```

---

### Step 6.2: Deduplication & Null Filtering

We remove duplicate records based on the Primary Keys and filter out any rows where the Primary Key is missing to maintain referential integrity across our data model.

```python
clean_app = df_app.dropDuplicates(["SK_ID_CURR"]).filter(F.col("SK_ID_CURR").isNotNull())
clean_prev = df_prev.dropDuplicates(["SK_ID_PREV"]).filter(F.col("SK_ID_CURR").isNotNull())
clean_inst = df_inst.dropDuplicates().filter(F.col("SK_ID_CURR").isNotNull())
clean_bureau = df_bureau.dropDuplicates().filter(F.col("SK_ID_CURR").isNotNull())
```

---

### Step 6.3: Business Logic & Anomaly Correction

We address specific logical errors inherent in the dataset, such as negative days, the anomalous employment flag (365243), and invalid zero-value credits.
```python
# Fix anomalies in Application Train
clean_app_fixed = clean_app \
    .withColumn("DAYS_EMPLOYED", F.when(F.col("DAYS_EMPLOYED") == 365243, None).otherwise(F.col("DAYS_EMPLOYED"))) \
    .withColumn("OCCUPATION_TYPE", F.when(F.col("NAME_INCOME_TYPE") == "Pensioner", "Retired").otherwise(F.col("OCCUPATION_TYPE"))) \
    .withColumn("DAYS_BIRTH", F.abs(F.col("DAYS_BIRTH"))) \
    .withColumn("DAYS_EMPLOYED", F.abs(F.col("DAYS_EMPLOYED")))

# Fix negative days in Installments Payments
clean_inst_fixed = clean_inst \
    .withColumn("DAYS_INSTALMENT", F.abs(F.col("DAYS_INSTALMENT"))) \
    .withColumn("DAYS_ENTRY_PAYMENT", F.abs(F.col("DAYS_ENTRY_PAYMENT")))

# Fix invalid zeros and nulls in Bureau Data
clean_bureau_fixed = clean_bureau \
    .withColumn("AMT_CREDIT_SUM", F.when(F.col("AMT_CREDIT_SUM") <= 0.0, None).otherwise(F.col("AMT_CREDIT_SUM"))) \
    .fillna(0.0, subset=["AMT_CREDIT_SUM_DEBT", "AMT_CREDIT_SUM_OVERDUE", "AMT_CREDIT_MAX_OVERDUE"])
```

---

### Step 6.4: Staging Layer Export (Parquet Format)
The fully cleaned DataFrames are written back to HDFS in Parquet format under the staging zone using overwrite mode. Parquet provides columnar compression and fast read performance, acting as the clean handoff point for the Feature Engineering team.

```python
clean_app_fixed.write.mode("overwrite").parquet("/user/student/home_credit/staging/application_train") 
clean_prev.write.mode("overwrite").parquet("/user/student/home_credit/staging/previous_application")
clean_inst_fixed.write.mode("overwrite").parquet("/user/student/home_credit/staging/installments_payments") 
clean_bureau_fixed.write.mode("overwrite").parquet("/user/student/home_credit/staging/bureau")
```

> **Note for teammates:** Subsequent feature engineering pipelines can directly load from `/user/student/home_credit/staging/` without processing raw ingestion files.
