## 8. Data Warehouse Serving Layer (Star Schema)

### Overview
In this final phase, we transition our unified, engineered dataset into a **Star Schema** architecture optimized for Machine Learning and BI analytics. We split the data into a dimension table (`Dim_Customer`) for categorical/demographic data and a central fact table (`Fact_Loan`) containing pure numerical features and the target variable.

---

### Step 8.1: Dimension Table (`Dim_Customer`)
We extract all descriptive and categorical applicant profiles to maintain a clean numerical Fact table. This table provides the necessary metadata for descriptive analytics and BI dashboards without cluttering the ML feeding layer.

**Columns Included:**
* `SK_ID_CURR` (Primary Key)
* `NAME_CONTRACT_TYPE`
* `OCCUPATION_TYPE`
* `NAME_EDUCATION_TYPE`
* `NAME_FAMILY_STATUS`
* `NAME_HOUSING_TYPE`
* `NAME_INCOME_TYPE`
* `FLAG_OWN_REALTY`

### Step 8.2: Fact Table (`Fact_Loan`)
We drop the categorical columns from our master joined table. The resulting `Fact_Loan` table exclusively contains the `TARGET` label and all calculated numerical/continuous features, making it mathematically ready for ingestion by algorithms like XGBoost or LightGBM.

### Step 8.3: Hive & HDFS Export
We persist the finalized tables into the Hadoop Distributed File System (HDFS) using the **Parquet** format with **Snappy** compression for optimal read performance and minimal storage footprint. Both tables are subsequently registered in the Hive Metastore for immediate Spark SQL querying.

```python
from pyspark.sql.functions import monotonically_increasing_id

# 1. Drop existing tables from Hive Metastore to clear old schema metadata
spark.sql("DROP TABLE IF EXISTS home_credit_dw.fact_loan")
spark.sql("DROP TABLE IF EXISTS home_credit_dw.dim_customer")

# 2. Create Dimension Customer (Descriptive Attributes)
dim_customer = clean_app_fixed.select(
    "SK_ID_CURR",
    "CODE_GENDER",
    "FLAG_OWN_CAR",
    "NAME_CONTRACT_TYPE",
    "OCCUPATION_TYPE",
    "NAME_EDUCATION_TYPE",
    "NAME_FAMILY_STATUS",
    "NAME_HOUSING_TYPE",
    "NAME_INCOME_TYPE",
    "FLAG_OWN_REALTY"
)

# 3. Prepare Fact Loan: Keep NAME_CONTRACT_TYPE for partitioning, drop the rest
fact_loan_clean = fact_loan.drop(
    "CODE_GENDER",
    "FLAG_OWN_CAR",
    "OCCUPATION_TYPE",
    "NAME_EDUCATION_TYPE",
    "NAME_FAMILY_STATUS",
    "NAME_HOUSING_TYPE",
    "NAME_INCOME_TYPE",
    "FLAG_OWN_REALTY"
)

# 4. Add Surrogate Key
fact_loan_final = fact_loan_clean.withColumn("loan_key", monotonically_increasing_id())

# 5. Ensure Hive Database exists
spark.sql("CREATE DATABASE IF NOT EXISTS home_credit_dw")

# 6. Save Dimension Table
(dim_customer.write 
    .mode("overwrite") 
    .format("parquet") 
    .option("compression", "snappy") 
    .option("path", "/user/student/home_credit/warehouse/dim_customer") 
    .saveAsTable("home_credit_dw.dim_customer"))

# 7. Save Fact Table with Partitioning
(fact_loan_final.write 
    .mode("overwrite") 
    .format("parquet") 
    .option("compression", "snappy") 
    .partitionBy("NAME_CONTRACT_TYPE") 
    .option("path", "/user/student/home_credit/warehouse/fact_loan") 
    .saveAsTable("home_credit_dw.fact_loan"))

print("Data Warehouse tables created successfully!")
```
