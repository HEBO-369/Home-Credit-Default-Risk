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
# ==========================================
# 1. Create Dimension Table (Dim_Customer)
# ==========================================
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
# ==========================================
# 2. Finalize Fact Table (Fact_Loan)
# ==========================================
fact_loan_final = fact_loan.drop(
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

# ==========================================
# 3. Export to Hive & HDFS
# ==========================================
# Ensure database exists
spark.sql("CREATE DATABASE IF NOT EXISTS home_credit_dw")

# Write Dim_Customer
(dim_customer.write 
    .mode("overwrite") 
    .format("parquet") 
    .option("compression", "snappy") 
    .option("path", "/user/student/home_credit/warehouse/dim_customer") 
    .saveAsTable("home_credit_dw.dim_customer"))

# Write Fact_Loan
(fact_loan_final.write 
    .mode("overwrite") 
    .format("parquet") 
    .option("compression", "snappy") 
    .option("path", "/user/student/home_credit/warehouse/fact_loan") 
    .saveAsTable("home_credit_dw.fact_loan"))

print("Data Warehouse tables (Dim_Customer & Fact_Loan) created and saved to Hive successfully!")