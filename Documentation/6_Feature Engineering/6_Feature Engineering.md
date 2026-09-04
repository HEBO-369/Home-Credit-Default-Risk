## 7. Feature Engineering (Phase 1: Application & Previous Applications)

### Overview
In this phase, we engineer predictive features mandated by our Star Schema design. We compute direct application-level ratios on `application_train` and perform grouped aggregations on the `previous_application` historical records at the `SK_ID_CURR` grain to maintain a strictly one-to-one customer relationship and avoid Cartesian joins.

---

### Step 7.1: Application-Level Features (application_train)
We calculate four core financial and demographic ratios directly on the customer's primary loan application record.

* **DTI (Debt-to-Income):** Ratio of requested credit to total income (`AMT_CREDIT / AMT_INCOME_TOTAL`). Measures the applicant's overall debt burden.
* **Annuity_to_Income:** Proportion of total income allocated to servicing the regular loan payment (`AMT_ANNUITY / AMT_INCOME_TOTAL`). Assesses monthly installment affordability.
* **LTV (Loan-to-Value):** Ratio of the loan amount relative to the asset price (`AMT_CREDIT / AMT_GOODS_PRICE`). Evaluates collateral coverage.
* **Employed_to_Age:** Proportion of the applicant's life spent in their current employment (`DAYS_EMPLOYED / DAYS_BIRTH`). Indicates employment stability relative to age.

```python
from pyspark.sql import functions as F

# Compute primary application-level ratios with zero-division guards
fact_app_features = (
    clean_app_fixed
    .withColumn("DTI", F.when(F.col("AMT_INCOME_TOTAL") > 0, F.round(F.col("AMT_CREDIT") / F.col("AMT_INCOME_TOTAL"), 4)).otherwise(None))
    .withColumn("Annuity_to_Income", F.when(F.col("AMT_INCOME_TOTAL") > 0, F.round(F.col("AMT_ANNUITY") / F.col("AMT_INCOME_TOTAL"), 4)).otherwise(None))
    .withColumn("LTV", F.when(F.col("AMT_GOODS_PRICE") > 0, F.round(F.col("AMT_CREDIT") / F.col("AMT_GOODS_PRICE"), 4)).otherwise(None))
    .withColumn("Employed_to_Age", F.when(F.col("DAYS_EMPLOYED").isNotNull(), F.round(F.col("DAYS_EMPLOYED") / F.col("DAYS_BIRTH"), 4)).otherwise(0.0))
)
```
### Step 7.2: Historical Application Aggregations (previous_application)
We perform a `groupBy("SK_ID_CURR")` aggregation over previous loan applications to condense multiple historical events into exactly one record per customer.

* **Prev_App_Count:** Total count of previous loans attempted with Home Credit (`COUNT(SK_ID_PREV)`).
* **Approved_App_Ratio:** Proportion of past applications that received approval (`COUNT(Approved) / Total Applications`).
* **Refused_App_Ratio:** Proportion of past applications rejected by the lender (`COUNT(Refused) / Total Applications`).
* **Avg_Prev_Credit:** Baseline average credit amount borrowed across past applications (`AVG(AMT_CREDIT)`).

```python
# Aggregate historical applications per customer
prev_features = clean_prev.groupBy("SK_ID_CURR").agg(
    F.count("SK_ID_PREV").alias("Prev_App_Count"),
    F.round(
        F.sum(F.when(F.col("NAME_CONTRACT_STATUS") == "Approved", 1).otherwise(0)) / F.count("SK_ID_PREV"),
        4
    ).alias("Approved_App_Ratio"),
    F.round(
        F.sum(F.when(F.col("NAME_CONTRACT_STATUS") == "Refused", 1).otherwise(0)) / F.count("SK_ID_PREV"),
        4
    ).alias("Refused_App_Ratio"),
    F.round(F.avg("AMT_CREDIT"), 2).alias("Avg_Prev_Credit")
)
```
### Step 7.3: Installments Repayment Features (installments_payments)
To resolve the partial payments issue where multiple transactions map to a single scheduled installment, we used a two-stage aggregation:
1. **Installment Grain Consolidation:** Grouped by `SK_ID_CURR`, `SK_ID_PREV`, and `DAYS_INSTALMENT` to capture true single installment totals (`max` for scheduled amount, `sum` for actual payments).
2. **Customer Grain Aggregation:** Grouped by `SK_ID_CURR` to generate the 6 target credit-discipline metrics:
   * `Total_Days_Past_Due`: Total cumulative delay in installment payments.
   * `Num_Late_Payments`: Total count of delayed installments.
   * `Avg_Days_Past_Due`: Average delay days across all historical installments.
   * `Max_Days_Past_Due`: Worst single installment delay recorded.
   * `Total_Underpaid`: Cumulative unpaid amount across installments.
   * `Payment_Ratio`: Total paid amount relative to total billed amount.
   
```python
inst_level = (
    clean_inst_fixed
    .groupBy(
        "SK_ID_CURR",
        "SK_ID_PREV",
        "NUM_INSTALMENT_VERSION",
        "NUM_INSTALMENT_NUMBER"
    )
    .agg(
        F.min("DAYS_INSTALMENT").alias("DAYS_INSTALMENT"),
        F.max("DAYS_ENTRY_PAYMENT").alias("DAYS_ENTRY_PAYMENT"),
        F.max("AMT_INSTALMENT").alias("AMT_INSTALMENT"),
        F.sum("AMT_PAYMENT").alias("AMT_PAYMENT")
    )
)

inst_level = (
    inst_level
    .withColumn(
        "Days_Past_Due",
        F.greatest(F.col("DAYS_INSTALMENT") - F.col("DAYS_ENTRY_PAYMENT"), F.lit(0.0))
    )
    .withColumn(
        "Is_Late",
        F.when(F.col("Days_Past_Due") > 0, 1).otherwise(0)
    )
    .withColumn(
        "Underpaid_Amount",
        F.greatest(F.col("AMT_INSTALMENT") - F.col("AMT_PAYMENT"), F.lit(0.0))
    )
)

inst_agg = (
    inst_level
    .groupBy("SK_ID_CURR")
    .agg(
        # Feature 1: Total_Days_Past_Due
        F.round(F.sum("Days_Past_Due"), 2).alias("Total_Days_Past_Due"),
        
        # Feature 2: Num_Late_Payments
        F.sum("Is_Late").alias("Num_Late_Payments"),
        
        # Feature 3: Avg_Days_Past_Due
        F.round(F.avg("Days_Past_Due"), 2).alias("Avg_Days_Past_Due"),
        
        # Feature 4: Max_Days_Past_Due
        F.round(F.max("Days_Past_Due"), 2).alias("Max_Days_Past_Due"),
        
        # Feature 5: Total_Underpaid
        F.round(F.sum("Underpaid_Amount"), 2).alias("Total_Underpaid"),
        
        # Intermediate sums needed for Payment_Ratio
        F.sum("AMT_PAYMENT").alias("SUM_AMT_PAYMENT"),
        F.sum("AMT_INSTALMENT").alias("SUM_AMT_INSTALMENT")
    )
)

inst_features = (
    inst_agg
    .withColumn(
        "Payment_Ratio",
        F.when(
            F.col("SUM_AMT_INSTALMENT") > 0,
            F.round(F.col("SUM_AMT_PAYMENT") / F.col("SUM_AMT_INSTALMENT"), 4)
        ).otherwise(None)
    )
    .drop("SUM_AMT_PAYMENT", "SUM_AMT_INSTALMENT") # Drop intermediate columns
)

```
### Step 7.4: Bureau Credit Features (bureau)
We aggregate historical records reported by external credit bureaus to measure applicant leverage, outside debt exposure, and delinquency behavior across other financial institutions.

* **Bureau_Credit_Count:** Total count of external credit accounts on record (`COUNT(DISTINCT SK_ID_BUREAU)`).
* **Bureau_Active_Loans:** Number of currently active credit facilities (`COUNT(CREDIT_ACTIVE == 'Active')`).
* **Total_External_Debt:** Total outstanding debt obligation across external lenders (`SUM(AMT_CREDIT_SUM_DEBT)`).
* **Total_External_Overdue:** Total balance currently past due across external lenders (`SUM(AMT_CREDIT_SUM_OVERDUE)`).
* **Max_External_Overdue:** Peak historical overdue amount reported (`MAX(AMT_CREDIT_MAX_OVERDUE)`).
* **Bureau_Max_Days_Overdue:** Worst single overdue duration in days (`MAX(CREDIT_DAY_OVERDUE)`).
* **Active_Credit_Ratio:** Proportion of external accounts that are still open (`Bureau_Active_Loans / Bureau_Credit_Count`).
* **Debt_to_Credit_Bureau:** Overall external credit utilization (`Total_External_Debt / Total_Credit_Limit`).

```python
bureau_features = (
    clean_bureau_fixed
    .groupBy("SK_ID_CURR")
    .agg(
        F.countDistinct("SK_ID_BUREAU").alias("Bureau_Credit_Count"),
        F.sum(F.when(F.col("CREDIT_ACTIVE") == "Active", 1).otherwise(0)).alias("Bureau_Active_Loans"),
        F.round(F.sum("AMT_CREDIT_SUM_DEBT"), 2).alias("Total_External_Debt"),
        F.round(F.sum("AMT_CREDIT_SUM_OVERDUE"), 2).alias("Total_External_Overdue"),
        F.round(F.max("AMT_CREDIT_MAX_OVERDUE"), 2).alias("Max_External_Overdue"),
        F.max("CREDIT_DAY_OVERDUE").alias("Bureau_Max_Days_Overdue"),
        F.sum("AMT_CREDIT_SUM").alias("SUM_AMT_CREDIT_SUM")
    )
    .withColumn(
        "Active_Credit_Ratio",
        F.when(
            F.col("Bureau_Credit_Count") > 0,
            F.round(F.col("Bureau_Active_Loans") / F.col("Bureau_Credit_Count"), 4)
        ).otherwise(0.0)
    )
    .withColumn(
        "Debt_to_Credit_Bureau",
        F.when(
            F.col("SUM_AMT_CREDIT_SUM") > 0,
            F.round(F.col("Total_External_Debt") / F.col("SUM_AMT_CREDIT_SUM"), 4)
        ).otherwise(0.0)
    )
    .drop("SUM_AMT_CREDIT_SUM")
)
```
### Step 7.5: Cross-Table Feature Engineering & Fact Construction (Fact_Loan)
We join all aggregated entity summaries onto the central application table using left outer joins on `SK_ID_CURR`. We derive composite risk features, handling missing historical data through zero-coalescing and null validations.

* **Total_Overall_Debt:** Total combined debt obligations (`AMT_CREDIT + COALESCE(Total_External_Debt, 0)`).
* **External_DTI:** Ratio of external debt to reported total income (`Total_External_Debt / AMT_INCOME_TOTAL`).
* **Current_vs_Prev_Credit:** Expansion ratio comparing requested credit to past borrowing scale (`AMT_CREDIT / Avg_Prev_Credit`).
* **Annuity_vs_Historical_Payment:** Payment scale factor assessing new installment affordability against observed payment behavior (`AMT_ANNUITY / Avg_Historical_Payment`).

```python
# Extract historical average payment baseline
avg_payment_df = inst_level.groupBy("SK_ID_CURR").agg(
    F.round(F.avg("AMT_PAYMENT"), 2).alias("Avg_Historical_Payment")
)

# Fact Table Master Left Join
fact_loan = (
    fact_app_features
    .join(prev_features, on="SK_ID_CURR", how="left")
    .join(inst_features, on="SK_ID_CURR", how="left")
    .join(bureau_features, on="SK_ID_CURR", how="left")
    .join(avg_payment_df, on="SK_ID_CURR", how="left")
)

# Derive cross-table composite features
fact_loan = (
    fact_loan
    .withColumn(
        "Total_Overall_Debt",
        F.col("AMT_CREDIT") + F.coalesce(F.col("Total_External_Debt"), F.lit(0.0))
    )
    .withColumn(
        "External_DTI",
        F.when(
            F.col("AMT_INCOME_TOTAL") > 0,
            F.round(F.coalesce(F.col("Total_External_Debt"), F.lit(0.0)) / F.col("AMT_INCOME_TOTAL"), 4)
        ).otherwise(0.0)
    )
    .withColumn(
        "Current_vs_Prev_Credit",
        F.when(
            F.col("Avg_Prev_Credit").isNotNull() & (F.col("Avg_Prev_Credit") > 0),
            F.round(F.col("AMT_CREDIT") / F.col("Avg_Prev_Credit"), 4)
        ).otherwise(None)
    )
    .withColumn(
        "Annuity_vs_Historical_Payment",
        F.when(
            F.col("Avg_Historical_Payment").isNotNull() & (F.col("Avg_Historical_Payment") > 0),
            F.round(F.col("AMT_ANNUITY") / F.col("Avg_Historical_Payment"), 4)
        ).otherwise(None)
    )
)

# Schema & Grain Integrity Validation
# Result: 307,511 rows == 307,511 distinct SK_ID_CURR
assert fact_loan.count() == fact_loan.select("SK_ID_CURR").distinct().count()
```
