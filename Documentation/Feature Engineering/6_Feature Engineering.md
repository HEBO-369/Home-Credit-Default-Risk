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

# Compute primary application-level ratios
fact_app_features = (
    clean_app_fixed
    .withColumn("DTI", F.round(F.col("AMT_CREDIT") / F.col("AMT_INCOME_TOTAL"), 4))
    .withColumn("Annuity_to_Income", F.round(F.col("AMT_ANNUITY") / F.col("AMT_INCOME_TOTAL"), 4))
    .withColumn("LTV", F.round(F.col("AMT_CREDIT") / F.col("AMT_GOODS_PRICE"), 4))
    .withColumn("Employed_to_Age", F.round(F.col("DAYS_EMPLOYED") / F.col("DAYS_BIRTH"), 4))
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
