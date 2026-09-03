# Spark Processing Output Handoff

## Purpose

This document explains the processed Spark outputs that should be used in the next stage of the Home Credit data warehouse project.

The final warehouse grain is:

> **One row per current application/customer (`SK_ID_CURR`)**

The three historical tables were aggregated first so that they are safe to join with the main application table.

---

# 1. Main Application Features — `df_app_features`

This DataFrame represents the **current loan application**.

### Join Key
- `SK_ID_CURR` — unique ID of the current application/customer.

### Main Raw Features
- `TARGET` — 1 means payment difficulty/default outcome, 0 means no payment difficulty.
- `AMT_INCOME_TOTAL` — applicant total reported income.
- `AMT_CREDIT` — amount of credit granted for the current application.
- `AMT_ANNUITY` — scheduled periodic payment amount.
- `AMT_GOODS_PRICE` — price of the financed goods.
- `NAME_CONTRACT_TYPE` — type of current credit contract.
- Applicant profile fields such as occupation, education, family status, housing type, and income type.

### Derived Features
- `Employed_to_Age` — employment duration relative to the applicant's age.
  - Higher value means the applicant has spent a larger part of their life employed.

- `Credit_to_Income` — `AMT_CREDIT / AMT_INCOME_TOTAL`
  - Shows how large the loan is compared with the applicant's income.

- `Annuity_to_Income` — `AMT_ANNUITY / AMT_INCOME_TOTAL`
  - Shows how large the scheduled payment is compared with income.

- `LTV` — `AMT_CREDIT / AMT_GOODS_PRICE`
  - Credit-to-goods-price ratio used as an LTV proxy in this learning project.

- `Credit_to_Annuity` — `AMT_CREDIT / AMT_ANNUITY`
  - Shows the relationship between total credit and scheduled payment.

---

# 2. Previous Applications Features — `prev_agg`

This DataFrame summarizes the customer's **previous Home Credit applications**.

### Grain
- One row per `SK_ID_CURR`.

### Features
- `Prev_App_Count` — total number of previous Home Credit applications.
- `Approved_App_Count` — number of previous applications that were approved.
- `Refused_App_Count` — number of previous applications that were refused.
- `Approved_App_Ratio` — approved applications divided by all previous applications.
- `Refused_App_Ratio` — refused applications divided by all previous applications.
- `Prev_Total_Credit` — total credit amount across previous applications.
- `Prev_Avg_Credit` — average previous credit amount.
- `Prev_Avg_Annuity` — average scheduled payment across previous applications.
- `Prev_Avg_Payment_Term` — average planned number of installments.
- `Days_Since_Last_Prev_App` — number of days since the most recent previous application.

---

# 3. Installment Payment Features — `inst_agg`

This DataFrame summarizes how the customer **actually repaid previous Home Credit installments**.

### Grain
- One row per `SK_ID_CURR`.

### Features
- `Total_Installments` — total number of historical installments.
- `Total_Expected_Payment` — total amount that should have been paid.
- `Total_Actual_Payment` — total amount actually paid.
- `Total_Days_Past_Due` — sum of all payment-delay days.
- `Num_Late_Payments` — number of installments paid after the scheduled due date.
- `Late_Payment_Ratio` — late installments divided by total installments.
- `Avg_Days_Past_Due` — average payment delay in days.
- `Max_Days_Past_Due` — worst historical payment delay.
- `Total_Underpaid` — total amount by which installment payments were below the expected amount.
- `Payment_Ratio` — `Total_Actual_Payment / Total_Expected_Payment`.

---

# 4. Bureau Features — `bureau_agg`

This DataFrame summarizes the customer's **credit history reported by other lenders**.

### Grain
- One row per `SK_ID_CURR`.

### Features
- `Bureau_Credit_Count` — number of external credit accounts.
- `Active_Credit_Count` — number of external credits that are still active.
- `Active_Credit_Ratio` — active external credits divided by all bureau credits.
- `Bureau_Total_Credit` — total amount of external credit recorded by the credit bureau.
- `Bureau_Total_Debt` — total remaining external debt.
- `Debt_to_Credit_Ratio` — remaining debt divided by total external credit.
- `Bureau_Total_Overdue` — total amount currently overdue with other lenders.
- `Bureau_Max_Days_Overdue` — worst number of overdue days across external credits.

---

# 5. How to Use These Outputs

Join the processed outputs using:

```text
SK_ID_CURR
```

Recommended pattern:

```python
fact_loan = (
    df_app_features
    .join(prev_agg, "SK_ID_CURR", "left")
    .join(inst_agg, "SK_ID_CURR", "left")
    .join(bureau_agg, "SK_ID_CURR", "left")
)
```

Use **left joins** because `df_app_features` is the main current-application dataset and some applicants may not have previous applications, payment history, or bureau records.

---

# 6. Critical Rule Before Joining

Each aggregated DataFrame must contain at most one row per `SK_ID_CURR`.

Expected grain:

```text
df_app_features  -> one row per current application
prev_agg         -> one row per SK_ID_CURR
inst_agg         -> one row per SK_ID_CURR
bureau_agg       -> one row per SK_ID_CURR
```

Do not join the raw `previous_application`, `installments_payments`, or `bureau` tables directly to the main application table.

---

# 7. Final Validation

After joining, verify:

```python
fact_loan.count()
```

and:

```python
fact_loan.select("SK_ID_CURR").distinct().count()
```

These two values should be equal.

For the original `application_train` dataset, the expected grain is:

```text
307,511 rows
307,511 distinct SK_ID_CURR
```

This confirms that `fact_loan` still contains one row per current application.
