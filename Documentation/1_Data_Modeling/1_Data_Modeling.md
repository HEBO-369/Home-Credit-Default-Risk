# Data Modeling & Star Schema (Home Credit Default Risk)

This document outlines the final Star Schema design for our Data Engineering pipeline. It consists of a Dimension table for descriptive customer data (used for Power BI dashboards) and a Fact table containing both raw and engineered numeric features (used for Machine Learning).

---

## 1. Dimension Table: `Dim_Customer`
This table contains the descriptive data of the customer and is primarily used for filters (Slicers) in the Power BI dashboard.

| Column Name | Calculation / Status | Source Table |
| :--- | :--- | :--- |
| `SK_ID_CURR` | Existing | `application_train` (Serves as the unique Primary Key (PK) to guarantee exactly one row per customer). |
| `CODE_GENDER` | Existing | `application_train` |
| `FLAG_OWN_CAR` | Existing | `application_train` |
| `NAME_CONTRACT_TYPE` | Existing | `application_train` |
| `OCCUPATION_TYPE` | Existing | `application_train` |
| `NAME_EDUCATION_TYPE` | Existing | `application_train` |
| `NAME_FAMILY_STATUS` | Existing | `application_train` |
| `NAME_HOUSING_TYPE` | Existing | `application_train` |
| `NAME_INCOME_TYPE` | Existing | `application_train` |
| `FLAG_OWN_REALTY` | Existing | `application_train` |
---


## 2. Fact Table: `Fact_Loan`
This is the central table (Feature Matrix). It contains the raw numbers necessary for the AI model as well as the computed columns (Feature Engineering) that the team will create through GroupBy and Join operations.

### A. Raw Columns (Taken as is)
| Column Name | Calculation / Status | Source Table |
| :--- | :--- | :--- |
| `SK_ID_CURR` | Existing (Join Key) | `application_train` |
| `TARGET` | Existing (Target Label) | `application_train` |
| `AMT_INCOME_TOTAL` | Existing | `application_train` |
| `AMT_CREDIT` | Existing | `application_train` |
| `AMT_ANNUITY` | Existing | `application_train` |
| `AMT_GOODS_PRICE` | Existing | `application_train` |
| `DAYS_EMPLOYED` | Existing | `application_train` |
| `DAYS_BIRTH` | Existing (Age as a numeric value for the ML model) | `application_train` |

> 🛑 **CRITICAL WARNING - READ BEFORE WRITING ANY CODE!** 🛑
> 
> **DO NOT**, under any circumstances, attempt to join the secondary tables (`previous_application`, `installments_payments`, `bureau`) directly to the main table as they are. 
> 
> You **ABSOLUTELY MUST** perform a `GroupBy("SK_ID_CURR")` on these tables FIRST to aggregate the data. Ensure that your output DataFrame has exactly **ONE row per customer** before sending it for the final Join. 
> 
> Skipping this step will result in a massive Many-to-Many data explosion, duplicate records, and will immediately CRASH our entire PySpark pipeline due to memory overload!
> 
### B. Engineered Features (Requires PySpark processing)

| Column Name | Calculation / Status | Source Table | Business Rationale (Why we added it) |
| :--- | :--- | :--- | :--- |
| `DTI` | `AMT_CREDIT` / `AMT_INCOME_TOTAL` | `application_train` | Measures financial burden; a high ratio indicates potential struggle to repay. |
| `Annuity_to_Income` | `AMT_ANNUITY` / `AMT_INCOME_TOTAL` | `application_train` | Assesses if the regular loan installment is affordable given the customer's income. |
| `LTV` | `AMT_CREDIT` / `AMT_GOODS_PRICE` | `application_train` | Evaluates collateral risk; high LTV means the loan exceeds the asset's actual value. |
| `Employed_to_Age` | `DAYS_EMPLOYED` / `DAYS_BIRTH` | `application_train` | Indicates employment stability relative to the applicant's age. |
| `Prev_App_Count` | `COUNT(SK_ID_PREV)` per `SK_ID_CURR` | `previous_application` | Shows the customer's historical reliance on Home Credit loans. |
| `Approved_App_Ratio` | `COUNT(STATUS == Approved)` / `Prev_App_Count` | `previous_application` | Reflects the customer's historical creditworthiness and success rate with us. |
| `Refused_App_Ratio` | `COUNT(STATUS == Refused)` / `Prev_App_Count` | `previous_application` | Highlights past rejections, which is a strong indicator of historical risk. |
| `Avg_Prev_Credit` | `AVG(AMT_CREDIT)` for previous loans | `previous_application` | Establishes a baseline for the customer's typical borrowing size. |
| `Total_Days_Past_Due` | `SUM(MAX(DAYS_INSTALMENT - DAYS_ENTRY_PAYMENT, 0))` | `installments_payments` | Quantifies the overall severity of historical payment delays. |
| `Num_Late_Payments` | `COUNT(DAYS_INSTALMENT > DAYS_ENTRY_PAYMENT)` | `installments_payments` | Indicates the frequency of poor repayment behavior and lack of discipline. |
| `Avg_Days_Past_Due` | `AVG(MAX(DAYS_INSTALMENT - DAYS_ENTRY_PAYMENT, 0))` | `installments_payments` | Shows the typical delay length, differentiating chronic lateness from minor slips. |
| `Max_Days_Past_Due` | `MAX(DAYS_INSTALMENT - DAYS_ENTRY_PAYMENT)` | `installments_payments` | Identifies the worst-case historical default behavior for this customer. |
| `Total_Underpaid` | `SUM(MAX(AMT_INSTALMENT - AMT_PAYMENT, 0))` | `installments_payments` | Highlights situations where the customer consistently paid less than expected. |
| `Payment_Ratio` | `SUM(AMT_PAYMENT)` / `SUM(AMT_INSTALMENT)` | `installments_payments` | Measures overall repayment discipline (1.0 = perfect, < 1.0 = underpaying). |
| `Bureau_Active_Loans`| `COUNT(CREDIT_ACTIVE == 'Active')` | `bureau` | Shows the customer's current active exposure to other external lenders. |
| `Total_External_Debt` | `SUM(AMT_CREDIT_SUM_DEBT)` | `bureau` | Quantifies the total financial obligation outside of Home Credit. |
| `Total_External_Overdue`| `SUM(AMT_CREDIT_SUM_OVERDUE)` | `bureau` | Strong risk signal showing the exact amount currently defaulted with other lenders. |
| `Max_External_Overdue` | `MAX(AMT_CREDIT_MAX_OVERDUE)` | `bureau` | Highlights the worst external default, indicating severe financial distress. |
| `Debt_to_Credit_Bureau`| `SUM(AMT_CREDIT_SUM_DEBT)` / `SUM(AMT_CREDIT_SUM)` | `bureau` | Indicates credit utilization across all external accounts. |
| `Bureau_Credit_Count` | `COUNT(DISTINCT SK_ID_BUREAU)` | `bureau` | Total external credit accounts on record. |
| `Active_Credit_Ratio` | `Bureau_Active_Loans / Bureau_Credit_Count` | `bureau` | Ratio of active obligations to total historical credit lines. |
| `Bureau_Max_Days_Overdue` | `MAX(CREDIT_DAY_OVERDUE)` | `bureau` | Captures behavioral default severity in terms of time. |
| `External_DTI` | `Total_External_Debt` / `AMT_INCOME_TOTAL` | `bureau` + `application_train` | Measures total external debt burden against the customer's actual income. |
| `Current_vs_Prev_Credit` | `AMT_CREDIT` / `Avg_Prev_Credit` | `app_train` + `previous_app` | Detects unusual borrowing behavior (e.g., requesting significantly more than usual). |
| `Total_Overall_Debt` | `AMT_CREDIT` + `Total_External_Debt` | `app_train` + `bureau` | Provides the complete, combined debt picture (Home Credit + external lenders). |
| `Annuity_vs_Historical_Payment`| `AMT_ANNUITY` / `AVG(AMT_PAYMENT)` | `app_train` + `installments` | Checks if the new payment is realistic compared to what they historically afforded. |