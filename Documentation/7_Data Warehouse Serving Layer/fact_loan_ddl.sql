-- Deliverable 3: Hive DDL for Fact_Loan Table
CREATE DATABASE IF NOT EXISTS home_credit_dw;

USE home_credit_dw;

CREATE EXTERNAL TABLE IF NOT EXISTS fact_loan (
    loan_key BIGINT,
    SK_ID_CURR INT,
    TARGET INT,
    DAYS_BIRTH INT,
    AMT_INCOME_TOTAL DOUBLE,
    AMT_CREDIT DOUBLE,
    AMT_ANNUITY DOUBLE,
    AMT_GOODS_PRICE DOUBLE,
    DAYS_EMPLOYED INT,
    DTI DOUBLE,
    Annuity_to_Income DOUBLE,
    LTV DOUBLE,
    Employed_to_Age DOUBLE,
    Prev_App_Count INT,
    Approved_App_Ratio DOUBLE,
    Refused_App_Ratio DOUBLE,
    Avg_Prev_Credit DOUBLE,
    Total_Days_Past_Due DOUBLE,
    Num_Late_Payments INT,
    Avg_Days_Past_Due DOUBLE,
    Max_Days_Past_Due DOUBLE,
    Total_Underpaid DOUBLE,
    Payment_Ratio DOUBLE,
    Bureau_Credit_Count INT,
    Bureau_Active_Loans INT,
    Total_External_Debt DOUBLE,
    Total_External_Overdue DOUBLE,
    Max_External_Overdue DOUBLE,
    Bureau_Max_Days_Overdue INT,
    Active_Credit_Ratio DOUBLE,
    Debt_to_Credit_Bureau DOUBLE,
    Avg_Historical_Payment DOUBLE,
    Total_Overall_Debt DOUBLE,
    External_DTI DOUBLE,
    Current_vs_Prev_Credit DOUBLE,
    Annuity_vs_Historical_Payment DOUBLE
)
PARTITIONED BY (NAME_CONTRACT_TYPE STRING)
STORED AS PARQUET
LOCATION '/user/student/home_credit/warehouse/fact_loan'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Repair table to register partitions created by Spark
MSCK REPAIR TABLE fact_loan;