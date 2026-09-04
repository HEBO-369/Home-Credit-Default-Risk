## 1. Project Overview & Architecture

This phase implements an end-to-end Machine Learning pipeline to predict loan default risks (`TARGET`) using the Home Credit dataset. The modeling workflow is designed to consume data directly from the Spark data warehouse layer, integrating the partitioned `fact_loan` table and the `dim_customer` table. 

Key technical aspects addressed in this pipeline include:
* **Storage Ingestion:** Robust ingestion of Hive-partitioned Parquet files, ensuring full recovery of partition keys and surrogate keys without schema mismatch.
* **Class Imbalance Mitigation:** Handling severe target imbalance (~91.93% non-default vs. ~8.07% default) through dynamically weighted loss functions to prioritize minority-class recall.
* **Missing Value Strategy:** Preserving informative missing indicators across financial and bureau history features, allowing tree-based gradient boosting algorithms to handle nulls natively without synthetic distortion.
* **Evaluation Framework:** Centering performance verification on discrimination metrics (ROC-AUC score, Precision-Recall balance, and confusion matrix diagnostics) rather than baseline accuracy.

## 2. Data Ingestion & Deduplication

To bridge the analytical warehouse with the machine learning environment, data is ingested directly from the local Parquet storage directories (`fact_loan` and `dim_customer`). The ingestion pipeline resolves structural partition artifacts and ensures record-level consistency through the following steps:

* **Partition-Aware Loading:** The root-level directory of `fact_loan` contains structural partition folders split by contract type (`NAME_CONTRACT_TYPE=*`). Using `pathlib.Path` and `pyarrow`, individual partition files are discovered recursively and concatenated into a unified DataFrame, automatically preserving partition labels and preventing empty schema reads.
* **Hive-Dataset Parsing:** The `dim_customer` dimension table is read using `pyarrow.dataset` with explicit Hive-style partitioning, mapping table schemas directly into memory.
* **Deduplication Strategy:** To neutralize duplicate entries resulting from Spark overwrite commands or incremental pipeline reruns, records are deduplicated based on the primary customer key (`SK_ID_CURR`), retaining the latest record instance.
* **Relational Merging:** The cleaned fact and dimension DataFrames are merged via an inner join on `SK_ID_CURR`. Redundant join suffixes are dynamically stripped to prevent column collision, resulting in a consolidated dataset of 307,511 unique loan applicants.

## 3. Data Cleaning & Preprocessing

Before passing features into training, data hygiene and schema refinement are applied to eliminate leakage and prepare categorical inputs:

* **Identifier & Key Removal:** Non-predictive identification columns—specifically the primary customer ID (`SK_ID_CURR`) and the synthetic surrogate key (`loan_key`)—are dropped to prevent the model from memorizing record ordering or arbitrary surrogate mappings.
* **Missing Value Preservation:** Numerical features containing null values (such as `DAYS_EMPLOYED`, external bureau indicators, and previous application ratios) are kept intact without synthetic imputation, allowing tree-based decision algorithms to treat missingness as an informative signal.
* **Categorical Encoding:** Object and categorical attributes (`CODE_GENDER`, `FLAG_OWN_CAR`, `NAME_CONTRACT_TYPE`, `OCCUPATION_TYPE`, `NAME_EDUCATION_TYPE`, `NAME_FAMILY_STATUS`, `NAME_HOUSING_TYPE`, `NAME_INCOME_TYPE`, `FLAG_OWN_REALTY`) are transformed using One-Hot Encoding (`pd.get_dummies`) with `drop_first=True` to avoid the dummy variable trap and reduce collinearity, expanding the feature space to 78 columns.

## 4. Dataset Splitting & Numerical Standardization

To ensure fair evaluation and prevent data leakage, the processed dataset is systematically partitioned and scaled:

* **Target & Feature Separation:** The dependent variable (`TARGET`) is isolated from the explanatory feature matrix ($X$), leaving 77 training features.
* **Stratified Train-Test Split:** The dataset is partitioned into an 80% training set (246,008 samples) and a 20% test set (61,503 samples). Stratification on `y` is enforced to guarantee that the extreme ~8.07% default rate is identically represented across both splits.
* **Feature Standardization:** Numerical attributes (`int64` and `float64`) are standardized using `StandardScaler`. The scaler is fitted strictly on the training partition (`X_train`) and subsequently used to transform both `X_train` and `X_test`, preventing validation leakage while placing high-variance financial totals and ratios on a unified scale.

## 5. Handling Class Imbalance & Model Training

To counteract the severe class skew in the target variable, algorithmic cost adjustments are integrated directly into the model objective:

* **Dynamic Class Weighting:** An empirical ratio of majority-to-minority classes is calculated ($N_{\text{negative}} / N_{\text{positive}} \approx 11.39$). This value is passed directly to the `scale_pos_weight` parameter, heavily penalizing false negatives and compelling the model to treat default instances with equal operational importance.
* **Gradient Boosting Architecture:** An `XGBClassifier` is deployed to capture complex non-linear credit patterns and manage missing data natively. Key hyperparameters include:
  * `scale_pos_weight=11.39`: Mitigates class imbalance without requiring synthetic oversampling techniques.
  * `n_estimators=200` & `learning_rate=0.1`: Establishes progressive, stable residual shrinkage across boosting stages.
  * `max_depth=5`: Limits individual tree complexity to guard against overfitting on noisy financial anomalies.
  * `subsample=0.8`: Injects row-level stochasticity to bolster tree variance reduction and generalization.
  * `n_jobs=-1`: Distributes tree building across all available CPU cores for efficient local training.

  ## 6. Model Evaluation & Diagnostics

Performance validation is structured around discrimination capacity and classification behavior under cost-sensitive weighting:

* **Primary Evaluation Metric (ROC-AUC):** Because overall accuracy is misleading in highly skewed settings, evaluation focuses on the Receiver Operating Characteristic Area Under the Curve (ROC-AUC). The model yields a test ROC-AUC score of `0.7172`, demonstrating solid discriminative ability between defaulting and non-defaulting borrowers.
* **Confusion Matrix Analysis:** Class-level diagnostics on the 61,503 test instances illustrate the deliberate trade-off created by the class weighting:
  * `True Negatives (0 predicted as 0)`: 40,147 instances.
  * `False Positives (0 predicted as 1)`: 16,391 instances.
  * `False Negatives (1 predicted as 0)`: 1,987 instances.
  * `True Positives (1 predicted as 1)`: 2,978 instances.
* **Classification Report Breakdown:**
  * `Class 0 (Non-Default)`: Achieves a high precision of `0.95`, a recall of `0.71`, and an F1-score of `0.81`.
  * `Class 1 (Default)`: Prioritizes recall (`0.60`), successfully capturing 60% of all real defaults at the expense of precision (`0.15`) due to increased false alarms, leading to an F1-score of `0.24`.
  * `Overall Baseline`: Delivers an overall classification accuracy of `70%` with a weighted average F1-score of `0.77`, confirming that the pipeline prioritizes credit risk detection over naive majority-class guessing.
  ## 7. Key Findings & Future Enhancements

The implementation highlights critical trade-offs and avenues for iterative optimization:

* **Operational Risk Trade-off:** By configuring `scale_pos_weight`, the model effectively identifies the majority of credit defaults (60% recall), preventing catastrophic lending losses at the cost of a higher false-positive rate (15% precision). In production, this output serves as a triage threshold where flagged applications can undergo secondary human review rather than outright rejection.
* **Feature Importance Insights:** Risk indicators driven by external credit bureau records (`Bureau_*`), debt-to-income (`DTI`), loan-to-value (`LTV`), and employment stability metrics contribute heavily to separating default likelihoods.
* **Future Optimization Strategies:**
  * **Threshold Tuning:** Shifting the classification probability threshold away from the default $0.5$ toward a business-aligned threshold to balance approval volume and loss mitigation.
  * **Advanced Hyperparameter Tuning:** Utilizing Bayesian optimization (e.g., Optuna) over tree depth, learning rate, and regularizers (`reg_alpha`, `reg_lambda`) to push the ROC-AUC score higher.
  * **Alternative Architectures:** Benchmarking against histogram-based gradient boosters like LightGBM and CatBoost, which offer tailored handling of categorical variables and faster convergence on wide tabular datasets.