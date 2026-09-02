## 4. Big Data Ingestion (HDFS & Apache Sqoop)

### Overview
Extract raw relational tables from the host Dockerized MySQL instance into the Hadoop Distributed File System (HDFS) landing zones using Apache Sqoop MapReduce tasks, maintaining proper multi-user permission separation (service daemons vs. client execution).

---

### Step 1: Verify Host Connection & IP Address
Since MySQL is running on the host machine (in Docker) while Sqoop runs inside the virtual machine, identify your host IP address first.

Run this on your **host machine terminal** (outside the VM):
```bash
hostname -I
```
* **Host IP**: Use your primary host IP (e.g., `10.29.51.13`).
* **Connection String**: `jdbc:mysql://<HOST_IP>:3306/home_credit`
* **Port & Credentials**: Default MySQL port `3306`, user `root`, password `rootpassword`.
*(Note: If MySQL is installed locally inside the VM itself without Docker, replace `<HOST_IP>` with `localhost` and adapt credentials accordingly).*

---

### Step 2: Configure Service Account Permissions (Hadoop User)
To emulate production role separation, switch to the cluster service account (`hadoop`) to start system daemons and provision the client's HDFS workspace with proper ownership.

From the VM terminal:
```bash
# Fix ownership and permissions for daemon log directories
sudo chown -R hadoop:hadoop /home/hadoop/hadoop/logs
sudo chmod 775 /home/hadoop/hadoop/logs

# Switch to the hadoop service user
su - hadoop
# (Enter password: hadoop)

# Ensure cluster daemons are active
start-dfs.sh
start-yarn.sh

# Create client workspace on HDFS and assign ownership to user 'student'
hdfs dfs -mkdir -p /user/student
hdfs dfs -chown -R student:student /user/student

# Return to your standard user
exit
```

---

### Step 3: Create Landing Zones (Student User)
Now running as `student`, prepare the raw ingestion directories in HDFS:

```bash
hdfs dfs -mkdir -p /user/student/home_credit/raw/application_train
hdfs dfs -mkdir -p /user/student/home_credit/raw/previous_application
hdfs dfs -mkdir -p /user/student/home_credit/raw/bureau
hdfs dfs -mkdir -p /user/student/home_credit/raw/installments_payments
```

---

### Step 4: Execute Sqoop Ingestion Jobs
Run the Sqoop import jobs connecting to the host database. Each job triggers a MapReduce task submitting to YARN:

```bash
# 1. Import application_train (~307K rows)
sqoop import \
  --connect jdbc:mysql://10.29.51.13:3306/home_credit \
  --username root \
  --password rootpassword \
  --table application_train \
  --target-dir /user/student/home_credit/raw/application_train \
  --delete-target-dir \
  -m 1

# 2. Import previous_application (~1.67M rows)
sqoop import \
  --connect jdbc:mysql://10.29.51.13:3306/home_credit \
  --username root \
  --password rootpassword \
  --table previous_application \
  --target-dir /user/student/home_credit/raw/previous_application \
  --delete-target-dir \
  -m 1

# 3. Import bureau (~1.71M rows)
sqoop import \
  --connect jdbc:mysql://10.29.51.13:3306/home_credit \
  --username root \
  --password rootpassword \
  --table bureau \
  --target-dir /user/student/home_credit/raw/bureau \
  --delete-target-dir \
  -m 1

# 4. Import installments_payments (~13.6M rows)
sqoop import \
  --connect jdbc:mysql://10.29.51.13:3306/home_credit \
  --username root \
  --password rootpassword \
  --table installments_payments \
  --target-dir /user/student/home_credit/raw/installments_payments \
  --delete-target-dir \
  -m 1
```

---

### Step 5: Verify Ingestion Results
Verify that partition data (`part-m-00000`) and job completion tokens (`_SUCCESS`) landed properly in HDFS:

```bash
hdfs dfs -ls -h /user/student/home_credit/raw/*
```