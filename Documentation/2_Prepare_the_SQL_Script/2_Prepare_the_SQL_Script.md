# Database Setup & Bulk Ingestion Guide

> **Note:** This guide assumes you are running MySQL inside a Docker container (named `my_local_mysql`). If you are running MySQL natively without Docker, ensure your MySQL client has `--local-infile=1` enabled and adjust file paths accordingly.

---

## Prerequisites

* Ensure your MySQL service/container is up and running.
* Dataset CSV files placed inside a `raw/` directory.
* The initialization script `init_db.sql` ready in the current working directory.

---

## 1. Prepare the SQL Script

Make sure the file paths inside `init_db.sql` point to the container's temporary directory (`/tmp/raw/`):

```sql
LOAD DATA LOCAL INFILE '/tmp/raw/application_train.csv'
-- Repeat the same path for the remaining tables:
-- '/tmp/raw/bureau.csv'
-- '/tmp/raw/previous_application.csv'
-- '/tmp/raw/installments_payments.csv'
```

---

## 2. Copy Raw Data into the Container

Transfer the `raw` directory from your host machine into the container's `/tmp` directory:

```bash
docker cp raw my_local_mysql:/tmp/raw
```

---

## 3. Execute the Ingestion Script

Run the script against the container to create the schema and bulk-load the data:

```bash
docker exec -i my_local_mysql mysql -u root -prootpassword --local-infile=1 < init_db.sql
```

*(Note: The process handles millions of rows and will take a few minutes to complete).*

---

## 4. Clean Up Temporary Files

Once the script finishes execution, delete the copied raw data from inside the container to free up storage:

```bash
docker exec my_local_mysql rm -rf /tmp/raw
```