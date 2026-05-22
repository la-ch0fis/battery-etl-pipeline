# Battery ETL Pipeline

AWS serverless ETL pipeline that filters and transforms cognitive battery test data, stores the results as an Iceberg table in Glue Data Catalog, and makes it queryable via Athena or PySpark SQL.

---

## Architecture

```
EventBridge (schedule)
        │
        ▼
  Lambda (start_etl)          ← triggers the State Machine with bucket params
        │
        ▼
Step Functions (State Machine)
        │
        ├─▶ Glue Job: etl_job            ← filter + transform + join → Parquet
        │         reads  : s3://raw/battery14_df.csv
        │                  s3://raw/degrees.csv
        │         writes : s3://processed/transformed/
        │
        └─▶ Glue Job: iceberg_job        ← Parquet → Iceberg table
                  reads  : s3://processed/transformed/
                  writes : s3://iceberg/warehouse/  (Glue Data Catalog)
```

### AWS Services Used

| Service | Role |
|---|---|
| EventBridge | Scheduled trigger (cron/rate) |
| Lambda | Starts the Step Function execution |
| Step Functions | Orchestrates Glue jobs sequentially |
| Glue (PySpark) | ETL transform and Iceberg table creation |
| S3 | Raw data, processed Parquet, Iceberg warehouse, Glue scripts |
| Glue Data Catalog | Iceberg table metadata |
| Athena | Ad-hoc SQL queries on the Iceberg table |

### Infrastructure-as-Code

All resources are defined in `template.yaml` using **AWS SAM** (`Transform: AWS::Serverless-2016-10-31`).

---

## ETL Logic

**Filter criteria** applied to `battery14_df.csv`:
- `gender == 'f'`
- `age > 30`
- `country == 'US'`
- `education_level > 6` (higher than Master's degree = 6)
- `raw_score > 300`

**Transformations:**
- `gender`: `'f'` → `'Female'`, `'m'` → `'Male'`
- `education_level` code joined with `degrees.csv` to add a human-readable `description` column

**Output schema:**

| Column | Type | Notes |
|---|---|---|
| education_level | int | numeric code |
| user_id | int | |
| age | double | |
| gender | string | `Female` / `Male` |
| country | string | |
| test_run_id | int | |
| battery_id | int | |
| specific_subtest_id | int | |
| raw_score | double | |
| time_of_day | int | |
| grand_index | double | |
| description | string | degree name from degrees.csv |

---

## Project Structure

```
.
├── template.yaml                    # SAM template (all infrastructure)
├── Makefile                         # Deployment automation
├── degrees.csv                      # Reference lookup — upload to raw bucket
├── battery14_df.csv                 # Source data — upload to raw bucket
├── src/
│   ├── glue/
│   │   ├── etl_job.py               # Glue Job 1: filter, transform, join
│   │   └── iceberg_job.py           # Glue Job 2: write Iceberg table
│   └── lambda/
│       └── start_etl/
│           └── handler.py           # Lambda: start Step Function execution
└── statemachine/
    └── etl_pipeline.asl.json        # Step Function ASL definition
```

---

## Prerequisites

### Tools (install locally)
1. **AWS CLI v2** — `aws --version`
2. **AWS SAM CLI** — `sam --version`
3. **Python 3.12** — for SAM build

### AWS Account Setup — Manual Steps

These steps must be done **once** in the AWS console or via CLI before running `make deploy`.

---

#### Step 1 — Configure AWS credentials

```bash
aws configure
# Enter: Access Key ID, Secret Access Key, Region (e.g. us-east-1), output format (json)
```

Verify:
```bash
aws sts get-caller-identity
```

---

#### Step 2 — Ensure the deploying IAM user/role has sufficient permissions

The identity running `make deploy` needs the following IAM permissions (attach inline or via a managed policy):

- `cloudformation:*`
- `s3:*`
- `iam:CreateRole`, `iam:AttachRolePolicy`, `iam:PutRolePolicy`, `iam:PassRole`, `iam:GetRole`, `iam:DeleteRole`, `iam:DeleteRolePolicy`, `iam:DetachRolePolicy`
- `lambda:*`
- `states:*`
- `glue:*`
- `events:*`
- `logs:*`
- `xray:*`

> **Simplest option for a dev/test account:** attach `AdministratorAccess` to the deploying user.  
> **Production:** create a least-privilege deployment role scoped to the above actions.

---

#### Step 3 — Enable Athena (for validation only)

1. Go to **Athena** in the AWS Console.
2. Click **Settings → Manage**.
3. Set the **Query result location** to: `s3://battery-etl-raw-<YOUR_ACCOUNT_ID>/athena-results/`
4. Set **Engine version** to **Athena engine version 3** (required for Iceberg queries).
5. Click **Save**.

This is a one-time console action; it cannot be configured via CloudFormation.

---

#### Step 4 — (Optional) Request Glue worker quota increase

Default quota for Glue G.1X workers is **10 per account per region**.  
The pipeline uses 4 workers total (2 per job × 2 jobs). No increase needed.

---

## Deployment

```bash
# 1. Deploy the full stack (creates all S3 buckets, IAM roles, Glue jobs, Lambda, Step Functions)
make deploy

# 2. Upload the Glue PySpark scripts to the scripts bucket
make upload-scripts

# 3. Upload raw data files to the raw bucket
make upload-data
```

# I tried to use Mexico's region but I was reading the documentation and since the region is relatively new (opened in 2025) I could have had issues with some services not being available 
Custom region or environment:
```bash
make deploy ENV=prod REGION=us-east-1
```

---

## Running the Pipeline

### Automatic
EventBridge fires the Lambda once per day (configurable via `ScheduleExpression` parameter).

### Manual trigger
```bash
make trigger
```

This invokes the Lambda directly and prints the Step Function execution ARN.

Monitor execution in the AWS console:  
**Step Functions → State machines → battery-etl-pipeline-dev → Executions**

---

## Validation

### Option A — SQL PySpark (built into iceberg_job.py)
The Iceberg job automatically runs a validation query and prints:
- Row count
- Sample of 5 rows (user_id, age, gender, description, raw_score, country)

Check Glue job logs in **CloudWatch → Log groups → /aws-glue/jobs/output**.

### Option B — AWS Athena

1. Open **Athena** in the AWS console.
2. Select database: `battery_etl_db`
3. Run:

```sql
-- Row count
SELECT COUNT(*) AS total FROM "battery_etl_db"."battery_results";

-- Sample rows
SELECT user_id, age, gender, description, raw_score, country
FROM "battery_etl_db"."battery_results"
LIMIT 10;

-- Iceberg time-travel (shows table history)
SELECT * FROM "battery_etl_db"."battery_results$history";
```

Expected result: **294 rows** matching all filter criteria.

### Option C — Makefile shortcut
```bash
make validate
```

Starts an Athena query execution and returns the QueryExecutionId. Fetch results from the raw bucket's `athena-results/` prefix.

---

## Teardown

```bash
make clean
```

> This deletes the CloudFormation stack. **S3 buckets with data will block deletion** if they are not empty. Empty them manually first or add a bucket lifecycle policy for auto-cleanup.

To empty a bucket before teardown:
```bash
aws s3 rm s3://<bucket-name> --recursive
```

---

## Configuration Reference

| Makefile variable | Default | Description |
|---|---|---|
| `PROJECT` | `battery-etl` | Resource name prefix |
| `ENV` | `dev` | Environment suffix |
| `REGION` | `us-east-1` | AWS region |

| SAM Parameter | Default | Description |
|---|---|---|
| `ProjectName` | `battery-etl` | Resource name prefix |
| `Environment` | `dev` | dev / prod |
| `ScheduleExpression` | `rate(1 day)` | EventBridge schedule |
| `GlueDatabase` | `battery_etl_db` | Glue Data Catalog DB name |
| `IcebergTable` | `battery_results` | Iceberg table name |
