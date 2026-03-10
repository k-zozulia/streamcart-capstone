# AWS Teardown Checklist

Complete all steps below after finishing the project to avoid unexpected AWS charges.

---

## Step 1 — Stop Docker Containers

```bash
cd ~/PycharmProjects/streamcart-capstone/docker
docker-compose down --volumes
docker ps  # should return empty list
```

---

## Step 2 — Delete Glue ETL Job

```bash
aws glue delete-job --job-name streamcart-raw-to-curated --region eu-central-1
```

Or via AWS Console: **Glue → ETL Jobs → streamcart-raw-to-curated → Actions → Delete**

---

## Step 3 — Delete Glue Crawler (if used)

```bash
aws glue delete-crawler --name streamcart-crawler --region eu-central-1
```

---

## Step 4 — Delete CloudWatch Log Groups

```bash
aws logs delete-log-group \
  --log-group-name /aws-glue/jobs/streamcart-raw-to-curated \
  --region eu-central-1

aws logs delete-log-group \
  --log-group-name /aws-glue/jobs/error \
  --region eu-central-1
```

Or via AWS Console: **CloudWatch → Log groups → filter "glue" → Delete**

---

## Step 5 — Delete S3 Objects

```bash
# Delete all objects
aws s3 rm s3://karina-ecommerce-lakehouse/ --recursive

# Verify empty
aws s3 ls s3://karina-ecommerce-lakehouse/

# Optional: delete the bucket itself
aws s3 rb s3://karina-ecommerce-lakehouse
```

Screenshot required: S3 console showing bucket is empty or deleted.

---

## Step 6 — Delete IAM Roles and Policies

```bash
# Detach managed policy from Glue role
aws iam detach-role-policy \
  --role-name streamcart-glue-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Delete inline policies
aws iam delete-role-policy \
  --role-name streamcart-glue-role \
  --policy-name streamcart-s3-access

aws iam delete-role-policy \
  --role-name streamcart-glue-role \
  --policy-name streamcart-cloudwatch-access

# Delete the role
aws iam delete-role --role-name streamcart-glue-role

# Delete pipeline user inline policies
aws iam delete-user-policy \
  --user-name streamcart-pipeline-user \
  --policy-name streamcart-glue-access

aws iam delete-user-policy \
  --user-name streamcart-pipeline-user \
  --policy-name streamcart-s3-pipeline-access

# Delete access keys first, then user
aws iam list-access-keys --user-name streamcart-pipeline-user
aws iam delete-access-key \
  --user-name streamcart-pipeline-user \
  --access-key-id <KEY_ID>
aws iam delete-user --user-name streamcart-pipeline-user
```

---

## Step 7 — Delete Snowflake Objects (optional cleanup)

```sql
USE ROLE SYSADMIN;
DROP STAGE IF EXISTS STREAMCART_DB.RAW.streamcart_curated_stage;
DROP STAGE IF EXISTS STREAMCART_DB.RAW.streamcart_quarantine_stage;

USE ROLE ACCOUNTADMIN;
DROP STORAGE INTEGRATION IF EXISTS streamcart_s3_integration;

-- Optional: drop entire database
-- DROP DATABASE IF EXISTS STREAMCART_DB;
```

---

## Step 8 — Verify No Active Resources

Go to **AWS Console → Billing → Cost Explorer** and confirm:
- No active Glue jobs
- No S3 storage costs
- No CloudWatch log storage

Screenshot: AWS Billing console showing $0 or only the $5 alert threshold.

---

## Checklist Summary

| Step | Resource | Status |
|------|----------|--------|
| 1 | Docker containers stopped | ⬜ |
| 2 | Glue ETL job deleted | ⬜ |
| 3 | Glue Crawler deleted | ⬜ |
| 4 | CloudWatch log groups deleted | ⬜ |
| 5 | S3 objects deleted | ⬜ |
| 6 | IAM roles and users deleted | ⬜ |
| 7 | Snowflake stages and integration dropped | ⬜ |
| 8 | Billing console verified | ⬜ |
