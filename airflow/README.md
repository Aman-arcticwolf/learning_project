# Airflow Configuration for Data & AI Pipeline

This directory contains Apache Airflow DAGs for orchestrating the data pipeline.

## DAG Overview

**DAG Name:** `corporate_data_ai_pipeline`

**Schedule:** Daily at 2 AM UTC (`0 2 * * *`)

**Tasks:**
1. **check_source_data** - Verify source data files exist in S3
2. **create_emr_cluster** - Spin up EMR cluster for Spark processing
3. **add_pipeline_step** - Submit Spark job to EMR
4. **wait_for_pipeline_completion** - Monitor job execution
5. **validate_pipeline** - Validate successful completion
6. **terminate_emr_cluster** - Clean up EMR resources
7. **send_completion_notification** - Send alerts/notifications

## Prerequisites

### Airflow Variables

Set these variables in Airflow UI (Admin -> Variables):

```
s3_bucket_name: corporate-data-pipeline
emr_subnet_id: subnet-xxxxx
emr_ec2_instance_profile: data-ai-pipeline-emr-ec2-profile
emr_service_role: arn:aws:iam::xxxx:role/data-ai-pipeline-emr-service-role
```

### Airflow Connections

Configure AWS connection in Airflow UI (Admin -> Connections):

**Connection ID:** `aws_default`
- **Connection Type:** Amazon Web Services
- **AWS Access Key ID:** Your AWS access key
- **AWS Secret Access Key:** Your AWS secret key
- **Region:** us-east-1

## Local Testing with Docker

### 1. Install Docker and Docker Compose

### 2. Create docker-compose.yml for Airflow

```bash
cd airflow
docker-compose up -d
```

### 3. Access Airflow UI

Open http://localhost:8080
- Username: admin
- Password: admin

### 4. Enable the DAG

1. Navigate to the DAGs page
2. Toggle the `corporate_data_ai_pipeline` DAG to ON
3. Trigger manually or wait for scheduled run

## AWS MWAA Deployment

### 1. Create S3 bucket for Airflow

```bash
aws s3 mb s3://your-airflow-bucket
aws s3 cp dags/ s3://your-airflow-bucket/dags/ --recursive
```

### 2. Create MWAA environment

```bash
# Use Terraform or AWS Console to create MWAA environment
# Point to s3://your-airflow-bucket/dags/
```

### 3. Set Airflow variables and connections in MWAA Console

## Monitoring

- **Airflow UI:** Monitor task status, logs, and execution history
- **EMR Console:** View cluster details and Spark job logs
- **S3:** Check output data in iceberg-warehouse/
- **MLflow:** View model training metrics and artifacts

## Customization

Edit `corporate_data_pipeline_dag.py` to:
- Change schedule interval
- Modify EMR cluster configuration
- Add additional validation steps
- Configure alerting (Slack, PagerDuty, etc.)

## Troubleshooting

**DAG not appearing:**
- Check DAG file syntax: `python -m py_compile dags/corporate_data_pipeline_dag.py`
- Verify file is in correct dags/ folder
- Check Airflow scheduler logs

**EMR cluster creation fails:**
- Verify IAM roles and permissions
- Check VPC and subnet configuration
- Ensure EMR service limits not exceeded

**Spark job fails:**
- Check EMR step logs in S3
- Verify S3 paths and permissions
- Check Spark configuration and dependencies
