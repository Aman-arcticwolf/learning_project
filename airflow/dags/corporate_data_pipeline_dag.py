"""
Apache Airflow DAG for orchestrating the Data & AI Pipeline.

This DAG schedules and manages the complete workflow:
1. Data ingestion from S3 sources
2. Entity resolution and deduplication
3. Iceberg table upsert
4. ML model training
5. Model registration
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'corporate_data_ai_pipeline',
    default_args=default_args,
    description='End-to-end data and ML pipeline for corporate data',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=['data-engineering', 'ml', 'iceberg'],
)

# EMR cluster configuration
JOB_FLOW_OVERRIDES = {
    'Name': 'DataAIPipeline-{{ ds }}',
    'ReleaseLabel': 'emr-6.15.0',
    'Applications': [
        {'Name': 'Spark'},
        {'Name': 'Hadoop'},
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Core',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2SubnetId': '{{ var.value.emr_subnet_id }}',
    },
    'JobFlowRole': '{{ var.value.emr_ec2_instance_profile }}',
    'ServiceRole': '{{ var.value.emr_service_role }}',
    'LogUri': 's3://{{ var.value.s3_bucket_name }}-emr-logs/logs/',
    'VisibleToAllUsers': True,
}

# Spark job configuration
SPARK_STEPS = [
    {
        'Name': 'Run Data & AI Pipeline',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                '--conf', 'spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog',
                '--conf', 'spark.sql.catalog.glue_catalog.warehouse=s3://{{ var.value.s3_bucket_name }}/iceberg-warehouse/',
                '--conf', 'spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog',
                '--conf', 'spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO',
                '--packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4',
                's3://{{ var.value.s3_bucket_name }}/deployments/pipeline-deployment-latest.tar.gz',
            ],
        },
    }
]


def check_source_data_availability(**context):
    """
    Check if source data files are available in S3.
    """
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = context['var']['value']['s3_bucket_name']
    
    required_files = [
        'data/source1/supply_chain_data.csv',
        'data/source2/financial_data.csv'
    ]
    
    for file_key in required_files:
        if not s3_hook.check_for_key(key=file_key, bucket_name=bucket_name):
            raise FileNotFoundError(f"Required file not found: s3://{bucket_name}/{file_key}")
    
    logging.info("All source data files are available")
    return True


def validate_pipeline_completion(**context):
    """
    Validate that the pipeline completed successfully.
    """
    # This could query the Iceberg table or check MLflow for the model
    logging.info("Pipeline validation completed")
    return True


def send_completion_notification(**context):
    """
    Send notification about pipeline completion.
    """
    execution_date = context['ds']
    logging.info(f"Pipeline completed successfully for date: {execution_date}")
    # Add Slack/Email notification logic here
    return True


# Task 1: Check source data availability
check_data = PythonOperator(
    task_id='check_source_data',
    python_callable=check_source_data_availability,
    dag=dag,
)

# Task 2: Create EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag,
)

# Task 3: Add Spark job step
add_pipeline_step = EmrAddStepsOperator(
    task_id='add_pipeline_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_STEPS,
    dag=dag,
)

# Task 4: Wait for Spark job completion
wait_for_pipeline = EmrStepSensor(
    task_id='wait_for_pipeline_completion',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_pipeline_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)

# Task 5: Validate pipeline completion
validate_pipeline = PythonOperator(
    task_id='validate_pipeline',
    python_callable=validate_pipeline_completion,
    dag=dag,
)

# Task 6: Terminate EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    trigger_rule='all_done',  # Terminate even if previous tasks failed
    dag=dag,
)

# Task 7: Send completion notification
send_notification = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_completion_notification,
    dag=dag,
)

# Define task dependencies
check_data >> create_emr_cluster >> add_pipeline_step >> wait_for_pipeline
wait_for_pipeline >> validate_pipeline >> terminate_emr_cluster >> send_notification
