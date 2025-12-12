# Data & AI Engineering Pipeline 🚀

A scalable, production-ready data pipeline demonstrating enterprise-grade data engineering practices with Apache Iceberg, PySpark ML, and automated CI/CD deployment on AWS.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup](#detailed-setup)
- [Usage](#usage)
- [Testing](#testing)
- [CI/CD Pipeline](#cicd-pipeline)
- [AWS Deployment](#aws-deployment)
- [Orchestration with Airflow](#orchestration-with-airflow)
- [Project Structure](#project-structure)
- [Entity Resolution Heuristic](#entity-resolution-heuristic)
- [Querying Results](#querying-results)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## 🎯 Overview

This project implements a complete data and AI engineering pipeline that:

1. **Ingests** corporate data from multiple sources (supply chain & financial data)
2. **Performs entity resolution** to identify and deduplicate corporation records using fuzzy matching
3. **Harmonizes** data into a unified format
4. **Stores** results in an Apache Iceberg table with ACID transactional guarantees
5. **Trains** a machine learning model to predict high-profit corporations
6. **Registers** the trained model in MLflow for tracking and deployment
7. **Automates** the entire workflow with CI/CD and orchestration

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Data Sources (S3)                          │
│  ┌─────────────────────┐       ┌─────────────────────────────────┐ │
│  │  Source 1           │       │  Source 2                       │ │
│  │  Supply Chain Data  │       │  Financial Data                 │ │
│  │  - corporate_name   │       │  - corporate_name               │ │
│  │  - address          │       │  - main_customers               │ │
│  │  - top_suppliers    │       │  - revenue, profit              │ │
│  └─────────────────────┘       └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    PySpark ETL Processing (EMR)                     │
│                                                                     │
│  ┌──────────────────┐   ┌──────────────────┐   ┌───────────────┐  │
│  │  Data Ingestion  │───│Entity Resolution │───│ Harmonization │  │
│  │  - Read CSV      │   │- Fuzzy Matching  │   │ - Merge Data  │  │
│  │  - Validation    │   │- Blocking        │   │ - Assign IDs  │  │
│  └──────────────────┘   └──────────────────┘   └───────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Apache Iceberg Table (AWS Glue Catalog)                │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  corporate_registry                                          │  │
│  │  - MERGE INTO (Upsert) operation                            │  │
│  │  - ACID transactions                                         │  │
│  │  - Time travel & Schema evolution                           │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                ML Pipeline (PySpark ML + MLflow)                    │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐   │
│  │   Feature    │───│    Model     │───│   Model Registry     │   │
│  │ Engineering  │   │   Training   │   │    (MLflow)          │   │
│  │- VectorAssem │   │- Logistic    │   │- Versioning          │   │
│  │- Scaling     │   │  Regression  │   │- Metrics Tracking    │   │
│  └──────────────┘   └──────────────┘   └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    CI/CD & Orchestration                            │
│                                                                     │
│  ┌─────────────────┐        ┌─────────────────────────────────┐   │
│  │  GitHub Actions │        │  Airflow / AWS MWAA             │   │
│  │  - Testing      │        │  - Scheduled Jobs               │   │
│  │  - Linting      │        │  - Workflow Management          │   │
│  │  - Deployment   │        │  - Monitoring                   │   │
│  └─────────────────┘        └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## ✨ Features

### Core Features
- ✅ **Multi-source data ingestion** from S3 with schema validation
- ✅ **Entity resolution** using fuzzy string matching (Levenshtein, Jaro-Winkler)
- ✅ **Apache Iceberg integration** with Glue Catalog for ACID transactions
- ✅ **PySpark ML pipeline** for profit prediction (Logistic Regression)
- ✅ **MLflow integration** for experiment tracking and model registry
- ✅ **Comprehensive unit tests** with pytest (90%+ coverage)
- ✅ **Data contract validation** to ensure schema compatibility

### CI/CD & Infrastructure
- ✅ **GitHub Actions workflow** for automated testing and deployment
- ✅ **Terraform IaC** for AWS infrastructure (S3, EMR, Glue, VPC, IAM)
- ✅ **Airflow DAG** for workflow orchestration (bonus)
- ✅ **Docker support** for local development and testing

### AWS Services Used
- **S3** - Data storage and Iceberg warehouse
- **EMR** - Spark cluster for distributed processing
- **Glue Data Catalog** - Iceberg metastore
- **IAM** - Security and access management
- **VPC** - Network isolation
- **CloudWatch** - Logging and monitoring
- **MWAA** - Managed Apache Airflow (optional)

## 📦 Prerequisites

### Local Development
- Python 3.9+
- Java 11+ (for Spark)
- Apache Spark 3.3+
- Docker & Docker Compose (optional, for local testing)

### AWS Deployment
- AWS Account with appropriate permissions
- AWS CLI configured
- Terraform 1.0+ (for infrastructure deployment)

### Install Dependencies

```bash
# Clone the repository
git clone https://github.com/yourusername/data-ai-pipeline.git
cd data-ai-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

## 🚀 Quick Start

### 1. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your AWS credentials
nano .env
```

### 2. Update Configuration

Edit `config/pipeline_config.yaml` with your settings:

```yaml
aws:
  region: us-east-1
  s3:
    bucket_name: your-unique-bucket-name
```

### 3. Run Locally (Without AWS)

For local testing, you can modify the pipeline to use local file paths:

```bash
# Run the pipeline
python src/main.py

# Skip ML training
python src/main.py --skip-ml

# Use existing data
python src/main.py --skip-ingestion
```

### 4. Run Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration
```

## 🔧 Detailed Setup

### Step 1: AWS Infrastructure Deployment

```bash
cd terraform

# Copy variables template
cp terraform.tfvars.example terraform.tfvars

# Edit with your values
nano terraform.tfvars

# Initialize Terraform
terraform init

# Review planned changes
terraform plan

# Apply infrastructure
terraform apply

# Note the outputs for later use
terraform output
```

### Step 2: Upload Data to S3

```bash
# Upload source data
aws s3 cp data/source1/ s3://your-bucket-name/data/source1/ --recursive
aws s3 cp data/source2/ s3://your-bucket-name/data/source2/ --recursive

# Verify uploads
aws s3 ls s3://your-bucket-name/data/ --recursive
```

### Step 3: Create EMR Cluster

```bash
# Use Terraform output values
aws emr create-cluster \
  --name "Data-AI-Pipeline-Cluster" \
  --release-label emr-6.15.0 \
  --applications Name=Spark Name=Hadoop \
  --ec2-attributes InstanceProfile=<EMR_EC2_INSTANCE_PROFILE>,SubnetId=<SUBNET_ID> \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --service-role <EMR_SERVICE_ROLE> \
  --log-uri s3://your-bucket-name-emr-logs/logs/ \
  --use-default-roles

# Note the cluster ID
export CLUSTER_ID=j-XXXXXXXXXXXXX
```

### Step 4: Package and Deploy Code

```bash
# Create deployment package
tar -czf pipeline-deployment.tar.gz src/ config/ requirements.txt setup.py

# Upload to S3
aws s3 cp pipeline-deployment.tar.gz s3://your-bucket-name/deployments/

# Submit Spark job
aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps Type=Spark,Name="Data-AI-Pipeline",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://your-bucket-name/deployments/pipeline-deployment.tar.gz]
```

## 📊 Usage

### Running the Full Pipeline

```bash
python src/main.py
```

### Running Individual Components

```python
from src.utils import get_spark_session, load_config
from src.ingestion import DataIngestor
from src.deduplication import EntityResolver
from src.iceberg import IcebergTableManager
from src.ml import MLModelTrainer

# Initialize
config = load_config()
spark = get_spark_session("MyPipeline", config)

# Data Ingestion
ingestor = DataIngestor(spark, config)
source1_df, source2_df = ingestor.ingest_all_sources()

# Entity Resolution
resolver = EntityResolver(spark, config)
harmonized_df = resolver.resolve_entities(source1_df, source2_df)

# Iceberg Operations
iceberg = IcebergTableManager(spark, config)
iceberg.create_database()
iceberg.create_table()
iceberg.upsert_data(harmonized_df)

# ML Training
trainer = MLModelTrainer(spark, config)
model, metrics = trainer.train_and_evaluate(harmonized_df)

print(f"Model AUC: {metrics['auc']:.4f}")
```

## 🧪 Testing

### Test Structure

```
tests/
├── test_entity_resolution.py   # Entity matching tests
├── test_data_ingestion.py      # Ingestion validation tests
├── test_data_contracts.py      # Schema validation tests
├── test_iceberg.py             # Iceberg table tests
├── test_ml_training.py         # ML pipeline tests
└── conftest.py                 # Pytest fixtures
```

### Running Tests

```bash
# All tests
pytest

# Specific test file
pytest tests/test_entity_resolution.py

# Specific test
pytest tests/test_entity_resolution.py::TestEntityResolver::test_normalize_name_basic

# With verbose output
pytest -v

# With coverage report
pytest --cov=src --cov-report=term-missing

# Skip slow tests
pytest -m "not slow"
```

## 🔄 CI/CD Pipeline

The GitHub Actions workflow automatically:

1. **Lint & Test** - Runs on every push/PR
   - Black code formatting check
   - Flake8 linting
   - Pylint analysis
   - Pytest unit tests with coverage

2. **Data Contract Validation** - Ensures schema compatibility

3. **Build & Package** - Creates deployment artifacts

4. **Deploy to AWS** - Uploads to S3 (on main branch)

### Required GitHub Secrets

Configure in your repository settings:

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
S3_BUCKET_NAME
EMR_CLUSTER_ID (optional)
```

### Workflow File

See `.github/workflows/ci-cd.yml` for full configuration.

## ☁️ AWS Deployment

### Infrastructure Components

The Terraform configuration deploys:

- **VPC** with public/private subnets
- **S3 buckets** for data and logs
- **IAM roles** for EMR and Glue
- **Security groups** for EMR cluster
- **Glue Database** for Iceberg catalog

### Deployment Steps

```bash
# 1. Deploy infrastructure
cd terraform
terraform apply

# 2. Upload application code
aws s3 cp pipeline-deployment.tar.gz s3://your-bucket/deployments/

# 3. Create EMR cluster (or use existing)
# See Step 3 in Detailed Setup

# 4. Monitor execution
aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID
```

### Cost Optimization

- Use spot instances for EMR core nodes
- Terminate EMR cluster when not in use
- Enable S3 lifecycle policies for old data
- Use appropriate EMR instance sizes

## 🔄 Orchestration with Airflow

### Local Airflow Setup

```bash
cd airflow

# Start Airflow with Docker
docker-compose up -d

# Access UI at http://localhost:8080
# Username: admin, Password: admin
```

### AWS MWAA Setup

```bash
# 1. Create S3 bucket for Airflow
aws s3 mb s3://your-airflow-bucket

# 2. Upload DAG
aws s3 cp airflow/dags/ s3://your-airflow-bucket/dags/ --recursive

# 3. Create MWAA environment (via Console or Terraform)
```

See `airflow/README.md` for detailed instructions.

## 📁 Project Structure

```
data-ai-pipeline/
├── .github/workflows/       # GitHub Actions CI/CD
│   └── ci-cd.yml
├── airflow/                 # Airflow orchestration
│   ├── dags/
│   │   └── corporate_data_pipeline_dag.py
│   └── README.md
├── config/                  # Configuration files
│   └── pipeline_config.yaml
├── data/                    # Sample data
│   ├── source1/
│   │   └── supply_chain_data.csv
│   └── source2/
│       └── financial_data.csv
├── src/                     # Source code
│   ├── deduplication/       # Entity resolution module
│   │   ├── __init__.py
│   │   └── entity_resolver.py
│   ├── iceberg/             # Iceberg table management
│   │   ├── __init__.py
│   │   └── table_manager.py
│   ├── ingestion/           # Data ingestion module
│   │   ├── __init__.py
│   │   └── data_ingestor.py
│   ├── ml/                  # ML training module
│   │   ├── __init__.py
│   │   └── model_trainer.py
│   ├── utils/               # Utility functions
│   │   └── __init__.py
│   └── main.py              # Main pipeline script
├── terraform/               # Infrastructure as Code
│   ├── main.tf
│   ├── variables.tf
│   ├── s3.tf
│   ├── iam.tf
│   ├── vpc.tf
│   ├── glue.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── tests/                   # Unit and integration tests
│   ├── conftest.py
│   ├── test_entity_resolution.py
│   ├── test_data_ingestion.py
│   ├── test_data_contracts.py
│   ├── test_iceberg.py
│   └── test_ml_training.py
├── .env.example             # Environment variables template
├── .gitignore
├── pytest.ini               # Pytest configuration
├── requirements.txt         # Python dependencies
├── setup.py                 # Package setup
└── README.md                # This file
```

## 🧩 Entity Resolution Heuristic

The pipeline uses a multi-step approach to identify duplicate corporations:

### 1. Name Normalization
```python
# Example transformations
"Acme Corporation" → "acme"
"ACME Corp." → "acme"  
"Acme Inc" → "acme"
```

- Converts to lowercase
- Removes corporate suffixes (Inc., Corp., Ltd., LLC, etc.)
- Strips special characters
- Normalizes whitespace

### 2. Blocking
To reduce comparison space, records are grouped using blocking keys:
- First 3 characters of normalized name
- City extracted from address

### 3. Similarity Scoring
For candidate pairs, we compute a weighted similarity score using:

- **Ratio** (30%): Character-level similarity
- **Token Sort Ratio** (40%): Word order-independent similarity
- **Token Set Ratio** (30%): Set-based token matching

```python
similarity_score = 0.3 * ratio + 0.4 * token_sort + 0.3 * token_set
```

### 4. Matching Threshold
Records with similarity ≥ 0.85 are considered matches.

### 5. Corporate ID Assignment
Matched entities receive the same `corporate_id`, creating a unified record.

## 🔍 Querying Results

### Query Iceberg Table

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QueryIceberg") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://your-bucket/iceberg-warehouse/") \
    .getOrCreate()

# Read current snapshot
df = spark.table("glue_catalog.corporate_db.corporate_registry")
df.show()

# Query specific data
high_profit_companies = spark.sql("""
    SELECT corporate_id, corporate_name, revenue, profit
    FROM glue_catalog.corporate_db.corporate_registry
    WHERE profit > 1000000
    ORDER BY profit DESC
""")
high_profit_companies.show()

# Time travel query
snapshot_df = spark.sql("""
    SELECT * 
    FROM glue_catalog.corporate_db.corporate_registry
    VERSION AS OF 12345678
""")
```

### Query with AWS Athena

```sql
-- Query Iceberg table via Athena
SELECT 
    corporate_id,
    corporate_name,
    revenue,
    profit,
    CASE WHEN profit > 1000000 THEN 'High' ELSE 'Low' END as profit_category
FROM corporate_db.corporate_registry
WHERE revenue IS NOT NULL
ORDER BY revenue DESC
LIMIT 10;
```

### View Registered Model

```bash
# Start MLflow UI
mlflow ui --backend-store-uri file:///path/to/mlruns

# Access at http://localhost:5000
```

Or query programmatically:

```python
import mlflow

mlflow.set_tracking_uri("s3://your-bucket/mlflow-artifacts/")

# Get latest model
client = mlflow.tracking.MlflowClient()
model_versions = client.search_model_versions("name='corporate_profit_predictor'")
latest_version = model_versions[0]

print(f"Model Version: {latest_version.version}")
print(f"Run ID: {latest_version.run_id}")

# Load and use model
model = mlflow.spark.load_model(f"models:/corporate_profit_predictor/{latest_version.version}")
predictions = model.transform(test_data)
```

## 🛠️ Troubleshooting

### Common Issues

**Issue: PySpark import errors**
```bash
# Ensure SPARK_HOME is set
export SPARK_HOME=/path/to/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-VERSION-src.zip:$PYTHONPATH
```

**Issue: AWS credentials not found**
```bash
# Configure AWS CLI
aws configure

# Or set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1
```

**Issue: S3 permission denied**
- Verify IAM roles have S3 read/write permissions
- Check bucket policies
- Ensure bucket exists and name is correct

**Issue: EMR step fails**
- Check EMR step logs in S3
- Verify Spark configuration
- Ensure dependencies are included in deployment package

**Issue: Iceberg table not found**
- Verify Glue database exists
- Check catalog configuration
- Ensure table was created successfully

### Debugging Tips

```bash
# Check Spark logs
cat logs/pipeline.log

# Test S3 connectivity
aws s3 ls s3://your-bucket-name/

# Validate Terraform
terraform validate
terraform plan

# Check EMR cluster status
aws emr describe-cluster --cluster-id j-XXXXXXXXXXXXX

# View EMR step logs
aws emr describe-step --cluster-id j-XXXXXXXXXXXXX --step-id s-XXXXXXXXXXXXX
```

## 📈 Performance Optimization

### Spark Tuning

```python
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \  # 128 MB
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

### Iceberg Optimization

```sql
-- Compact small files
CALL glue_catalog.system.rewrite_data_files('corporate_db.corporate_registry');

-- Expire old snapshots
CALL glue_catalog.system.expire_snapshots('corporate_db.corporate_registry', TIMESTAMP '2025-01-01 00:00:00');
```

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`pytest`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

### Code Quality

- Follow PEP 8 style guidelines
- Add unit tests for new features
- Update documentation as needed
- Ensure all tests pass before submitting PR

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.


## 🙏 Acknowledgments

- Apache Iceberg community
- PySpark documentation
- AWS EMR team
- MLflow contributors

---

**Built with ❤️ for enterprise data engineering**
