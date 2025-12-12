# Quick Start Guide - Data & AI Pipeline

This guide will help you get the pipeline running in 10 minutes.

## Option 1: Local Testing (No AWS Required)

### Step 1: Install Dependencies (2 minutes)

```bash
# Clone repository
git clone <your-repo-url>
cd data-ai-pipeline

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configure for Local Mode (1 minute)

```bash
# Copy environment file
cp .env.example .env

# Edit config to use local paths
nano config/pipeline_config.yaml
```

Change S3 paths to local paths:
```yaml
aws:
  s3:
    bucket_name: "local"
    source1_prefix: "data/source1/"
    source2_prefix: "data/source2/"
```

### Step 3: Run Pipeline (2 minutes)

```bash
# Run with local Spark
python src/main.py
```

### Step 4: View Results (1 minute)

Results will be in `spark-warehouse/` directory.

---

## Option 2: AWS Deployment (Recommended for Demo)

### Prerequisites

- AWS Account
- AWS CLI configured
- Terraform installed

### Step 1: Deploy Infrastructure (3 minutes)

```bash
cd terraform

# Create config file
cp terraform.tfvars.example terraform.tfvars

# Edit with your S3 bucket name (must be globally unique)
nano terraform.tfvars

# Deploy
terraform init
terraform apply -auto-approve
```

### Step 2: Upload Data (1 minute)

```bash
# Get bucket name from Terraform output
BUCKET=$(terraform output -raw s3_bucket_name)

# Upload sample data
aws s3 cp ../data/source1/ s3://$BUCKET/data/source1/ --recursive
aws s3 cp ../data/source2/ s3://$BUCKET/data/source2/ --recursive
```

### Step 3: Run Automated Deployment (5 minutes)

```bash
cd ..

# Make deployment script executable
chmod +x deploy.sh

# Run deployment (creates EMR cluster and runs pipeline)
./deploy.sh
```

### Step 4: Monitor Execution

```bash
# Check cluster status
aws emr list-clusters --active

# View step status
aws emr describe-step --cluster-id <CLUSTER_ID> --step-id <STEP_ID>

# Check logs in S3
aws s3 ls s3://$BUCKET-emr-logs/logs/
```

### Step 5: Query Results

**Option A: Using Athena**

1. Go to AWS Athena Console
2. Select database: `corporate_db`
3. Run query:
```sql
SELECT * FROM corporate_registry LIMIT 10;
```

**Option B: Using Spark**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .getOrCreate()

df = spark.table("glue_catalog.corporate_db.corporate_registry")
df.show()
```

---

## Option 3: Docker Local Testing

### Run with Docker Compose (5 minutes)

```bash
# Start Spark and MLflow
docker-compose up -d

# Check services
docker-compose ps

# Access UIs
# Spark Master: http://localhost:8080
# MLflow: http://localhost:5000

# Run pipeline in Spark container
docker exec -it spark-master spark-submit /app/src/main.py
```

---

## Verify Installation

### Run Tests

```bash
# Run all tests
pytest

# Run quick unit tests only
pytest -m unit -v
```

### Check Code Quality

```bash
# Format code
black src/ tests/

# Run linter
flake8 src/ tests/
```

---

## Next Steps

1. **Customize the Pipeline**
   - Edit `config/pipeline_config.yaml`
   - Modify entity resolution threshold
   - Add new features to ML model

2. **Set up CI/CD**
   - Add GitHub secrets (AWS credentials)
   - Push to trigger GitHub Actions workflow

3. **Enable Orchestration**
   - Deploy Airflow DAG
   - Schedule daily runs

4. **Monitor & Optimize**
   - View MLflow experiments
   - Query Iceberg table history
   - Optimize Spark configuration

---

## Troubleshooting

### "Module not found" errors

```bash
pip install -e .
```

### AWS permission errors

```bash
aws configure
# Enter your AWS credentials
```

### Spark errors

```bash
# Ensure Java 11 is installed
java -version

# Set SPARK_HOME
export SPARK_HOME=/path/to/spark
```

### Port already in use

```bash
# Change port in config or kill process
lsof -ti:8080 | xargs kill -9
```

---

## Need Help?

- 📖 See full [README.md](README.md) for detailed documentation
- 🐛 Report issues on GitHub
- 💬 Check [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines

---

**Happy Data Engineering! 🚀**
