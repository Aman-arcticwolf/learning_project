#!/bin/bash

# Deployment script for Data & AI Pipeline to AWS

set -e  # Exit on error

echo "=========================================="
echo "Data & AI Pipeline - AWS Deployment"
echo "=========================================="

# Check prerequisites
command -v aws >/dev/null 2>&1 || { echo "AWS CLI is required but not installed. Aborting." >&2; exit 1; }
command -v terraform >/dev/null 2>&1 || { echo "Terraform is required but not installed. Aborting." >&2; exit 1; }

# Load configuration
if [ -f .env ]; then
    export $(cat .env | grep -v '#' | awk '/=/ {print $1}')
fi

BUCKET_NAME=${S3_BUCKET_NAME:-"corporate-data-pipeline"}
AWS_REGION=${AWS_DEFAULT_REGION:-"us-east-1"}

echo "Configuration:"
echo "  S3 Bucket: $BUCKET_NAME"
echo "  AWS Region: $AWS_REGION"
echo ""

# Step 1: Deploy infrastructure with Terraform
echo "[1/5] Deploying AWS infrastructure with Terraform..."
cd terraform
terraform init -upgrade
terraform apply -auto-approve
cd ..
echo "✓ Infrastructure deployed"

# Step 2: Upload source data to S3
echo ""
echo "[2/5] Uploading source data to S3..."
aws s3 cp data/source1/ s3://$BUCKET_NAME/data/source1/ --recursive
aws s3 cp data/source2/ s3://$BUCKET_NAME/data/source2/ --recursive
echo "✓ Source data uploaded"

# Step 3: Package and upload pipeline code
echo ""
echo "[3/5] Packaging pipeline code..."
tar -czf pipeline-deployment.tar.gz src/ config/ requirements.txt setup.py
aws s3 cp pipeline-deployment.tar.gz s3://$BUCKET_NAME/deployments/pipeline-deployment-latest.tar.gz
rm pipeline-deployment.tar.gz
echo "✓ Pipeline code uploaded"

# Step 4: Create EMR cluster (optional, can also use existing cluster)
echo ""
echo "[4/5] EMR cluster setup..."
read -p "Do you want to create a new EMR cluster? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    EMR_INSTANCE_PROFILE=$(terraform -chdir=terraform output -raw emr_ec2_instance_profile)
    EMR_SERVICE_ROLE=$(terraform -chdir=terraform output -raw emr_service_role_arn | awk -F'/' '{print $NF}')
    SUBNET_ID=$(terraform -chdir=terraform output -json public_subnet_ids | jq -r '.[0]')
    
    echo "Creating EMR cluster..."
    CLUSTER_ID=$(aws emr create-cluster \
        --name "Data-AI-Pipeline-$(date +%Y%m%d-%H%M%S)" \
        --release-label emr-6.15.0 \
        --applications Name=Spark Name=Hadoop \
        --ec2-attributes InstanceProfile=$EMR_INSTANCE_PROFILE,SubnetId=$SUBNET_ID \
        --instance-type m5.xlarge \
        --instance-count 3 \
        --service-role $EMR_SERVICE_ROLE \
        --log-uri s3://$BUCKET_NAME-emr-logs/logs/ \
        --region $AWS_REGION \
        --query 'ClusterId' \
        --output text)
    
    echo "✓ EMR Cluster created: $CLUSTER_ID"
    echo "  Waiting for cluster to be ready..."
    aws emr wait cluster-running --cluster-id $CLUSTER_ID
    echo "  Cluster is ready!"
else
    echo "Skipping EMR cluster creation"
    read -p "Enter existing EMR Cluster ID (or press Enter to skip): " CLUSTER_ID
fi

# Step 5: Submit Spark job
echo ""
echo "[5/5] Submitting Spark job..."
if [ ! -z "$CLUSTER_ID" ]; then
    STEP_ID=$(aws emr add-steps \
        --cluster-id $CLUSTER_ID \
        --steps Type=Spark,Name="Data-AI-Pipeline",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,s3://$BUCKET_NAME/deployments/pipeline-deployment-latest.tar.gz] \
        --query 'StepIds[0]' \
        --output text)
    
    echo "✓ Spark job submitted: $STEP_ID"
    echo ""
    echo "Monitor job progress:"
    echo "  aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID"
    echo ""
    echo "View logs:"
    echo "  aws s3 ls s3://$BUCKET_NAME-emr-logs/logs/$CLUSTER_ID/"
else
    echo "No cluster ID provided, skipping job submission"
fi

echo ""
echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Monitor the EMR job in the AWS Console"
echo "2. Query results using Athena or Spark:"
echo "   SELECT * FROM corporate_db.corporate_registry LIMIT 10;"
echo "3. View ML model in MLflow"
echo ""
echo "Clean up:"
echo "  - Terminate EMR cluster when done"
echo "  - Run 'terraform destroy' to remove infrastructure"
echo ""
