# S3 Bucket for Data Storage
resource "aws_s3_bucket" "data_pipeline" {
  bucket = var.s3_bucket_name
  
  tags = merge(
    var.tags,
    {
      Name = "${var.project_name}-bucket"
    }
  )
}

resource "aws_s3_bucket_versioning" "data_pipeline" {
  bucket = aws_s3_bucket.data_pipeline.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_pipeline" {
  bucket = aws_s3_bucket.data_pipeline.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# S3 Bucket for EMR Logs
resource "aws_s3_bucket" "emr_logs" {
  bucket = "${var.s3_bucket_name}-emr-logs"
  
  tags = merge(
    var.tags,
    {
      Name = "${var.project_name}-emr-logs"
    }
  )
}

# Create folder structure in S3
resource "aws_s3_object" "source1_folder" {
  bucket = aws_s3_bucket.data_pipeline.id
  key    = "data/source1/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "source2_folder" {
  bucket = aws_s3_bucket.data_pipeline.id
  key    = "data/source2/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "iceberg_warehouse_folder" {
  bucket = aws_s3_bucket.data_pipeline.id
  key    = "iceberg-warehouse/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "mlflow_artifacts_folder" {
  bucket = aws_s3_bucket.data_pipeline.id
  key    = "mlflow-artifacts/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "deployments_folder" {
  bucket = aws_s3_bucket.data_pipeline.id
  key    = "deployments/"
  content_type = "application/x-directory"
}
