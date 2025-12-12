# AWS Glue Database
resource "aws_glue_catalog_database" "corporate_db" {
  name        = "corporate_db"
  description = "Database for corporate data pipeline"
  
  location_uri = "s3://${aws_s3_bucket.data_pipeline.id}/iceberg-warehouse/"
}

# Glue Data Catalog is used as Iceberg catalog
# Tables will be created by the pipeline code
