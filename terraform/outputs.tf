# Output Values
output "s3_bucket_name" {
  description = "Name of the S3 bucket for data storage"
  value       = aws_s3_bucket.data_pipeline.id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.data_pipeline.arn
}

output "emr_logs_bucket" {
  description = "Name of the S3 bucket for EMR logs"
  value       = aws_s3_bucket.emr_logs.id
}

output "glue_database_name" {
  description = "Name of the Glue database"
  value       = aws_glue_catalog_database.corporate_db.name
}

output "emr_service_role_arn" {
  description = "ARN of the EMR service role"
  value       = aws_iam_role.emr_service_role.arn
}

output "emr_ec2_instance_profile" {
  description = "Name of the EMR EC2 instance profile"
  value       = aws_iam_instance_profile.emr_ec2_profile.name
}

output "vpc_id" {
  description = "ID of the VPC"
  value       = aws_vpc.main.id
}

output "public_subnet_ids" {
  description = "IDs of the public subnets"
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "IDs of the private subnets"
  value       = aws_subnet.private[*].id
}

output "emr_master_sg_id" {
  description = "ID of the EMR master security group"
  value       = aws_security_group.emr_master.id
}

output "emr_slave_sg_id" {
  description = "ID of the EMR slave security group"
  value       = aws_security_group.emr_slave.id
}

output "deployment_instructions" {
  description = "Instructions for deploying the pipeline"
  value = <<EOF
Terraform deployment completed successfully!

Next steps:
1. Upload your source data to S3:
   aws s3 cp data/source1/ s3://${aws_s3_bucket.data_pipeline.id}/data/source1/ --recursive
   aws s3 cp data/source2/ s3://${aws_s3_bucket.data_pipeline.id}/data/source2/ --recursive

2. Create an EMR cluster:
   aws emr create-cluster \
     --name "Data-AI-Pipeline" \
     --release-label ${var.emr_release_label} \
     --applications Name=Spark Name=Hadoop \
     --ec2-attributes InstanceProfile=${aws_iam_instance_profile.emr_ec2_profile.name},SubnetId=${aws_subnet.public[0].id} \
     --instance-type ${var.emr_instance_type} \
     --instance-count ${var.emr_instance_count} \
     --service-role ${aws_iam_role.emr_service_role.name} \
     --log-uri s3://${aws_s3_bucket.emr_logs.id}/logs/

3. Update your pipeline configuration:
   - S3 bucket: ${aws_s3_bucket.data_pipeline.id}
   - Glue database: ${aws_glue_catalog_database.corporate_db.name}
   - Region: ${var.aws_region}

4. Deploy your pipeline code to S3 and submit a Spark job to EMR
EOF
}
