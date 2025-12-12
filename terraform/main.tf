# AWS Provider Configuration
terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  backend "s3" {
    # Configure backend for state management
    # bucket = "your-terraform-state-bucket"
    # key    = "data-ai-pipeline/terraform.tfstate"
    # region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "DataAIPipeline"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}
