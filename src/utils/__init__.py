"""
Utility functions for configuration management and logging.
"""
import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv


def load_config(config_path: str = "config/pipeline_config.yaml") -> Dict[str, Any]:
    """
    Load pipeline configuration from YAML file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Dictionary containing configuration
    """
    # Load environment variables
    load_dotenv()
    
    # Get project root
    project_root = Path(__file__).parent.parent.parent
    full_path = project_root / config_path
    
    with open(full_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Override with environment variables if present
    if os.getenv('AWS_DEFAULT_REGION'):
        config['aws']['region'] = os.getenv('AWS_DEFAULT_REGION')
    if os.getenv('S3_BUCKET_NAME'):
        config['aws']['s3']['bucket_name'] = os.getenv('S3_BUCKET_NAME')
    
    return config


def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """
    Configure logging for the pipeline.
    
    Args:
        config: Pipeline configuration dictionary
        
    Returns:
        Configured logger instance
    """
    log_config = config.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO'))
    log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create logs directory if it doesn't exist
    log_path = log_config.get('log_path', 'logs/pipeline.log')
    log_dir = Path(log_path).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)


def get_spark_session(app_name: str = "DataAIPipeline", config: Dict[str, Any] = None):
    """
    Create and configure a Spark session with Iceberg and AWS support.
    
    Args:
        app_name: Name of the Spark application
        config: Pipeline configuration dictionary
        
    Returns:
        Configured SparkSession
    """
    from pyspark.sql import SparkSession
    
    if config is None:
        config = load_config()
    
    aws_config = config['aws']
    iceberg_config = config['iceberg']
    
    # Build Spark session with Iceberg and AWS configurations
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", aws_config['s3']['iceberg_warehouse']) \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.glue.region", aws_config['region']) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    
    # Add additional Iceberg properties
    for key, value in iceberg_config.get('properties', {}).items():
        builder = builder.config(f"spark.sql.catalog.glue_catalog.{key}", value)
    
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def get_s3_path(bucket: str, prefix: str, filename: str = None) -> str:
    """
    Construct S3 path from components.
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix/folder
        filename: Optional filename
        
    Returns:
        Full S3 path
    """
    path = f"s3a://{bucket}/{prefix.rstrip('/')}"
    if filename:
        path = f"{path}/{filename}"
    return path
