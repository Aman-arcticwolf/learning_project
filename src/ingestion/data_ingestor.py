"""
Data ingestion module for reading corporate data from S3 sources.
"""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

logger = logging.getLogger(__name__)


class DataIngestor:
    """
    Handles data ingestion from multiple sources into Spark DataFrames.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the DataIngestor.
        
        Args:
            spark: SparkSession instance
            config: Pipeline configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.aws_config = config['aws']
        self.ingestion_config = config['ingestion']
        
    def get_source1_schema(self) -> StructType:
        """Define schema for Source 1 (Supply Chain Data)."""
        return StructType([
            StructField("corporate_name_s1", StringType(), False),
            StructField("address", StringType(), True),
            StructField("activity_places", StringType(), True),
            StructField("top_suppliers", StringType(), True)
        ])
    
    def get_source2_schema(self) -> StructType:
        """Define schema for Source 2 (Financial Data)."""
        return StructType([
            StructField("corporate_name_s2", StringType(), False),
            StructField("main_customers", StringType(), True),
            StructField("revenue", LongType(), True),
            StructField("profit", LongType(), True)
        ])
    
    def read_source1(self) -> DataFrame:
        """
        Read Supply Chain Data from S3 Source 1.
        
        Returns:
            DataFrame containing source 1 data
        """
        s3_path = self._get_s3_path('source1_prefix', 'supply_chain_data.csv')
        logger.info(f"Reading Source 1 data from: {s3_path}")
        
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(self.get_source1_schema()) \
            .csv(s3_path)
        
        # Add metadata columns
        df = df.withColumn("source", F.lit("source1")) \
               .withColumn("ingestion_timestamp", F.current_timestamp())
        
        logger.info(f"Source 1 record count: {df.count()}")
        return df
    
    def read_source2(self) -> DataFrame:
        """
        Read Financial Data from S3 Source 2.
        
        Returns:
            DataFrame containing source 2 data
        """
        s3_path = self._get_s3_path('source2_prefix', 'financial_data.csv')
        logger.info(f"Reading Source 2 data from: {s3_path}")
        
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(self.get_source2_schema()) \
            .csv(s3_path)
        
        # Add metadata columns
        df = df.withColumn("source", F.lit("source2")) \
               .withColumn("ingestion_timestamp", F.current_timestamp())
        
        logger.info(f"Source 2 record count: {df.count()}")
        return df
    
    def validate_data(self, df: DataFrame, source_name: str) -> bool:
        """
        Validate ingested data for completeness and quality.
        
        Args:
            df: DataFrame to validate
            source_name: Name of the data source
            
        Returns:
            True if validation passes, raises exception otherwise
        """
        logger.info(f"Validating data from {source_name}")
        
        # Check if DataFrame is empty
        if df.count() == 0:
            raise ValueError(f"No data found in {source_name}")
        
        # Check for null values in required fields
        if source_name == "source1":
            null_counts = df.select(
                F.sum(F.col("corporate_name_s1").isNull().cast("int")).alias("null_names")
            ).collect()[0]
            
            if null_counts.null_names > 0:
                logger.warning(f"{source_name}: Found {null_counts.null_names} null corporate names")
        
        elif source_name == "source2":
            null_counts = df.select(
                F.sum(F.col("corporate_name_s2").isNull().cast("int")).alias("null_names"),
                F.sum(F.col("revenue").isNull().cast("int")).alias("null_revenue"),
                F.sum(F.col("profit").isNull().cast("int")).alias("null_profit")
            ).collect()[0]
            
            if null_counts.null_names > 0:
                logger.warning(f"{source_name}: Found {null_counts.null_names} null corporate names")
            if null_counts.null_revenue > 0:
                logger.warning(f"{source_name}: Found {null_counts.null_revenue} null revenue values")
            if null_counts.null_profit > 0:
                logger.warning(f"{source_name}: Found {null_counts.null_profit} null profit values")
        
        logger.info(f"Validation passed for {source_name}")
        return True
    
    def ingest_all_sources(self) -> tuple[DataFrame, DataFrame]:
        """
        Ingest data from all sources and validate.
        
        Returns:
            Tuple of (source1_df, source2_df)
        """
        logger.info("Starting data ingestion from all sources")
        
        # Read source 1
        source1_df = self.read_source1()
        self.validate_data(source1_df, "source1")
        
        # Read source 2
        source2_df = self.read_source2()
        self.validate_data(source2_df, "source2")
        
        logger.info("Data ingestion completed successfully")
        return source1_df, source2_df
    
    def _get_s3_path(self, prefix_key: str, filename: str = None) -> str:
        """
        Construct S3 path from configuration.
        
        Args:
            prefix_key: Key in s3 config for the prefix
            filename: Optional filename
            
        Returns:
            Full S3 path
        """
        bucket = self.aws_config['s3']['bucket_name']
        prefix = self.aws_config['s3'][prefix_key]
        
        path = f"s3a://{bucket}/{prefix.rstrip('/')}"
        if filename:
            path = f"{path}/{filename}"
        return path
