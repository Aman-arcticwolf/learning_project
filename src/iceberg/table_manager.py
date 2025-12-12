"""
Apache Iceberg table management module for transactional data operations.
"""
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class IcebergTableManager:
    """
    Manages Apache Iceberg table operations including creation, upsert, and queries.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the IcebergTableManager.
        
        Args:
            spark: SparkSession instance
            config: Pipeline configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.glue_config = config['aws']['glue']
        self.iceberg_config = config['iceberg']
        
        self.catalog_name = self.glue_config['catalog_name']
        self.database_name = self.glue_config['database_name']
        self.table_name = self.glue_config['table_name']
        self.full_table_name = f"{self.catalog_name}.{self.database_name}.{self.table_name}"
        
    def create_database(self):
        """Create Glue database if it doesn't exist."""
        logger.info(f"Creating database: {self.database_name}")
        
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.database_name}")
            logger.info(f"Database {self.database_name} created/verified successfully")
        except Exception as e:
            logger.error(f"Error creating database: {str(e)}")
            raise
    
    def create_table(self, sample_df: DataFrame = None):
        """
        Create Iceberg table if it doesn't exist.
        
        Args:
            sample_df: Optional sample DataFrame to infer schema
        """
        logger.info(f"Creating Iceberg table: {self.full_table_name}")
        
        try:
            # Check if table already exists
            tables = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{self.database_name}").collect()
            table_exists = any(row.tableName == self.table_name for row in tables)
            
            if table_exists:
                logger.info(f"Table {self.full_table_name} already exists")
                return
            
            # Create table with schema
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.full_table_name} (
                corporate_id STRING NOT NULL,
                corporate_name STRING,
                normalized_name STRING,
                address STRING,
                activity_places STRING,
                top_suppliers STRING,
                supplier_count INT,
                main_customers STRING,
                customer_count INT,
                revenue BIGINT,
                profit BIGINT,
                similarity_score DOUBLE,
                harmonization_timestamp TIMESTAMP,
                last_updated TIMESTAMP
            )
            USING iceberg
            """
            
            # Add partitioning if specified
            partition_by = self.iceberg_config.get('partition_by', [])
            if partition_by:
                # Add a date partition column
                create_table_sql += f"\nPARTITIONED BY (days(harmonization_timestamp))"
            
            # Add table properties
            properties = self.iceberg_config.get('properties', {})
            if properties:
                props_str = ", ".join([f"'{k}'='{v}'" for k, v in properties.items()])
                create_table_sql += f"\nTBLOCKPROPERTIES ({props_str})"
            
            self.spark.sql(create_table_sql)
            logger.info(f"Table {self.full_table_name} created successfully")
            
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            raise
    
    def table_exists(self) -> bool:
        """
        Check if the Iceberg table exists.
        
        Returns:
            True if table exists, False otherwise
        """
        try:
            tables = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{self.database_name}").collect()
            return any(row.tableName == self.table_name for row in tables)
        except Exception:
            return False
    
    def upsert_data(self, df: DataFrame):
        """
        Perform MERGE INTO operation (upsert) on the Iceberg table.
        
        Args:
            df: DataFrame containing new/updated records
        """
        logger.info(f"Starting upsert operation on {self.full_table_name}")
        
        try:
            # Add last_updated timestamp
            df_with_timestamp = df.withColumn("last_updated", F.current_timestamp())
            
            # Create or replace temp view for merge operation
            temp_view_name = "harmonized_updates"
            df_with_timestamp.createOrReplaceTempView(temp_view_name)
            
            # Get record counts before merge
            if self.table_exists():
                before_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.full_table_name}").collect()[0].count
                logger.info(f"Records in table before merge: {before_count}")
            else:
                before_count = 0
                logger.info("Table is new, performing initial load")
            
            update_count = df_with_timestamp.count()
            logger.info(f"Records to upsert: {update_count}")
            
            # Perform MERGE INTO operation
            merge_sql = f"""
            MERGE INTO {self.full_table_name} target
            USING {temp_view_name} source
            ON target.corporate_id = source.corporate_id
            WHEN MATCHED THEN UPDATE SET
                target.corporate_name = source.corporate_name,
                target.normalized_name = source.normalized_name,
                target.address = source.address,
                target.activity_places = source.activity_places,
                target.top_suppliers = source.top_suppliers,
                target.supplier_count = source.supplier_count,
                target.main_customers = source.main_customers,
                target.customer_count = source.customer_count,
                target.revenue = source.revenue,
                target.profit = source.profit,
                target.similarity_score = source.similarity_score,
                target.harmonization_timestamp = source.harmonization_timestamp,
                target.last_updated = source.last_updated
            WHEN NOT MATCHED THEN INSERT *
            """
            
            self.spark.sql(merge_sql)
            
            # Get record counts after merge
            after_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.full_table_name}").collect()[0].count
            logger.info(f"Records in table after merge: {after_count}")
            logger.info(f"Net new records: {after_count - before_count}")
            
            logger.info("Upsert operation completed successfully")
            
        except Exception as e:
            logger.error(f"Error during upsert operation: {str(e)}")
            raise
    
    def read_table(self) -> DataFrame:
        """
        Read the current snapshot of the Iceberg table.
        
        Returns:
            DataFrame containing all records from the table
        """
        logger.info(f"Reading table: {self.full_table_name}")
        
        try:
            df = self.spark.table(self.full_table_name)
            record_count = df.count()
            logger.info(f"Read {record_count} records from {self.full_table_name}")
            return df
        except Exception as e:
            logger.error(f"Error reading table: {str(e)}")
            raise
    
    def get_table_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the Iceberg table.
        
        Returns:
            Dictionary containing table metadata
        """
        logger.info(f"Fetching metadata for {self.full_table_name}")
        
        try:
            # Get table description
            describe_df = self.spark.sql(f"DESCRIBE EXTENDED {self.full_table_name}")
            
            # Get table history (Iceberg feature)
            history_df = self.spark.sql(f"SELECT * FROM {self.full_table_name}.history")
            
            # Get snapshots (Iceberg feature)
            snapshots_df = self.spark.sql(f"SELECT * FROM {self.full_table_name}.snapshots")
            
            metadata = {
                "table_name": self.full_table_name,
                "schema": describe_df.collect(),
                "history_count": history_df.count(),
                "snapshot_count": snapshots_df.count()
            }
            
            logger.info(f"Table metadata retrieved successfully")
            return metadata
            
        except Exception as e:
            logger.warning(f"Could not fetch complete metadata: {str(e)}")
            return {"table_name": self.full_table_name, "error": str(e)}
    
    def time_travel_query(self, snapshot_id: str = None, timestamp: str = None) -> DataFrame:
        """
        Query table at a specific point in time (Iceberg time travel feature).
        
        Args:
            snapshot_id: Specific snapshot ID to query
            timestamp: Timestamp to query (format: 'YYYY-MM-DD HH:MM:SS')
            
        Returns:
            DataFrame for the specified version
        """
        if snapshot_id:
            logger.info(f"Time travel query for snapshot: {snapshot_id}")
            query = f"SELECT * FROM {self.full_table_name} VERSION AS OF {snapshot_id}"
        elif timestamp:
            logger.info(f"Time travel query for timestamp: {timestamp}")
            query = f"SELECT * FROM {self.full_table_name} TIMESTAMP AS OF '{timestamp}'"
        else:
            raise ValueError("Either snapshot_id or timestamp must be provided")
        
        return self.spark.sql(query)
    
    def optimize_table(self):
        """
        Optimize Iceberg table by compacting small files.
        """
        logger.info(f"Optimizing table: {self.full_table_name}")
        
        try:
            # Compact small files
            self.spark.sql(f"CALL {self.catalog_name}.system.rewrite_data_files(table => '{self.database_name}.{self.table_name}')")
            logger.info("Table optimization completed")
        except Exception as e:
            logger.warning(f"Table optimization failed: {str(e)}")
    
    def get_record_count(self) -> int:
        """
        Get the current record count from the table.
        
        Returns:
            Number of records in the table
        """
        if not self.table_exists():
            return 0
        
        count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.full_table_name}").collect()[0].count
        return count
