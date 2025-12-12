"""
Tests for Iceberg table management.
"""
import pytest
from unittest.mock import Mock, MagicMock


class TestIcebergTableManager:
    """Test suite for IcebergTableManager class."""
    
    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing."""
        return {
            'aws': {
                'glue': {
                    'catalog_name': 'glue_catalog',
                    'database_name': 'test_db',
                    'table_name': 'test_table'
                }
            },
            'iceberg': {
                'table_name': 'test_table',
                'partition_by': ['registration_date'],
                'properties': {
                    'write.format.default': 'parquet'
                }
            }
        }
    
    def test_table_manager_initialization(self, sample_config):
        """Test IcebergTableManager initialization."""
        from src.iceberg.table_manager import IcebergTableManager
        
        spark = Mock()
        manager = IcebergTableManager(spark, sample_config)
        
        assert manager.catalog_name == 'glue_catalog'
        assert manager.database_name == 'test_db'
        assert manager.table_name == 'test_table'
        assert manager.full_table_name == 'glue_catalog.test_db.test_table'
    
    def test_full_table_name_format(self, sample_config):
        """Test full table name format."""
        from src.iceberg.table_manager import IcebergTableManager
        
        spark = Mock()
        manager = IcebergTableManager(spark, sample_config)
        
        expected = "glue_catalog.test_db.test_table"
        assert manager.full_table_name == expected


@pytest.mark.integration
class TestIcebergTableManagerIntegration:
    """Integration tests for Iceberg operations (requires Iceberg setup)."""
    
    @pytest.fixture
    def spark_session(self):
        """Create a Spark session for testing."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[2]") \
            .appName("test_iceberg") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_get_record_count_empty(self, spark_session):
        """Test record count on non-existent table."""
        from src.iceberg.table_manager import IcebergTableManager
        
        config = {
            'aws': {
                'glue': {
                    'catalog_name': 'glue_catalog',
                    'database_name': 'test_db',
                    'table_name': 'nonexistent_table'
                }
            },
            'iceberg': {
                'table_name': 'nonexistent_table',
                'properties': {}
            }
        }
        
        manager = IcebergTableManager(spark_session, config)
        count = manager.get_record_count()
        
        assert count == 0  # Non-existent table should return 0
