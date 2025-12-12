"""
Tests for data ingestion module.
"""
import pytest
from unittest.mock import Mock, patch
from pyspark.sql.types import StructType, StructField, StringType, LongType


class TestDataIngestor:
    """Test suite for DataIngestor class."""
    
    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing."""
        return {
            'aws': {
                's3': {
                    'bucket_name': 'test-bucket',
                    'source1_prefix': 'data/source1/',
                    'source2_prefix': 'data/source2/'
                }
            },
            'ingestion': {
                'source1': {
                    'name': 'supply_chain_data',
                    'schema': ['corporate_name_s1', 'address', 'activity_places', 'top_suppliers']
                },
                'source2': {
                    'name': 'financial_data',
                    'schema': ['corporate_name_s2', 'main_customers', 'revenue', 'profit']
                }
            }
        }
    
    def test_get_source1_schema(self, sample_config):
        """Test source 1 schema definition."""
        from src.ingestion.data_ingestor import DataIngestor
        
        spark = Mock()
        ingestor = DataIngestor(spark, sample_config)
        schema = ingestor.get_source1_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 4
        assert schema.fields[0].name == "corporate_name_s1"
        assert isinstance(schema.fields[0].dataType, StringType)
    
    def test_get_source2_schema(self, sample_config):
        """Test source 2 schema definition."""
        from src.ingestion.data_ingestor import DataIngestor
        
        spark = Mock()
        ingestor = DataIngestor(spark, sample_config)
        schema = ingestor.get_source2_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 4
        assert schema.fields[2].name == "revenue"
        assert isinstance(schema.fields[2].dataType, LongType)
    
    def test_get_s3_path(self, sample_config):
        """Test S3 path construction."""
        from src.ingestion.data_ingestor import DataIngestor
        
        spark = Mock()
        ingestor = DataIngestor(spark, sample_config)
        
        path = ingestor._get_s3_path('source1_prefix', 'test.csv')
        assert path == "s3a://test-bucket/data/source1/test.csv"
        
        path_no_file = ingestor._get_s3_path('source2_prefix')
        assert path_no_file == "s3a://test-bucket/data/source2"


@pytest.mark.integration
class TestDataIngestorIntegration:
    """Integration tests for data ingestion."""
    
    @pytest.fixture
    def spark_session(self):
        """Create a Spark session for testing."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[2]") \
            .appName("test") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_validate_data_valid(self, spark_session):
        """Test data validation with valid data."""
        from src.ingestion.data_ingestor import DataIngestor
        
        config = {
            'aws': {'s3': {'bucket_name': 'test'}},
            'ingestion': {}
        }
        
        ingestor = DataIngestor(spark_session, config)
        
        data = [
            ("Company A", "Address 1"),
            ("Company B", "Address 2")
        ]
        df = spark_session.createDataFrame(data, ["corporate_name_s1", "address"])
        
        # Should not raise exception
        assert ingestor.validate_data(df, "source1") is True
    
    def test_validate_data_empty(self, spark_session):
        """Test data validation with empty DataFrame."""
        from src.ingestion.data_ingestor import DataIngestor
        
        config = {
            'aws': {'s3': {'bucket_name': 'test'}},
            'ingestion': {}
        }
        
        ingestor = DataIngestor(spark_session, config)
        
        df = spark_session.createDataFrame([], "corporate_name_s1 string, address string")
        
        with pytest.raises(ValueError, match="No data found"):
            ingestor.validate_data(df, "source1")
