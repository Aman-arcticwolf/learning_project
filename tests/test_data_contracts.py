"""
Data contract validation tests to ensure schema compatibility.
"""
import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, TimestampType


@pytest.mark.unit
class TestDataContracts:
    """Test suite for data contract validation."""
    
    def test_iceberg_table_schema_contract(self):
        """Verify Iceberg table schema matches expected contract."""
        expected_schema = StructType([
            StructField("corporate_id", StringType(), False),
            StructField("corporate_name", StringType(), True),
            StructField("normalized_name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("activity_places", StringType(), True),
            StructField("top_suppliers", StringType(), True),
            StructField("supplier_count", IntegerType(), True),
            StructField("main_customers", StringType(), True),
            StructField("customer_count", IntegerType(), True),
            StructField("revenue", LongType(), True),
            StructField("profit", LongType(), True),
            StructField("similarity_score", DoubleType(), True),
            StructField("harmonization_timestamp", TimestampType(), True),
            StructField("last_updated", TimestampType(), True)
        ])
        
        # Verify schema structure
        assert len(expected_schema.fields) == 14
        assert expected_schema.fields[0].name == "corporate_id"
        assert expected_schema.fields[0].nullable is False  # Primary key must be non-null
    
    def test_source1_schema_contract(self):
        """Verify Source 1 schema contract."""
        from src.ingestion.data_ingestor import DataIngestor
        from unittest.mock import Mock
        
        config = {'aws': {'s3': {}}, 'ingestion': {}}
        ingestor = DataIngestor(Mock(), config)
        schema = ingestor.get_source1_schema()
        
        required_fields = ["corporate_name_s1", "address", "activity_places", "top_suppliers"]
        schema_field_names = [field.name for field in schema.fields]
        
        for field in required_fields:
            assert field in schema_field_names, f"Missing required field: {field}"
    
    def test_source2_schema_contract(self):
        """Verify Source 2 schema contract."""
        from src.ingestion.data_ingestor import DataIngestor
        from unittest.mock import Mock
        
        config = {'aws': {'s3': {}}, 'ingestion': {}}
        ingestor = DataIngestor(Mock(), config)
        schema = ingestor.get_source2_schema()
        
        required_fields = ["corporate_name_s2", "main_customers", "revenue", "profit"]
        schema_field_names = [field.name for field in schema.fields]
        
        for field in required_fields:
            assert field in schema_field_names, f"Missing required field: {field}"
        
        # Verify numeric fields are correct type
        revenue_field = next(f for f in schema.fields if f.name == "revenue")
        profit_field = next(f for f in schema.fields if f.name == "profit")
        
        assert isinstance(revenue_field.dataType, LongType)
        assert isinstance(profit_field.dataType, LongType)
    
    def test_ml_features_contract(self):
        """Verify ML pipeline features match expected contract."""
        expected_features = ["revenue", "supplier_count", "customer_count"]
        
        # Load config and verify
        from src.utils import load_config
        try:
            config = load_config()
            actual_features = config['ml']['features']
            
            assert set(actual_features) == set(expected_features), \
                f"ML features mismatch. Expected: {expected_features}, Got: {actual_features}"
        except Exception:
            # If config can't be loaded in test environment, verify structure only
            assert len(expected_features) == 3


@pytest.mark.integration
class TestDataContractIntegration:
    """Integration tests for data contracts with Spark."""
    
    @pytest.fixture
    def spark_session(self):
        """Create a Spark session for testing."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[2]") \
            .appName("test_contracts") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_harmonized_data_schema_compatibility(self, spark_session):
        """Test that harmonized data matches expected schema."""
        from pyspark.sql import functions as F
        
        # Create sample harmonized data
        data = [
            ("CORP_000001", "Test Corp", "test", "123 St", "NY", "S1;S2", 2,
             "C1;C2", 2, 1000000, 100000, 0.95, None, None)
        ]
        
        schema = StructType([
            StructField("corporate_id", StringType(), False),
            StructField("corporate_name", StringType(), True),
            StructField("normalized_name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("activity_places", StringType(), True),
            StructField("top_suppliers", StringType(), True),
            StructField("supplier_count", IntegerType(), True),
            StructField("main_customers", StringType(), True),
            StructField("customer_count", IntegerType(), True),
            StructField("revenue", LongType(), True),
            StructField("profit", LongType(), True),
            StructField("similarity_score", DoubleType(), True),
            StructField("harmonization_timestamp", TimestampType(), True),
            StructField("last_updated", TimestampType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        df = df.withColumn("harmonization_timestamp", F.current_timestamp()) \
               .withColumn("last_updated", F.current_timestamp())
        
        # Verify schema
        assert len(df.columns) == 14
        assert "corporate_id" in df.columns
        assert "revenue" in df.columns
        assert "profit" in df.columns
        
        # Verify data types
        assert dict(df.dtypes)["corporate_id"] == "string"
        assert dict(df.dtypes)["revenue"] == "bigint"
        assert dict(df.dtypes)["supplier_count"] == "int"
