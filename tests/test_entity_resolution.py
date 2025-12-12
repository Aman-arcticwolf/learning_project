"""
Unit tests for entity resolution functionality.
"""
import pytest
from src.deduplication.entity_resolver import EntityResolver


class TestEntityResolver:
    """Test suite for EntityResolver class."""
    
    def test_normalize_name_basic(self):
        """Test basic name normalization."""
        assert EntityResolver.normalize_name("Acme Corporation") == "acme"
        assert EntityResolver.normalize_name("ACME Corp") == "acme"
        assert EntityResolver.normalize_name("Acme Inc.") == "acme"
        assert EntityResolver.normalize_name("Acme Incorporated") == "acme"
    
    def test_normalize_name_with_special_chars(self):
        """Test normalization with special characters."""
        assert EntityResolver.normalize_name("Tech-Global, Inc.") == "tech global"
        assert EntityResolver.normalize_name("AT&T Corporation") == "at t"
        assert EntityResolver.normalize_name("3M Company") == "3m"
    
    def test_normalize_name_removes_suffixes(self):
        """Test removal of corporate suffixes."""
        test_cases = [
            ("Company Inc", "company"),
            ("Business Corp.", "business"),
            ("Enterprise Ltd", "enterprise"),
            ("Ventures LLC", "ventures"),
            ("Group Limited", "group"),
        ]
        for input_name, expected in test_cases:
            assert EntityResolver.normalize_name(input_name) == expected
    
    def test_normalize_name_empty(self):
        """Test normalization of empty or None values."""
        assert EntityResolver.normalize_name("") == ""
        assert EntityResolver.normalize_name(None) == ""
    
    def test_extract_city_standard_format(self):
        """Test city extraction from standard address format."""
        address = "123 Main Street, New York, NY 10001"
        assert EntityResolver.extract_city(address) == "new york"
    
    def test_extract_city_various_formats(self):
        """Test city extraction from various address formats."""
        addresses = [
            ("456 Oak Ave, San Francisco, CA 94105", "san francisco"),
            ("789 Elm St, Chicago, IL", "chicago"),
            ("321 Pine Road, Boston, MA 02101", "boston"),
        ]
        for address, expected_city in addresses:
            assert EntityResolver.extract_city(address) == expected_city
    
    def test_extract_city_invalid(self):
        """Test city extraction with invalid inputs."""
        assert EntityResolver.extract_city("") == ""
        assert EntityResolver.extract_city(None) == ""
        assert EntityResolver.extract_city("NoCommasHere") == ""
    
    def test_compute_similarity_identical(self):
        """Test similarity computation for identical names."""
        similarity = EntityResolver.compute_similarity("acme corp", "acme corp")
        assert similarity == 1.0
    
    def test_compute_similarity_similar(self):
        """Test similarity computation for similar names."""
        similarity = EntityResolver.compute_similarity("acme corporation", "acme corp")
        assert similarity > 0.7  # Should be high similarity
    
    def test_compute_similarity_different(self):
        """Test similarity computation for different names."""
        similarity = EntityResolver.compute_similarity("acme corp", "techglobal inc")
        assert similarity < 0.5  # Should be low similarity
    
    def test_compute_similarity_with_typos(self):
        """Test similarity computation with typos."""
        similarity = EntityResolver.compute_similarity("acme corporation", "acme corporaton")
        assert similarity > 0.8  # Should tolerate minor typos
    
    def test_compute_similarity_empty(self):
        """Test similarity computation with empty strings."""
        assert EntityResolver.compute_similarity("", "") == 0.0
        assert EntityResolver.compute_similarity("acme", "") == 0.0
        assert EntityResolver.compute_similarity("", "acme") == 0.0


@pytest.mark.integration
class TestEntityResolverIntegration:
    """Integration tests requiring Spark session."""
    
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
    
    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing."""
        return {
            'entity_resolution': {
                'matching_threshold': 0.85,
                'blocking_keys': ['normalized_name', 'city'],
                'algorithms': ['levenshtein', 'jaro_winkler']
            }
        }
    
    def test_prepare_source1_for_matching(self, spark_session, sample_config):
        """Test preparation of source 1 data."""
        resolver = EntityResolver(spark_session, sample_config)
        
        # Create sample data
        data = [
            ("Acme Corporation", "123 Main St, New York, NY", "NY;CA", "S1;S2;S3"),
            ("TechGlobal Inc", "456 Tech Ave, San Francisco, CA", "CA;TX", "T1;T2")
        ]
        df = spark_session.createDataFrame(
            data,
            ["corporate_name_s1", "address", "activity_places", "top_suppliers"]
        )
        
        result_df = resolver.prepare_source1_for_matching(df)
        
        # Verify new columns exist
        assert "normalized_name" in result_df.columns
        assert "city" in result_df.columns
        assert "supplier_count" in result_df.columns
        
        # Verify counts
        assert result_df.count() == 2
        
        # Verify normalization
        first_row = result_df.first()
        assert first_row.normalized_name == "acme"
        assert first_row.supplier_count == 3
