"""
Test configuration and fixtures for pytest.
"""
import pytest
import sys
from pathlib import Path

# Add src to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))


@pytest.fixture(scope="session")
def project_config():
    """Load project configuration for testing."""
    return {
        'aws': {
            'region': 'us-east-1',
            's3': {
                'bucket_name': 'test-bucket',
                'source1_prefix': 'data/source1/',
                'source2_prefix': 'data/source2/',
                'iceberg_warehouse': 's3://test-bucket/iceberg-warehouse/',
                'mlflow_artifacts': 's3://test-bucket/mlflow-artifacts/'
            },
            'glue': {
                'catalog_name': 'glue_catalog',
                'database_name': 'test_db',
                'table_name': 'corporate_registry'
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
        },
        'entity_resolution': {
            'matching_threshold': 0.85,
            'blocking_keys': ['normalized_name', 'city'],
            'algorithms': ['levenshtein', 'jaro_winkler']
        },
        'iceberg': {
            'table_name': 'corporate_registry',
            'partition_by': ['registration_date'],
            'properties': {
                'write.format.default': 'parquet'
            }
        },
        'ml': {
            'model_name': 'corporate_profit_predictor',
            'target_column': 'high_profit',
            'profit_threshold': 1000000,
            'features': ['revenue', 'supplier_count', 'customer_count'],
            'training': {
                'test_size': 0.2,
                'random_state': 42,
                'algorithm': 'logistic_regression',
                'max_iter': 100
            },
            'mlflow': {
                'experiment_name': 'corporate_profit_prediction',
                'tracking_uri': 'file:///tmp/mlruns',
                'registry_uri': 'file:///tmp/mlregistry'
            }
        },
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'log_path': 'logs/pipeline.log'
        }
    }


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests that don't require external dependencies")
    config.addinivalue_line("markers", "integration: Integration tests that require Spark or other services")
    config.addinivalue_line("markers", "slow: Tests that take a long time to run")
