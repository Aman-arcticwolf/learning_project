"""
Tests for ML model training module.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock


class TestMLModelTrainer:
    """Test suite for MLModelTrainer class."""
    
    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing."""
        return {
            'ml': {
                'model_name': 'test_model',
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
                    'tracking_uri': 'file:///tmp/mlruns',
                    'experiment_name': 'test_experiment',
                    'registry_uri': 'file:///tmp/mlregistry'
                }
            }
        }
    
    def test_ml_trainer_initialization(self, sample_config):
        """Test MLModelTrainer initialization."""
        from src.ml.model_trainer import MLModelTrainer
        
        spark = Mock()
        
        with patch('mlflow.set_tracking_uri'), \
             patch('mlflow.set_experiment'):
            trainer = MLModelTrainer(spark, sample_config)
            
            assert trainer.ml_config['model_name'] == 'test_model'
            assert trainer.ml_config['profit_threshold'] == 1000000
            assert len(trainer.ml_config['features']) == 3


@pytest.mark.integration
class TestMLModelTrainerIntegration:
    """Integration tests for ML training."""
    
    @pytest.fixture
    def spark_session(self):
        """Create a Spark session for testing."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[2]") \
            .appName("test_ml") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_data(self, spark_session):
        """Create sample data for ML training."""
        from pyspark.sql import functions as F
        
        data = [
            ("CORP_001", "Corp A", 5000000, 800000, 3, 2),
            ("CORP_002", "Corp B", 15000000, 2500000, 5, 4),
            ("CORP_003", "Corp C", 800000, 50000, 1, 1),
            ("CORP_004", "Corp D", 12000000, 1800000, 4, 3),
            ("CORP_005", "Corp E", 500000, 30000, 1, 1),
        ]
        
        df = spark_session.createDataFrame(
            data,
            ["corporate_id", "corporate_name", "revenue", "profit", "supplier_count", "customer_count"]
        )
        
        return df
    
    def test_prepare_training_data(self, spark_session, sample_data):
        """Test ML data preparation."""
        from src.ml.model_trainer import MLModelTrainer
        
        config = {
            'ml': {
                'model_name': 'test',
                'target_column': 'high_profit',
                'profit_threshold': 1000000,
                'features': ['revenue', 'supplier_count', 'customer_count'],
                'training': {},
                'mlflow': {
                    'tracking_uri': 'file:///tmp/mlruns',
                    'experiment_name': 'test',
                    'registry_uri': 'file:///tmp/mlregistry'
                }
            }
        }
        
        with patch('mlflow.set_tracking_uri'), \
             patch('mlflow.set_experiment'):
            trainer = MLModelTrainer(spark_session, config)
            ml_data = trainer.prepare_training_data(sample_data)
            
            # Verify target column creation
            assert "high_profit" in ml_data.columns
            
            # Verify feature columns present
            for feature in config['ml']['features']:
                assert feature in ml_data.columns
            
            # Verify data count
            assert ml_data.count() == 5
            
            # Verify target values
            target_values = [row.high_profit for row in ml_data.collect()]
            assert 1 in target_values  # At least one high profit
            assert 0 in target_values  # At least one low profit
    
    def test_create_ml_pipeline(self, spark_session):
        """Test ML pipeline creation."""
        from src.ml.model_trainer import MLModelTrainer
        from pyspark.ml import Pipeline
        
        config = {
            'ml': {
                'target_column': 'high_profit',
                'features': ['revenue', 'supplier_count', 'customer_count'],
                'training': {
                    'max_iter': 10,
                    'algorithm': 'logistic_regression'
                },
                'mlflow': {
                    'tracking_uri': 'file:///tmp/mlruns',
                    'experiment_name': 'test'
                }
            }
        }
        
        with patch('mlflow.set_tracking_uri'), \
             patch('mlflow.set_experiment'):
            trainer = MLModelTrainer(spark_session, config)
            pipeline = trainer.create_ml_pipeline()
            
            assert isinstance(pipeline, Pipeline)
            assert len(pipeline.getStages()) == 3  # Assembler, Scaler, Model
