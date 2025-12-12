"""
Machine Learning pipeline module for training profit prediction models.
"""
import logging
from typing import Dict, Any, Tuple
import mlflow
import mlflow.spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

logger = logging.getLogger(__name__)


class MLModelTrainer:
    """
    Trains machine learning models on harmonized corporate data.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the MLModelTrainer.
        
        Args:
            spark: SparkSession instance
            config: Pipeline configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.ml_config = config['ml']
        self.mlflow_config = self.ml_config['mlflow']
        
        # Setup MLflow
        mlflow.set_tracking_uri(self.mlflow_config['tracking_uri'])
        mlflow.set_experiment(self.mlflow_config['experiment_name'])
        
    def prepare_training_data(self, df: DataFrame) -> DataFrame:
        """
        Prepare data for ML training by creating features and target variable.
        
        Args:
            df: Input DataFrame from Iceberg table
            
        Returns:
            DataFrame ready for training
        """
        logger.info("Preparing data for ML training")
        
        # Filter out records with null revenue or profit
        df_clean = df.filter(
            (F.col("revenue").isNotNull()) & 
            (F.col("profit").isNotNull()) &
            (F.col("supplier_count").isNotNull())
        )
        
        # Create target variable: high_profit (1 if profit > threshold, 0 otherwise)
        profit_threshold = self.ml_config['profit_threshold']
        df_with_target = df_clean.withColumn(
            "high_profit",
            F.when(F.col("profit") > profit_threshold, 1).otherwise(0)
        )
        
        # Handle missing customer_count (default to 0)
        df_features = df_with_target.fillna({"customer_count": 0})
        
        # Select features for modeling
        feature_cols = self.ml_config['features']
        target_col = self.ml_config['target_column']
        
        ml_data = df_features.select(
            feature_cols + [target_col, "corporate_id", "corporate_name"]
        )
        
        record_count = ml_data.count()
        logger.info(f"Prepared {record_count} records for training")
        
        # Show class distribution
        class_dist = ml_data.groupBy(target_col).count().collect()
        for row in class_dist:
            logger.info(f"Class {row[target_col]}: {row['count']} records")
        
        return ml_data
    
    def create_ml_pipeline(self) -> Pipeline:
        """
        Create PySpark ML Pipeline with feature engineering and model.
        
        Returns:
            Configured ML Pipeline
        """
        logger.info("Creating ML pipeline")
        
        feature_cols = self.ml_config['features']
        
        # Feature assembler
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw"
        )
        
        # Feature scaling
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Logistic Regression model
        training_config = self.ml_config['training']
        lr = LogisticRegression(
            featuresCol="features",
            labelCol=self.ml_config['target_column'],
            maxIter=training_config['max_iter'],
            regParam=0.01,
            elasticNetParam=0.5
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, lr])
        
        logger.info("ML pipeline created successfully")
        return pipeline
    
    def split_data(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Split data into training and testing sets.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (train_df, test_df)
        """
        training_config = self.ml_config['training']
        test_size = training_config['test_size']
        random_state = training_config['random_state']
        
        train_df, test_df = df.randomSplit([1 - test_size, test_size], seed=random_state)
        
        logger.info(f"Training set size: {train_df.count()}")
        logger.info(f"Test set size: {test_df.count()}")
        
        return train_df, test_df
    
    def train_model(self, train_df: DataFrame, test_df: DataFrame):
        """
        Train the ML model and log to MLflow.
        
        Args:
            train_df: Training DataFrame
            test_df: Testing DataFrame
            
        Returns:
            Trained Pipeline Model
        """
        logger.info("Starting model training")
        
        with mlflow.start_run(run_name="corporate_profit_predictor") as run:
            
            # Log parameters
            mlflow.log_params({
                "model_type": self.ml_config['training']['algorithm'],
                "features": ",".join(self.ml_config['features']),
                "profit_threshold": self.ml_config['profit_threshold'],
                "test_size": self.ml_config['training']['test_size'],
                "max_iter": self.ml_config['training']['max_iter']
            })
            
            # Create and train pipeline
            pipeline = self.create_ml_pipeline()
            
            logger.info("Training model...")
            model = pipeline.fit(train_df)
            logger.info("Model training completed")
            
            # Make predictions on test set
            predictions = model.transform(test_df)
            
            # Evaluate model
            metrics = self.evaluate_model(predictions)
            
            # Log metrics to MLflow
            mlflow.log_metrics(metrics)
            
            # Log model
            mlflow.spark.log_model(
                model,
                artifact_path="model",
                registered_model_name=self.ml_config['model_name']
            )
            
            # Log feature importance (from logistic regression)
            lr_model = model.stages[-1]
            feature_importance = {
                f"feature_{i}_{feat}": float(coef) 
                for i, (feat, coef) in enumerate(zip(self.ml_config['features'], lr_model.coefficients))
            }
            mlflow.log_params(feature_importance)
            
            logger.info(f"Model logged to MLflow. Run ID: {run.info.run_id}")
            logger.info(f"Metrics: {metrics}")
            
            return model
    
    def evaluate_model(self, predictions: DataFrame) -> Dict[str, float]:
        """
        Evaluate model performance.
        
        Args:
            predictions: DataFrame with predictions
            
        Returns:
            Dictionary of evaluation metrics
        """
        logger.info("Evaluating model performance")
        
        target_col = self.ml_config['target_column']
        
        # Binary classification metrics
        binary_evaluator = BinaryClassificationEvaluator(
            labelCol=target_col,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        auc = binary_evaluator.evaluate(predictions)
        
        # Multiclass metrics
        accuracy_evaluator = MulticlassClassificationEvaluator(
            labelCol=target_col,
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = accuracy_evaluator.evaluate(predictions)
        
        precision_evaluator = MulticlassClassificationEvaluator(
            labelCol=target_col,
            predictionCol="prediction",
            metricName="weightedPrecision"
        )
        precision = precision_evaluator.evaluate(predictions)
        
        recall_evaluator = MulticlassClassificationEvaluator(
            labelCol=target_col,
            predictionCol="prediction",
            metricName="weightedRecall"
        )
        recall = recall_evaluator.evaluate(predictions)
        
        f1_evaluator = MulticlassClassificationEvaluator(
            labelCol=target_col,
            predictionCol="prediction",
            metricName="f1"
        )
        f1 = f1_evaluator.evaluate(predictions)
        
        metrics = {
            "auc": auc,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1_score": f1
        }
        
        logger.info(f"Model evaluation completed: {metrics}")
        return metrics
    
    def train_and_evaluate(self, df: DataFrame):
        """
        End-to-end training and evaluation pipeline.
        
        Args:
            df: Input DataFrame from Iceberg table
            
        Returns:
            Trained model and evaluation metrics
        """
        logger.info("Starting end-to-end ML training pipeline")
        
        # Prepare data
        ml_data = self.prepare_training_data(df)
        
        # Split data
        train_df, test_df = self.split_data(ml_data)
        
        # Train model
        model = self.train_model(train_df, test_df)
        
        # Make predictions on test set for final evaluation
        predictions = model.transform(test_df)
        metrics = self.evaluate_model(predictions)
        
        logger.info("ML training pipeline completed successfully")
        
        return model, metrics
    
    def load_model(self, model_uri: str = None):
        """
        Load a trained model from MLflow.
        
        Args:
            model_uri: MLflow model URI (e.g., 'models:/model_name/version')
            
        Returns:
            Loaded Pipeline Model
        """
        if model_uri is None:
            model_name = self.ml_config['model_name']
            model_uri = f"models:/{model_name}/latest"
        
        logger.info(f"Loading model from: {model_uri}")
        model = mlflow.spark.load_model(model_uri)
        logger.info("Model loaded successfully")
        
        return model
