#!/usr/bin/env python3
"""
Main pipeline orchestration script that runs the complete ETL and ML workflow.
"""
import sys
import logging
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import load_config, setup_logging, get_spark_session
from src.ingestion import DataIngestor
from src.deduplication import EntityResolver
from src.iceberg import IcebergTableManager
from src.ml import MLModelTrainer


def run_pipeline(config_path: str = "config/pipeline_config.yaml", 
                 skip_ml: bool = False,
                 skip_ingestion: bool = False):
    """
    Execute the complete data and ML pipeline.
    
    Args:
        config_path: Path to configuration file
        skip_ml: Skip ML training step
        skip_ingestion: Skip ingestion and use existing Iceberg table
    """
    # Load configuration
    config = load_config(config_path)
    logger = setup_logging(config)
    
    logger.info("=" * 80)
    logger.info("Starting Data & AI Pipeline")
    logger.info("=" * 80)
    
    try:
        # Initialize Spark session
        logger.info("Initializing Spark session...")
        spark = get_spark_session("DataAIPipeline", config)
        logger.info(f"Spark version: {spark.version}")
        
        # Initialize Iceberg table manager
        logger.info("Initializing Iceberg table manager...")
        iceberg_manager = IcebergTableManager(spark, config)
        iceberg_manager.create_database()
        iceberg_manager.create_table()
        
        if not skip_ingestion:
            # Step 1: Data Ingestion
            logger.info("\n" + "=" * 80)
            logger.info("STEP 1: DATA INGESTION")
            logger.info("=" * 80)
            
            ingestor = DataIngestor(spark, config)
            source1_df, source2_df = ingestor.ingest_all_sources()
            
            logger.info(f"Source 1 records: {source1_df.count()}")
            logger.info(f"Source 2 records: {source2_df.count()}")
            
            # Step 2: Entity Resolution & Deduplication
            logger.info("\n" + "=" * 80)
            logger.info("STEP 2: ENTITY RESOLUTION & DEDUPLICATION")
            logger.info("=" * 80)
            
            resolver = EntityResolver(spark, config)
            harmonized_df = resolver.resolve_entities(source1_df, source2_df)
            
            logger.info(f"Harmonized records: {harmonized_df.count()}")
            
            # Show sample of harmonized data
            logger.info("Sample harmonized records:")
            harmonized_df.select("corporate_id", "corporate_name", "revenue", "profit").show(5, truncate=False)
            
            # Step 3: Iceberg Upsert
            logger.info("\n" + "=" * 80)
            logger.info("STEP 3: ICEBERG TABLE UPSERT")
            logger.info("=" * 80)
            
            iceberg_manager.upsert_data(harmonized_df)
            
            logger.info(f"Total records in Iceberg table: {iceberg_manager.get_record_count()}")
        
        else:
            logger.info("Skipping ingestion, using existing Iceberg table data")
        
        # Step 4: ML Model Training
        if not skip_ml:
            logger.info("\n" + "=" * 80)
            logger.info("STEP 4: MACHINE LEARNING MODEL TRAINING")
            logger.info("=" * 80)
            
            # Read data from Iceberg table
            ml_data = iceberg_manager.read_table()
            logger.info(f"Records loaded for ML training: {ml_data.count()}")
            
            # Train model
            trainer = MLModelTrainer(spark, config)
            model, metrics = trainer.train_and_evaluate(ml_data)
            
            logger.info("Model training completed successfully")
            logger.info(f"Model metrics: {metrics}")
        else:
            logger.info("Skipping ML training step")
        
        # Pipeline completion
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        
        # Summary statistics
        logger.info("\nPipeline Summary:")
        logger.info(f"  - Total corporate entities: {iceberg_manager.get_record_count()}")
        if not skip_ml:
            logger.info(f"  - Model AUC: {metrics.get('auc', 'N/A'):.4f}")
            logger.info(f"  - Model Accuracy: {metrics.get('accuracy', 'N/A'):.4f}")
        logger.info(f"  - Iceberg table: {iceberg_manager.full_table_name}")
        
        # Stop Spark
        spark.stop()
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
        if 'spark' in locals():
            spark.stop()
        return 1


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Corporate Data & AI Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline
  python src/main.py
  
  # Run pipeline with custom config
  python src/main.py --config custom_config.yaml
  
  # Skip ML training
  python src/main.py --skip-ml
  
  # Use existing data (skip ingestion)
  python src/main.py --skip-ingestion
        """
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config/pipeline_config.yaml',
        help='Path to configuration file (default: config/pipeline_config.yaml)'
    )
    
    parser.add_argument(
        '--skip-ml',
        action='store_true',
        help='Skip ML training step'
    )
    
    parser.add_argument(
        '--skip-ingestion',
        action='store_true',
        help='Skip data ingestion and use existing Iceberg table'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    exit_code = run_pipeline(
        config_path=args.config,
        skip_ml=args.skip_ml,
        skip_ingestion=args.skip_ingestion
    )
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
