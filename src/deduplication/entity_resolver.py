"""
Entity resolution and deduplication module using fuzzy matching algorithms.
"""
import logging
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType
from fuzzywuzzy import fuzz
import re

logger = logging.getLogger(__name__)


class EntityResolver:
    """
    Performs entity resolution to identify and merge duplicate corporate records.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the EntityResolver.
        
        Args:
            spark: SparkSession instance
            config: Pipeline configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.resolution_config = config['entity_resolution']
        self.matching_threshold = self.resolution_config['matching_threshold']
        
    @staticmethod
    def normalize_name(name: str) -> str:
        """
        Normalize corporate name for matching.
        
        Args:
            name: Corporate name to normalize
            
        Returns:
            Normalized name
        """
        if not name:
            return ""
        
        # Convert to lowercase
        normalized = name.lower()
        
        # Remove common corporate suffixes
        suffixes = [
            r'\s+inc\.?$', r'\s+incorporated$', r'\s+corp\.?$', 
            r'\s+corporation$', r'\s+ltd\.?$', r'\s+limited$',
            r'\s+llc$', r'\s+co\.?$', r'\s+company$'
        ]
        for suffix in suffixes:
            normalized = re.sub(suffix, '', normalized)
        
        # Remove special characters and extra spaces
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    @staticmethod
    def extract_city(address: str) -> str:
        """
        Extract city from address string.
        
        Args:
            address: Full address string
            
        Returns:
            Extracted city name
        """
        if not address:
            return ""
        
        # Simple heuristic: city is typically before the state/ZIP
        # Format: "123 Street, City, ST 12345"
        parts = address.split(',')
        if len(parts) >= 2:
            return parts[1].strip().lower()
        return ""
    
    @staticmethod
    def compute_similarity(name1: str, name2: str) -> float:
        """
        Compute similarity score between two corporate names.
        
        Args:
            name1: First corporate name
            name2: Second corporate name
            
        Returns:
            Similarity score between 0 and 1
        """
        if not name1 or not name2:
            return 0.0
        
        # Use multiple fuzzy matching algorithms
        ratio = fuzz.ratio(name1, name2) / 100.0
        token_sort = fuzz.token_sort_ratio(name1, name2) / 100.0
        token_set = fuzz.token_set_ratio(name1, name2) / 100.0
        
        # Weighted average favoring token-based matching
        similarity = (ratio * 0.3 + token_sort * 0.4 + token_set * 0.3)
        
        return similarity
    
    def prepare_source1_for_matching(self, df: DataFrame) -> DataFrame:
        """
        Prepare source 1 data for entity resolution.
        
        Args:
            df: Source 1 DataFrame
            
        Returns:
            Prepared DataFrame with normalized fields
        """
        logger.info("Preparing Source 1 data for matching")
        
        # Register UDF for name normalization
        normalize_udf = F.udf(self.normalize_name, StringType())
        extract_city_udf = F.udf(self.extract_city, StringType())
        
        # Add normalized fields
        df_prepared = df.withColumn("normalized_name", normalize_udf(F.col("corporate_name_s1"))) \
                        .withColumn("city", extract_city_udf(F.col("address"))) \
                        .withColumn("supplier_count", 
                                  F.size(F.split(F.col("top_suppliers"), ";")))
        
        return df_prepared
    
    def prepare_source2_for_matching(self, df: DataFrame) -> DataFrame:
        """
        Prepare source 2 data for entity resolution.
        
        Args:
            df: Source 2 DataFrame
            
        Returns:
            Prepared DataFrame with normalized fields
        """
        logger.info("Preparing Source 2 data for matching")
        
        # Register UDF for name normalization
        normalize_udf = F.udf(self.normalize_name, StringType())
        
        # Add normalized fields
        df_prepared = df.withColumn("normalized_name", normalize_udf(F.col("corporate_name_s2"))) \
                        .withColumn("customer_count", 
                                  F.size(F.split(F.col("main_customers"), ";")))
        
        return df_prepared
    
    def perform_blocking(self, df1: DataFrame, df2: DataFrame) -> DataFrame:
        """
        Perform blocking to reduce comparison space using blocking keys.
        
        Args:
            df1: Prepared source 1 DataFrame
            df2: Prepared source 2 DataFrame
            
        Returns:
            DataFrame of candidate pairs
        """
        logger.info("Performing blocking to generate candidate pairs")
        
        # Create blocking keys - first 3 characters of normalized name
        df1_blocked = df1.withColumn("block_key", F.substring(F.col("normalized_name"), 1, 3))
        df2_blocked = df2.withColumn("block_key", F.substring(F.col("normalized_name"), 1, 3))
        
        # Join on blocking key to create candidate pairs
        candidates = df1_blocked.alias("s1").join(
            df2_blocked.alias("s2"),
            F.col("s1.block_key") == F.col("s2.block_key"),
            "inner"
        )
        
        candidate_count = candidates.count()
        logger.info(f"Generated {candidate_count} candidate pairs for comparison")
        
        return candidates
    
    def compute_match_scores(self, candidates: DataFrame) -> DataFrame:
        """
        Compute similarity scores for candidate pairs.
        
        Args:
            candidates: DataFrame of candidate pairs
            
        Returns:
            DataFrame with similarity scores
        """
        logger.info("Computing similarity scores for candidate pairs")
        
        # Register UDF for similarity computation
        similarity_udf = F.udf(self.compute_similarity, DoubleType())
        
        # Compute similarity score
        scored = candidates.withColumn(
            "similarity_score",
            similarity_udf(F.col("s1.normalized_name"), F.col("s2.normalized_name"))
        )
        
        # Filter by threshold
        matches = scored.filter(F.col("similarity_score") >= self.matching_threshold)
        
        match_count = matches.count()
        logger.info(f"Found {match_count} matches above threshold {self.matching_threshold}")
        
        return matches
    
    def assign_corporate_ids(self, matches: DataFrame) -> DataFrame:
        """
        Assign unique corporate IDs to matched entities.
        
        Args:
            matches: DataFrame of matched pairs
            
        Returns:
            DataFrame with assigned corporate IDs
        """
        logger.info("Assigning unique corporate IDs to matched entities")
        
        # Use dense_rank to assign IDs based on normalized name
        # Entities with same normalized name get same ID
        window_spec = Window.orderBy("s1.normalized_name")
        
        with_ids = matches.withColumn(
            "corporate_id",
            F.concat(F.lit("CORP_"), F.lpad(F.dense_rank().over(window_spec).cast("string"), 6, "0"))
        )
        
        return with_ids
    
    def create_harmonized_dataset(self, matched_df: DataFrame, 
                                  source1_df: DataFrame, 
                                  source2_df: DataFrame) -> DataFrame:
        """
        Create unified harmonized dataset from matched entities.
        
        Args:
            matched_df: DataFrame with matched pairs and corporate IDs
            source1_df: Original source 1 DataFrame
            source2_df: Original source 2 DataFrame
            
        Returns:
            Harmonized DataFrame with all fields
        """
        logger.info("Creating harmonized dataset")
        
        # Select and rename fields from matched pairs
        harmonized = matched_df.select(
            F.col("corporate_id"),
            F.coalesce(F.col("s1.corporate_name_s1"), F.col("s2.corporate_name_s2")).alias("corporate_name"),
            F.col("s1.normalized_name"),
            F.col("s1.address"),
            F.col("s1.activity_places"),
            F.col("s1.top_suppliers"),
            F.col("s1.supplier_count"),
            F.col("s2.main_customers"),
            F.col("s2.customer_count"),
            F.col("s2.revenue"),
            F.col("s2.profit"),
            F.col("similarity_score"),
            F.current_timestamp().alias("harmonization_timestamp")
        )
        
        # Add records from source1 that didn't match
        source1_prepared = self.prepare_source1_for_matching(source1_df)
        matched_s1_names = matched_df.select("s1.corporate_name_s1").distinct()
        
        unmatched_s1 = source1_prepared.join(
            matched_s1_names,
            source1_prepared.corporate_name_s1 == matched_s1_names.corporate_name_s1,
            "left_anti"
        )
        
        # Add corporate IDs to unmatched source1 records
        max_id_query = harmonized.agg(F.max("corporate_id")).collect()[0][0]
        if max_id_query:
            start_id = int(max_id_query.split("_")[1]) + 1
        else:
            start_id = 1
        
        unmatched_s1_with_id = unmatched_s1.withColumn(
            "row_num",
            F.row_number().over(Window.orderBy("corporate_name_s1"))
        ).withColumn(
            "corporate_id",
            F.concat(F.lit("CORP_"), F.lpad((F.col("row_num") + start_id).cast("string"), 6, "0"))
        ).select(
            F.col("corporate_id"),
            F.col("corporate_name_s1").alias("corporate_name"),
            F.col("normalized_name"),
            F.col("address"),
            F.col("activity_places"),
            F.col("top_suppliers"),
            F.col("supplier_count"),
            F.lit(None).cast("string").alias("main_customers"),
            F.lit(0).alias("customer_count"),
            F.lit(None).cast("long").alias("revenue"),
            F.lit(None).cast("long").alias("profit"),
            F.lit(0.0).alias("similarity_score"),
            F.current_timestamp().alias("harmonization_timestamp")
        )
        
        # Union all records
        final_harmonized = harmonized.union(unmatched_s1_with_id)
        
        record_count = final_harmonized.count()
        logger.info(f"Harmonized dataset created with {record_count} records")
        
        return final_harmonized
    
    def resolve_entities(self, source1_df: DataFrame, source2_df: DataFrame) -> DataFrame:
        """
        Main method to perform end-to-end entity resolution.
        
        Args:
            source1_df: Source 1 DataFrame
            source2_df: Source 2 DataFrame
            
        Returns:
            Harmonized DataFrame with resolved entities
        """
        logger.info("Starting entity resolution process")
        
        # Prepare data
        s1_prepared = self.prepare_source1_for_matching(source1_df)
        s2_prepared = self.prepare_source2_for_matching(source2_df)
        
        # Perform blocking
        candidates = self.perform_blocking(s1_prepared, s2_prepared)
        
        # Compute similarity scores and filter matches
        matches = self.compute_match_scores(candidates)
        
        # Assign corporate IDs
        with_ids = self.assign_corporate_ids(matches)
        
        # Create harmonized dataset
        harmonized = self.create_harmonized_dataset(with_ids, source1_df, source2_df)
        
        logger.info("Entity resolution completed successfully")
        return harmonized
