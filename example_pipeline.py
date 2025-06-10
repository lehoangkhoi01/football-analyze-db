# example_etl_pipeline.py
import os
from dotenv import load_dotenv 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.data_configure.unified_data_utils import UnifiedDataUtils

def create_spark_session():
    """Create Spark session with appropriate configuration"""
    print("🚀 Creating Spark session...")
    spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    return spark

def bronze_layer_processing(utils: UnifiedDataUtils):
    """Bronze layer - Raw data ingestion"""
    print("=== BRONZE LAYER PROCESSING ===")
    
    # Initialize directory structure
    utils.initialize_medallion_structure()
    
    # Example: Create sample raw data
    sample_data = [
        ("1", "John Doe", "john@email.com", "2024-01-01", "active"),
        ("2", "Jane Smith", "jane@email.com", "2024-01-02", "inactive"),
        ("3", "Bob Johnson", "bob@email.com", "2024-01-03", "active"),
    ]
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    raw_df = utils.spark.createDataFrame(sample_data, schema)
    
    # Add ingestion metadata
    raw_df_with_metadata = raw_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                 .withColumn("source_system", lit("sample_system"))
    
    # Write to bronze layer
    success = utils.write_delta_table(
        raw_df_with_metadata, 
        "bronze", 
        "customer_raw",
        mode="overwrite"
    )
    
    if success:
        print("✅ Bronze layer processing completed")
        
        # List files in bronze layer
        bronze_files = utils.list_files(utils.get_layer_path("bronze"))
        print(f"Files in bronze layer: {len(bronze_files)}")
        for file in bronze_files[:5]:  # Show first 5 files
            print(f"  - {file['name']} ({file['size']} bytes)")
    else:
        print("❌ Bronze layer processing failed")

def silver_layer_processing(utils: UnifiedDataUtils):
    """Silver layer - Data cleaning and transformation"""
    print("\n=== SILVER LAYER PROCESSING ===")
    
    # Read from bronze layer
    bronze_df = utils.read_delta_table("bronze", "customer_raw")
    
    if bronze_df is None:
        print("❌ Could not read from bronze layer")
        return
    
    print(f"Read {bronze_df.count()} records from bronze layer")
    
    # Data cleaning and transformations
    silver_df = bronze_df.filter(col("status") == "active") \
                         .withColumn("email_domain", regexp_extract(col("email"), "@(.+)", 1)) \
                         .withColumn("created_date", to_date(col("created_date"), "yyyy-MM-dd")) \
                         .withColumn("processed_timestamp", current_timestamp()) \
                         .select("id", "name", "email", "email_domain", "created_date", 
                                "ingestion_timestamp", "processed_timestamp")
    
    # Write to silver layer
    success = utils.write_delta_table(
        silver_df,
        "silver",
        "customer_clean",
        mode="overwrite"
    )
    
    if success:
        print("✅ Silver layer processing completed")
        print(f"Processed {silver_df.count()} active customers")
    else:
        print("❌ Silver layer processing failed")

def gold_layer_processing(utils: UnifiedDataUtils):
    """Gold layer - Business metrics and aggregations"""
    print("\n=== GOLD LAYER PROCESSING ===")
    
    # Read from silver layer
    silver_df = utils.read_delta_table("silver", "customer_clean")
    
    if silver_df is None:
        print("❌ Could not read from silver layer")
        return
    
    # Create business metrics
    domain_metrics = silver_df.groupBy("email_domain") \
                             .agg(count("*").alias("customer_count"),
                                  max("created_date").alias("latest_registration")) \
                             .withColumn("report_date", current_date())
    
    # Write to gold layer
    success = utils.write_delta_table(
        domain_metrics,
        "gold",
        "customer_domain_metrics",
        mode="overwrite"
    )
    
    if success:
        print("✅ Gold layer processing completed")
        domain_metrics.show()
    else:
        print("❌ Gold layer processing failed")

def demonstrate_file_operations(utils: UnifiedDataUtils):
    """Demonstrate various file operations"""
    print("\n=== FILE OPERATIONS DEMO ===")
    
    # Create a sample JSON configuration
    config_data = {
        "pipeline_version": "1.0",
        "last_run": "2024-01-01T10:00:00Z",
        "settings": {
            "batch_size": 1000,
            "retention_days": 30
        }
    }
    
    config_path = utils.get_temp_path("pipeline_config.json")
    
    # Write JSON file
    if utils.write_json_file(config_path, config_data):
        print("✅ JSON config file written")
        
        # Read it back
        read_config = utils.read_json_file(config_path)
        if read_config:
            print(f"✅ JSON config file read: {read_config['pipeline_version']}")
        
        # Check if file exists
        exists = utils.file_exists(config_path)
        print(f"✅ File exists check: {exists}")
        
        # Delete the file
        utils.delete_path(config_path)
        print("✅ Temporary file cleaned up")

def demonstrate_secrets(utils: UnifiedDataUtils):
    """Demonstrate secret management"""
    print("\n=== SECRETS MANAGEMENT DEMO ===")
    
    # In Databricks, this would use secret scopes
    # In local, this uses environment variables like AZURE_STORAGE_ACCOUNT_KEY
    storage_key = utils.get_secret("azure-storage", "account-key")
    
    if storage_key:
        print("✅ Secret retrieved successfully")
    else:
        print("⚠️  Secret not found (this is expected in demo)")

def main():
    """Main ETL pipeline execution"""
    print("🚀 Starting Medallion ETL Pipeline")
    
    # Create Spark session
    spark = create_spark_session()
    print("🚀 Create spark session success")
    
    # Initialize unified utilities
    utils = UnifiedDataUtils(spark)
    
    # Print environment information
    utils.print_environment_info()
    
    try:
        # Execute ETL pipeline
        bronze_layer_processing(utils)
        silver_layer_processing(utils)
        gold_layer_processing(utils)
        
        # Demonstrate additional features
        demonstrate_file_operations(utils)
        demonstrate_secrets(utils)
        
        print("\n🎉 Pipeline completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        
    finally:
        # Cleanup
        spark.stop()

if __name__ == "__main__":
    # Set environment variables for local testing
    os.environ["LOCAL_DATA_PATH"] = "/tmp/medallion_demo"
    os.environ["AZURE_STORAGE_ACCOUNT"] = "demostorageaccount"
    
    main()