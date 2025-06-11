from pyspark.sql import SparkSession

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