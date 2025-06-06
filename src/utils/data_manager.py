import os
from dotenv import load_dotenv

load_dotenv()

class DataManager:
    def __init__(self):
        self.environment = os.getenv('ENVIRONMENT', 'local')
        self.data_path = os.getenv('DATA_PATH')
        self.output_path = os.getenv('OUTPUT_PATH')
    
    def get_input_path(self, filename):
        return f"{self.data_path}/{filename}"
    
    def get_output_path(self, filename):
        return f"{self.output_path}/{filename}"
    
    def configure_azure_access(self, spark):
        if self.environment == 'azure':
            storage_account = os.getenv('AZURE_STORAGE_ACCOUNT')
            storage_key = os.getenv('AZURE_STORAGE_KEY')
            spark.conf.set(
                f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
                storage_key
            )

# Usage in both environments
data_manager = DataManager()
spark = create_spark_session()
data_manager.configure_azure_access(spark)

# This works in both local and Azure environments
df = spark.read.parquet(data_manager.get_input_path("dataset.parquet"))