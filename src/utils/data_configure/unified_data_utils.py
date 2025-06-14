import os
import json
import shutil
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import logging
from ..env_utils.env_loader import get_env

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UnifiedDataUtils:
    """
    Unified utilities that work both in Databricks (with dbutils) and local environments
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.is_databricks = self._is_databricks_environment()
        self.dbutils = self._get_dbutils() if self.is_databricks else None
        self.config = self._load_configuration()
        
    def _is_databricks_environment(self) -> bool:
        """Check if running in Databricks environment"""
        try:
            # Multiple ways to detect Databricks
            databricks_indicators = [
                "DATABRICKS_RUNTIME_VERSION" in os.environ,
                any("databricks" in str(conf).lower() for conf in self.spark.sparkContext.getConf().getAll()),
                os.path.exists("/databricks/")
            ]
            return any(databricks_indicators)
        except:
            return False
    
    def _get_dbutils(self):
        """Get dbutils if available"""
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark)
        except ImportError:
            try:
                # Alternative way to get dbutils
                return self.spark._jvm.com.databricks.backend.daemon.dbutils.DBUtils.get()
            except:
                return None
    
    def _load_configuration(self) -> Dict[str, Any]:
        """Load configuration based on environment"""
        if self.is_databricks:
            return self._get_databricks_config()
        else:
            return self._get_local_config()
    
    def _get_databricks_config(self) -> Dict[str, Any]:
        """Configuration for Databricks environment"""
        storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "yourstorageaccount")
        container = os.getenv("AZURE_CONTAINER", "medallion")
        base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
        
        return {
            "environment": "databricks",
            "storage_type": "adls",
            "base_path": base_path,
            "bronze_path": f"{base_path}/bronze",
            "silver_path": f"{base_path}/silver",
            "gold_path": f"{base_path}/gold",
            "checkpoint_path": f"{base_path}/checkpoints",
            "temp_path": f"{base_path}/temp"
        }
    
    def _get_local_config(self) -> Dict[str, Any]:
        """Configuration for local environment"""
        env = get_env(".env.local")
        base_path = env.get("BASE_DATA_PATH", "data")
        return {
            "environment": env.get("ENVIRONMENT"),
            "storage_type": env.get("STORAGE_TYPE"),
            "base_path": base_path,
            "bronze_path": f"{base_path}/bronze",
            "silver_path": f"{base_path}/silver",
            "gold_path": f"{base_path}/gold",
            "checkpoint_path": f"{base_path}/checkpoints",
            "temp_path": f"{base_path}/temp"
        }
    
    # ============ FILE SYSTEM OPERATIONS ============
    
    def list_files(self, path: str) -> List[Dict[str, Any]]:
        """List files in directory - works for both environments"""
        try:
            if self.is_databricks and self.dbutils:
                files = self.dbutils.fs.ls(path)
                return [{"name": f.name, "path": f.path, "size": f.size, "is_dir": f.isDir()} 
                       for f in files]
            else:
                # Local implementation
                if not os.path.exists(path):
                    return []
                
                files = []
                for item in os.listdir(path):
                    item_path = os.path.join(path, item)
                    stat = os.stat(item_path)
                    files.append({
                        "name": item,
                        "path": item_path,
                        "size": stat.st_size,
                        "is_dir": os.path.isdir(item_path)
                    })
                return files
        except Exception as e:
            logger.error(f"Error listing files in {path}: {str(e)}")
            return []
    
    def file_exists(self, path: str) -> bool:
        """Check if file/directory exists"""
        try:
            if self.is_databricks and self.dbutils:
                try:
                    self.dbutils.fs.ls(path)
                    return True
                except:
                    return False
            else:
                return os.path.exists(path)
        except:
            return False
    
    def create_directory(self, path: str) -> bool:
        """Create directory if it doesn't exist"""
        try:
            if self.is_databricks and self.dbutils:
                if not self.file_exists(path):
                    self.dbutils.fs.mkdirs(path)
                return True
            else:
                os.makedirs(path, exist_ok=True)
                return True
        except Exception as e:
            logger.error(f"Error creating directory {path}: {str(e)}")
            return False
    
    def delete_path(self, path: str, recursive: bool = False) -> bool:
        """Delete file or directory"""
        try:
            if self.is_databricks and self.dbutils:
                self.dbutils.fs.rm(path, recursive)
                return True
            else:
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    if recursive:
                        shutil.rmtree(path)
                    else:
                        os.rmdir(path)
                return True
        except Exception as e:
            logger.error(f"Error deleting {path}: {str(e)}")
            return False
    
    def copy_file(self, source: str, destination: str) -> bool:
        """Copy file from source to destination"""
        try:
            if self.is_databricks and self.dbutils:
                self.dbutils.fs.cp(source, destination)
                return True
            else:
                if os.path.isfile(source):
                    # Ensure destination directory exists
                    dest_dir = os.path.dirname(destination)
                    os.makedirs(dest_dir, exist_ok=True)
                    shutil.copy2(source, destination)
                elif os.path.isdir(source):
                    shutil.copytree(source, destination, dirs_exist_ok=True)
                return True
        except Exception as e:
            logger.error(f"Error copying {source} to {destination}: {str(e)}")
            return False
    
    def move_file(self, source: str, destination: str) -> bool:
        """Move file from source to destination"""
        try:
            if self.is_databricks and self.dbutils:
                self.dbutils.fs.mv(source, destination)
                return True
            else:
                # Ensure destination directory exists
                dest_dir = os.path.dirname(destination)
                os.makedirs(dest_dir, exist_ok=True)
                shutil.move(source, destination)
                return True
        except Exception as e:
            logger.error(f"Error moving {source} to {destination}: {str(e)}")
            return False
    
    # ============ DATA OPERATIONS ============
    
    def read_text_file(self, path: str) -> Optional[str]:
        """Read text file content"""
        try:
            if self.is_databricks and self.dbutils:
                return self.dbutils.fs.head(path)
            else:
                with open(path, 'r', encoding='utf-8') as f:
                    return f.read()
        except Exception as e:
            logger.error(f"Error reading text file {path}: {str(e)}")
            return None
    
    def write_text_file(self, path: str, content: str) -> bool:
        """Write text content to file"""
        try:
            if self.is_databricks and self.dbutils:
                # For Databricks, we need to use Spark to write text files
                temp_path = f"{self.config['temp_path']}/temp_text_file"
                df = self.spark.createDataFrame([(content,)], ["content"])
                df.coalesce(1).write.mode("overwrite").text(temp_path)
                
                # Find the actual file and move it
                files = self.list_files(temp_path)
                text_file = next((f for f in files if f["name"].endswith(".txt")), None)
                if text_file:
                    self.move_file(text_file["path"], path)
                    self.delete_path(temp_path, recursive=True)
                return True
            else:
                # Ensure directory exists
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(content)
                return True
        except Exception as e:
            logger.error(f"Error writing text file {path}: {str(e)}")
            return False
    
    def read_json_file(self, path: str) -> Optional[Dict]:
        """Read JSON file"""
        try:
            content = self.read_text_file(path)
            return json.loads(content) if content else None
        except Exception as e:
            logger.error(f"Error reading JSON file {path}: {str(e)}")
            return None
    
    def write_json_file(self, path: str, data: Dict) -> bool:
        """Write JSON data to file"""
        try:
            content = json.dumps(data, indent=2)
            return self.write_text_file(path, content)
        except Exception as e:
            logger.error(f"Error writing JSON file {path}: {str(e)}")
            return False
    
    # ============ SPARK DATA OPERATIONS ============
    
    def read_delta_table(self, layer: str, table_name: str) -> Optional[DataFrame]:
        """Read Delta table from specified layer"""
        try:
            path = f"{self.config[f'{layer}_path']}/{table_name}"
            
            if self.config["storage_type"] == "local":
                # Check if delta table exists, otherwise try parquet
                if self.file_exists(f"{path}/_delta_log"):
                    return self.spark.read.format("delta").load(path)
                elif self.file_exists(path):
                    return self.spark.read.parquet(path)
                else:
                    logger.warning(f"Table not found at {path}")
                    return None
            else:
                return self.spark.read.format("delta").load(path)
                
        except Exception as e:
            logger.error(f"Error reading table {table_name} from {layer}: {str(e)}")
            return None
    
    def write_delta_table(self, df: DataFrame, layer: str, table_name: str, 
                         mode: str = "overwrite", partition_by: Optional[List[str]] = None) -> bool:
        """Write DataFrame to Delta table in specified layer"""
        try:
            path = f"{self.config[f'{layer}_path']}/{table_name}"
            
            # Ensure directory exists
            self.create_directory(self.config[f'{layer}_path'])
            
            writer = df.write.mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            if self.config["storage_type"] == "local":
                # For local, try Delta first, fall back to Parquet if Delta not available
                try:
                    writer.format("delta").save(path)
                except Exception as delta_error:
                    logger.warning(f"Delta format failed, using Parquet: {delta_error}")
                    writer.parquet(path)
            else:
                writer.format("delta").save(path)
            
            logger.info(f"Successfully wrote table {table_name} to {layer} layer")
            return True
            
        except Exception as e:
            logger.error(f"Error writing table {table_name} to {layer}: {str(e)}")
            return False
    
    def read_csv(self, path: str, header: bool = True, infer_schema: bool = True, 
                schema: Optional[StructType] = None) -> Optional[DataFrame]:
        """Read CSV file"""
        try:
            reader = self.spark.read.option("header", header)
            
            if schema:
                reader = reader.schema(schema)
            elif infer_schema:
                reader = reader.option("inferSchema", "true")
            
            return reader.csv(path)
        except Exception as e:
            logger.error(f"Error reading CSV from {path}: {str(e)}")
            return None
    
    def write_csv(self, df: DataFrame, path: str, header: bool = True, 
                  mode: str = "overwrite") -> bool:
        """Write DataFrame to CSV"""
        try:
            writer = df.write.mode(mode).option("header", header)
            writer.csv(path)
            return True
        except Exception as e:
            logger.error(f"Error writing CSV to {path}: {str(e)}")
            return False
    
    
    def write_single_json(self, df: DataFrame, output_path: str, filename: str="data.json") -> bool: 
        """Write DataFrame to a single JSON file"""
        try:
            temp_dir = f"{output_path}_temp"
            # Write to temporary directory
            df.coalesce(1).write.mode("overwrite").json(temp_dir)
        
            # Find the part file
            part_files = [f for f in os.listdir(temp_dir) if f.startswith("part-") and f.endswith(".json")]
        
            if part_files:
                # Create output directory if it doesn't exist
                os.makedirs(output_path, exist_ok=True)
                
                # Read JSON Lines and convert to array format
                temp_file_path = os.path.join(temp_dir, part_files[0])
                output_file_path = os.path.join(output_path, filename)
                
                # Convert JSON Lines to JSON array
                json_objects = []
                with open(temp_file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:  # Skip empty lines
                            json_objects.append(json.loads(line))
                
                # Write as proper JSON array
                with open(output_file_path, 'w', encoding='utf-8') as f:
                    json.dump(json_objects, f, indent=2, ensure_ascii=False)
                
                # Clean up temp directory
                shutil.rmtree(temp_dir)
                return True
            else:
                logger.error(f"Error writing JSON to {output_path}/{filename}: Can not find part file")
                shutil.rmtree(temp_dir)
                return False
        except Exception as e:
            logger.error(f"Error writing JSON to {output_path}/{filename}: {str(e)}")
            return False
        
    # ============ PATH UTILITIES ============
    
    def get_layer_path(self, layer: str, table_name: Optional[str] = None) -> str:
        """Get path for medallion layer"""
        layer_path = self.config[f"{layer}_path"]
        return f"{layer_path}/{table_name}" if table_name else layer_path
    
    def get_checkpoint_path(self, job_name: str) -> str:
        """Get checkpoint path for streaming jobs"""
        return f"{self.config['checkpoint_path']}/{job_name}"
    
    def get_temp_path(self, temp_name: str) -> str:
        """Get temporary path"""
        return f"{self.config['temp_path']}/{temp_name}"
    
    def initialize_medallion_structure(self) -> bool:
        """Initialize the medallion architecture directory structure"""
        try:
            layers = ['bronze', 'silver', 'gold']
            for layer in layers:
                self.create_directory(self.config[f"{layer}_path"])
            
            # Create additional directories
            self.create_directory(self.config["checkpoint_path"])
            self.create_directory(self.config["temp_path"])
            
            logger.info("Medallion directory structure initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Error initializing medallion structure: {str(e)}")
            return False
    
    # ============ SECRET MANAGEMENT ============
    
    def get_secret(self, scope: str, key: str) -> Optional[str]:
        """Get secret from secret scope (Databricks) or environment variable (local)"""
        try:
            if self.is_databricks and self.dbutils:
                return self.dbutils.secrets.get(scope, key)
            else:
                # For local, use environment variables with naming convention
                env_var = f"{scope}_{key}".upper().replace("-", "_")
                return os.getenv(env_var)
        except Exception as e:
            logger.error(f"Error getting secret {scope}/{key}: {str(e)}")
            return None
    
    # ============ CONFIGURATION UTILITIES ============
    
    def get_config(self) -> Dict[str, Any]:
        """Get current configuration"""
        return self.config.copy()
    
    def print_environment_info(self):
        """Print environment information for debugging"""
        print(f"Environment: {'Databricks' if self.is_databricks else 'Local'}")
        print(f"Storage Type: {self.config['storage_type']}")
        print(f"Base Path: {self.config['base_path']}")
        print(f"DBUtils Available: {self.dbutils is not None}")
        print("Layer Paths:")
        for layer in ['bronze', 'silver', 'gold']:
            print(f"  {layer.capitalize()}: {self.config[f'{layer}_path']}")