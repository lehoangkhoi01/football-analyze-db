import logging
import logging.config
from pathlib import Path
import yaml
from typing import Optional, Dict, Any

class DatabricksLogger:
    def __init__(
        self,
        name: str = "databricks",
        config_path: Optional[str] = None,
        defaults: Optional[Dict[str, Any]] = None
    ):
        """
        Args:
            name: Logger name (appears in log messages)
            config_path: Absolute path to YAML config file
            defaults: Fallback config if YAML not found
        """
        self.name = name
        self.config_path = config_path or str(
            Path(__file__).parent / "config/defaults.yaml"
        )
        self.defaults = defaults or self._get_default_config()
        self.logger = self._configure_logger()

    def _get_default_config(self) -> Dict[str, Any]:
        """Fallback configuration if YAML fails"""
        return {
            "version": 1,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "INFO",
                    "formatter": "standard",
                    "stream": "ext://sys.stdout"
                }
            },
            "root": {
                "level": "INFO",
                "handlers": ["console"]
            }
        }

    def _configure_logger(self) -> logging.Logger:
        """Initialize logger with YAML or defaults"""
        try:
            config = self._load_config()
            logging.config.dictConfig(config)
        except Exception as e:
            logging.basicConfig(**self.defaults)
            logging.warning(f"Config load failed, using defaults. Error: {str(e)}")
        return logging.getLogger(self.name)

    def _load_config(self) -> Dict[str, Any]:
        """Load YAML config with path validation"""
        path = Path(self.config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found at {path}")
        
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def get_logger(self) -> logging.Logger:
        """Get the configured logger instance"""
        return self.logger

# Pre-configured instance for quick imports
default_logger = DatabricksLogger().get_logger()