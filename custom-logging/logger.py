import logging
import logging.config
import yaml
from pathlib import Path
from typing import Optional

class DatabricksLogger:
    def __init__(self, name: str = "databricks", config_path: Optional[str] = None):
        """
        Args:
            name: Logger name (shows in logs)
            config_path: Path to YAML config (defaults to package defaults)
        """
        self.name = name
        self.config_path = config_path or str(Path(__file__).parent) / "config/defaults.yaml"
        self._configure_logging()

    def _configure_logging(self) -> None:
        """Load config from YAML and set up logging."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            logging.config.dictConfig(config)
        except Exception as e:
            # Fallback to basic config
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            logging.warning(f"Failed to load logging config: {e}. Using defaults.")

    def get_logger(self) -> logging.Logger:
        """Returns a configured logger instance."""
        return logging.getLogger(self.name)