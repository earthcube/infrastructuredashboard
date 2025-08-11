"""Configuration classes for infrastructure dashboard."""

from dataclasses import dataclass
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


@dataclass
class ServerConfig:
    """Configuration for a single server/environment."""
    name: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_port: int
    s3_use_ssl: bool
    s3_bucket: str
    portainer_api_url: str
    portainer_api_key: str
    graph_server_url: str
    dagster_graphql_url: str
    dagster_ingest_prefixes: list = None

    def __post_init__(self):
        if self.dagster_ingest_prefixes is None:
            self.dagster_ingest_prefixes = []
    
    def has_s3_credentials(self) -> bool:
        """Check if S3 credentials are available."""
        return bool(self.s3_access_key and self.s3_secret_key)


class ConfigManager:
    """Manages server configurations from Streamlit secrets."""
    
    def __init__(self, secrets: Dict[str, Any]):
        self.secrets = secrets
        self._servers = {}
        self._load_servers()
    
    def _load_servers(self):
        """Load server configurations from secrets."""
        for key in self.secrets.keys():
            if key.startswith("GLEANERIO_"):
                try:
                    server_secrets = self.secrets[key]
                    config = ServerConfig(
                        name=server_secrets.get("NAME", key),
                        s3_endpoint=server_secrets["S3_ADDRESS"],
                        s3_access_key=server_secrets["S3_ACCESS_KEY"], 
                        s3_secret_key=server_secrets["S3_SECRETS_KEY"],
                        s3_port=server_secrets.get("S3_PORT", server_secrets.get("S3__PORT", 443)),
                        s3_use_ssl=server_secrets["S3_USE_SSL"],
                        s3_bucket=server_secrets["S3_BUCKET"],
                        portainer_api_url=server_secrets["PORTAINER_API_URL"],
                        portainer_api_key=server_secrets["PORTAINER_API_KEY"],
                        graph_server_url=server_secrets["GRAPH_SERVER_URL"],
                        dagster_graphql_url=server_secrets["DAGSTER_GRAPHQL_URL"],
                        dagster_ingest_prefixes=server_secrets.get("DAGSTER_INGEST_PREFIXES", [])
                    )
                    self._servers[key] = config
                    logger.info(f"Loaded configuration for server: {key}")
                except KeyError as e:
                    logger.error(f"Missing required configuration key for {key}: {e}")
                except Exception as e:
                    logger.error(f"Error loading configuration for {key}: {e}")
    
    def get_server_config(self, server_key: str) -> ServerConfig:
        """Get configuration for a specific server."""
        if server_key not in self._servers:
            raise ValueError(f"Server configuration not found: {server_key}")
        return self._servers[server_key]
    
    def get_server_keys(self) -> list:
        """Get list of configured server keys."""
        return list(self._servers.keys())
    
    def validate_config(self) -> bool:
        """Validate all server configurations."""
        if not self._servers:
            logger.error("No server configurations found")
            return False
        
        valid = True
        for server_key, config in self._servers.items():
            # Check required fields are not empty (credentials are optional for S3 public access)
            required_fields = [
                's3_endpoint', 'portainer_api_url', 'portainer_api_key', 
                'graph_server_url', 'dagster_graphql_url'
            ]
            
            for field in required_fields:
                if not getattr(config, field):
                    logger.error(f"Empty required field '{field}' in {server_key}")
                    valid = False
        
        return valid