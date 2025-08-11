import docker
import requests
from dagster_graphql import DagsterGraphQLClient
from minio import Minio
from config import ConfigManager, ServerConfig
import yaml
from datetime import datetime
from typing import Dict, List, Any, Optional

import logging

# Configure logging for the application
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def servers(secrets):
    """Get list of server keys from secrets - legacy function."""
    keys = secrets.keys()
    servers = list(filter(lambda k: k.startswith("GLEANERIO_"), keys))
    return servers


def get_config_manager(secrets) -> ConfigManager:
    """Get configuration manager instance."""
    return ConfigManager(secrets)

# PORTAINER ISSUE:
# api: https://docs.docker.com/engine/api/v1.45/#tag/Service/operation/ServiceUpdate
# list services
# get service logs

# Debug in Postman.
# pass a header with an X-API-Key
# docker.py add a version.
# fails:
# GET https://portainer.geocodes-aws-dev.earthcube.org/api/endpoints/2/docker/v1.43/services

# WORKS:
# GET https://portainer.geocodes-aws-dev.earthcube.org/api/endpoints/2/docker/services
#
# So need to use requests to send the information directly and parse the response

def docker_server_client(secrets, server_key):
    """Create Docker client with Portainer authentication - legacy version."""
    try:
        p_url = secrets[server_key]['PORTAINER_API_URL']
        p_api_key = secrets[server_key]['PORTAINER_API_KEY']
        headers = {'X-API-Key': p_api_key}

        client = docker.DockerClient(base_url=p_url, version="1.43")

        if client.api._general_configs:
            client.api._general_configs["HttpHeaders"] = headers
        else:
            client.api._general_configs = {"HttpHeaders": headers}
        client.api.headers['X-API-Key'] = p_api_key

        logger.info(f"Docker client created for {server_key}")
        return client
    except Exception as e:
        logger.error(f"Error creating docker client for {server_key}: {str(e)}")
        raise


def docker_client_from_config(config: ServerConfig):
    """Create Docker client from configuration object."""
    try:
        headers = {'X-API-Key': config.portainer_api_key}

        client = docker.DockerClient(base_url=config.portainer_api_url, version="1.43")

        if client.api._general_configs:
            client.api._general_configs["HttpHeaders"] = headers
        else:
            client.api._general_configs = {"HttpHeaders": headers}
        client.api.headers['X-API-Key'] = config.portainer_api_key

        logger.info(f"Docker client created for {config.name}")
        return client
    except Exception as e:
        logger.error(f"Error creating docker client for {config.name}: {str(e)}")
        raise


def graph_status(secrets, server_key):
    """Check if the graph server is responding - legacy function."""
    try:
        health = check_triplestore_health(secrets, server_key)
        return health['status'] == 'up'
    except Exception as e:
        logger.error(f"Error in graph_status for {server_key}: {str(e)}")
        return False

def get_dagster_client(secrets, server_key):
    """Get DagsterGraphQLClient for the specified server."""
    try:
        url = secrets[server_key]['DAGSTER_GRAPHQL_URL']
        client = DagsterGraphQLClient(url)
        logger.info(f"DagsterGraphQLClient created for {server_key}")
        return client
    except Exception as e:
        logger.error(f"Error creating DagsterGraphQLClient for {server_key}: {str(e)}")
        raise

def get_run_status(secrets, server_key, run_id):
    """Get run status using DagsterGraphQLClient."""
    try:
        client = get_dagster_client(secrets, server_key)
        status = client.get_run_status(run_id)
        logger.info(f"Got run status for {run_id} from {server_key}: {status}")
        return status
    except Exception as e:
        logger.error(f"Error getting run status for {run_id} from {server_key}: {str(e)}")
        return None

def graph_ql(secrets, server_key, query):
    """Execute custom GraphQL query against Dagster (for queries not supported by client)."""
    try:
        url = f"{secrets[server_key]['DAGSTER_GRAPHQL_URL']}"
        operation = {
            "query": query
        }
        r = requests.post(url, json=operation, timeout=30)
        if r.status_code == 200:
            logger.info(f"GraphQL query successful for {server_key}")
            return r.json()
        else:
            logger.error(f"GraphQL query failed for {server_key}: status {r.status_code}, response: {r.text}")
            return {}
    except Exception as e:
        logger.error(f"Error executing GraphQL query for {server_key}: {str(e)}")
        return {}

def s3_client(secrets, server_key):
    """Create MinIO/S3 client - try public access first, then authentication."""
    s3_endpoint = secrets[server_key]['S3_ADDRESS']
    s3_access_key = secrets[server_key]['S3_ACCESS_KEY']
    s3_secret_key = secrets[server_key]['S3_SECRETS_KEY']
    s3_port = secrets[server_key].get('S3_PORT', secrets[server_key].get('S3__PORT', 443))
    s3_use_ssl = secrets[server_key]['S3_USE_SSL']

    # Try public access first
    try:
        client = Minio(
            f"{s3_endpoint}:{s3_port}",
            secure=s3_use_ssl,
        )
        # Test the connection with a simple operation
        client.bucket_exists(secrets[server_key]['S3_BUCKET'])
        logger.info(f"S3 client created with public access for {server_key}")
        return client
    except Exception as public_error:
        logger.warning(f"S3 public access failed for {server_key}: {str(public_error)}")
        logger.info(f"Attempting authenticated access for {server_key}")

    # Fall back to authenticated access
    if s3_access_key and s3_secret_key:
        try:
            client = Minio(
                f"{s3_endpoint}:{s3_port}",
                access_key=s3_access_key,
                secret_key=s3_secret_key,
                secure=s3_use_ssl,
            )
            # Test the connection
            client.bucket_exists(secrets[server_key]['S3_BUCKET'])
            logger.info(f"S3 client created with authentication for {server_key}")
            return client
        except Exception as auth_error:
            logger.error(f"S3 authentication also failed for {server_key}: {str(auth_error)}")
            raise
    else:
        logger.error(f"No S3 credentials available for {server_key} and public access failed")
        raise Exception("Both public access and authentication failed")


def s3_client_from_config(config: ServerConfig):
    """Create MinIO/S3 client from configuration object - try public access first, then authentication."""
    # Try public access first
    try:
        client = Minio(
            f"{config.s3_endpoint}:{config.s3_port}",
            secure=config.s3_use_ssl,
        )
        # Test the connection with a simple operation
        client.bucket_exists(config.s3_bucket)
        logger.info(f"S3 client created with public access for {config.name}")
        return client
    except Exception as public_error:
        logger.warning(f"S3 public access failed for {config.name}: {str(public_error)}")
        logger.info(f"Attempting authenticated access for {config.name}")

    # Fall back to authenticated access
    if config.s3_access_key and config.s3_secret_key:
        try:
            client = Minio(
                f"{config.s3_endpoint}:{config.s3_port}",
                access_key=config.s3_access_key,
                secret_key=config.s3_secret_key,
                secure=config.s3_use_ssl,
            )
            # Test the connection
            client.bucket_exists(config.s3_bucket)
            logger.info(f"S3 client created with authentication for {config.name}")
            return client
        except Exception as auth_error:
            logger.error(f"S3 authentication also failed for {config.name}: {str(auth_error)}")
            raise
    else:
        logger.error(f"No S3 credentials available for {config.name} and public access failed")
        raise Exception("Both public access and authentication failed")


def get_gleaner_config(secrets, server_key: str) -> Optional[Dict[str, Any]]:
    """Read and parse gleanerconfig.yaml from MinIO."""
    try:
        client = s3_client(secrets, server_key)
        bucket = secrets[server_key]['S3_BUCKET']
        config_path = secrets[server_key]['S3_GLEANERCONFIG']

        # Download the config file
        response = client.get_object(bucket, config_path)
        config_content = response.read().decode('utf-8')

        # Parse YAML
        config_data = yaml.safe_load(config_content)
        logger.info(f"Successfully loaded gleaner config from {server_key}")
        return config_data

    except Exception as e:
        logger.error(f"Error reading gleaner config from {server_key}: {str(e)}")
        return None


def get_tenant_config(secrets, server_key: str) -> Optional[Dict[str, Any]]:
    """Read and parse tenant.yaml from MinIO."""
    try:
        client = s3_client(secrets, server_key)
        bucket = secrets[server_key]['S3_BUCKET']
        config_path = secrets[server_key]['S3_TENNANTCONFIG']

        # Download the config file
        response = client.get_object(bucket, config_path)
        config_content = response.read().decode('utf-8')

        # Parse YAML
        config_data = yaml.safe_load(config_content)
        logger.info(f"Successfully loaded tenant config from {server_key}")
        return config_data

    except Exception as e:
        logger.error(f"Error reading tenant config from {server_key}: {str(e)}")
        return None


def extract_sources_from_gleaner_config(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract source information from gleaner config."""
    sources = []
    try:
        if 'sources' in config:
            sources_data = config['sources']

            # Handle both dict and list formats
            if isinstance(sources_data, dict):
                # Dictionary format: {source_name: config}
                for source_name, source_config in sources_data.items():
                    source_info = {
                        'name': source_name,
                        'url': source_config.get('url', ''),
                        'logo': source_config.get('logo', ''),
                        'description': source_config.get('description', ''),
                        'active': source_config.get('active', True),
                        'type': source_config.get('type', 'unknown')
                    }
                    sources.append(source_info)
            elif isinstance(sources_data, list):
                # List format: [{name: source_name, ...}, ...]
                for source_config in sources_data:
                    if isinstance(source_config, dict):
                        source_info = {
                            'name': source_config.get('name', source_config.get('sourcename', 'Unknown')),
                            'url': source_config.get('url', source_config.get('domain', '')),
                            'logo': source_config.get('logo', ''),
                            'description': source_config.get('description', source_config.get('desc', '')),
                            'active': source_config.get('active', True),
                            'type': source_config.get('type', source_config.get('headless', False) and 'headless' or 'standard')
                        }
                        sources.append(source_info)
        logger.info(f"Extracted {len(sources)} sources from gleaner config")
    except Exception as e:
        logger.error(f"Error extracting sources from config: {str(e)}")

    return sources


def extract_sources_from_tenant_config(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract active source information from tenant config."""
    sources = []
    try:
        # Handle different possible structures
        tenant_data = None

        if 'tenant' in config:
            tenant_data = config['tenant']
        elif 'tenants' in config:
            tenant_data = config['tenants']

        if tenant_data:
            if isinstance(tenant_data, list):
                # List format: [{community: name, sources: [...]}]
                for tenant_config in tenant_data:
                    if isinstance(tenant_config, dict):
                        tenant_name = tenant_config.get('community', tenant_config.get('name', 'Unknown'))
                        tenant_sources = tenant_config.get('sources', [])
                        for source in tenant_sources:
                            source_info = {
                                'name': source,
                                'tenant': tenant_name,
                                'active': True
                            }
                            sources.append(source_info)
            elif isinstance(tenant_data, dict):
                # Dict format: {tenant_name: {sources: [...]}}
                for tenant_name, tenant_config in tenant_data.items():
                    if isinstance(tenant_config, dict):
                        tenant_sources = tenant_config.get('sources', [])
                        for source in tenant_sources:
                            source_info = {
                                'name': source,
                                'tenant': tenant_name,
                                'active': True
                            }
                            sources.append(source_info)

        logger.info(f"Extracted {len(sources)} active sources from tenant config")
    except Exception as e:
        logger.error(f"Error extracting sources from tenant config: {str(e)}")

    return sources


def check_triplestore_health(secrets, server_key: str) -> Dict[str, Any]:
    """Enhanced triplestore health check with timestamp and details."""
    start_time = datetime.now()

    try:
        url = f"{secrets[server_key]['GRAPH_SERVER_URL']}/status?showQueries"
        r = requests.get(url, timeout=10)

        end_time = datetime.now()
        response_time = (end_time - start_time).total_seconds()

        if r.status_code == 200:
            logger.info(f"Graph server {server_key} is responding in {response_time:.2f}s")
            return {
                'status': 'up',
                'response_time': response_time,
                'timestamp': end_time,
                'status_code': r.status_code,
                'details': r.text[:200] if r.text else 'No response body'
            }
        else:
            logger.warning(f"Graph server {server_key} returned status {r.status_code}")
            return {
                'status': 'degraded',
                'response_time': response_time,
                'timestamp': end_time,
                'status_code': r.status_code,
                'details': f"HTTP {r.status_code}"
            }
    except Exception as e:
        end_time = datetime.now()
        response_time = (end_time - start_time).total_seconds()
        logger.error(f"Error checking graph status for {server_key}: {str(e)}")
        return {
            'status': 'down',
            'response_time': response_time,
            'timestamp': end_time,
            'status_code': None,
            'details': str(e)
        }


# Portainer API functions using requests (avoiding docker-py issues)
def portainer_api_request(secrets, server_key: str, endpoint: str, method: str = 'GET') -> Optional[Dict[str, Any]]:
    """Make authenticated request to Portainer API using requests."""
    try:
        base_url = secrets[server_key]['PORTAINER_API_URL']
        api_key = secrets[server_key]['PORTAINER_API_KEY']

        # Ensure the base URL ends with /docker/ for Docker API endpoints
        if not base_url.endswith('/docker/'):
            if base_url.endswith('/docker'):
                base_url += '/'
            elif base_url.endswith('/'):
                base_url += 'docker/'
            else:
                base_url += '/docker/'

        url = f"{base_url}{endpoint.lstrip('/')}"
        headers = {'X-API-Key': api_key}

        response = requests.request(method, url, headers=headers, timeout=30)

        if response.status_code == 200:
            logger.info(f"Portainer API request successful: {method} {endpoint}")
            return response.json()
        else:
            logger.error(f"Portainer API request failed: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        logger.error(f"Error making Portainer API request to {server_key}: {str(e)}")
        return None


def get_portainer_services(secrets, server_key: str) -> List[Dict[str, Any]]:
    """Get all services from Portainer API."""
    services_data = portainer_api_request(secrets, server_key, 'services')
    if services_data:
        # Filter for gleaner scheduler services
        gleaner_services = [s for s in services_data if s.get('Spec', {}).get('Name', '').startswith('sch_')]
        logger.info(f"Found {len(gleaner_services)} gleaner services out of {len(services_data)} total services")
        return gleaner_services
    return []


def get_service_details(secrets, server_key: str, service_id: str) -> Optional[Dict[str, Any]]:
    """Get detailed information about a specific service."""
    return portainer_api_request(secrets, server_key, f'services/{service_id}')


def get_service_tasks(secrets, server_key: str, service_name: str) -> List[Dict[str, Any]]:
    """Get tasks for a specific service."""
    tasks_data = portainer_api_request(secrets, server_key, 'tasks')
    if tasks_data:
        # Filter tasks for the specific service
        service_tasks = [t for t in tasks_data if t.get('ServiceID') and
                        t.get('Spec', {}).get('ContainerSpec', {}).get('Image', '').find(service_name) >= 0]
        return service_tasks
    return []


def get_container_stats(secrets, server_key: str, container_id: str) -> Optional[Dict[str, Any]]:
    """Get real-time stats for a container."""
    return portainer_api_request(secrets, server_key, f'containers/{container_id}/stats?stream=false')


def get_service_resource_usage(secrets, server_key: str, service_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate resource usage for a service based on its tasks."""
    resource_usage = {
        'cpu_usage': 0.0,
        'memory_usage': 0,
        'memory_limit': 0,
        'network_io': {'rx_bytes': 0, 'tx_bytes': 0},
        'block_io': {'read_bytes': 0, 'write_bytes': 0},
        'container_count': 0,
        'active_containers': 0
    }

    try:
        tasks = service_data.get('Tasks', [])
        resource_usage['container_count'] = len(tasks)

        for task in tasks:
            if task.get('Status', {}).get('State') == 'running':
                resource_usage['active_containers'] += 1

                # Get container stats if available
                container_id = task.get('Status', {}).get('ContainerStatus', {}).get('ContainerID')
                if container_id:
                    stats = get_container_stats(secrets, server_key, container_id)
                    if stats:
                        # Parse CPU usage
                        cpu_stats = stats.get('cpu_stats', {})
                        precpu_stats = stats.get('precpu_stats', {})

                        if cpu_stats and precpu_stats:
                            cpu_usage = calculate_cpu_percentage(cpu_stats, precpu_stats)
                            resource_usage['cpu_usage'] += cpu_usage

                        # Parse memory usage
                        memory_stats = stats.get('memory_stats', {})
                        if memory_stats:
                            resource_usage['memory_usage'] += memory_stats.get('usage', 0)
                            resource_usage['memory_limit'] += memory_stats.get('limit', 0)

                        # Parse network I/O
                        networks = stats.get('networks', {})
                        for network_data in networks.values():
                            resource_usage['network_io']['rx_bytes'] += network_data.get('rx_bytes', 0)
                            resource_usage['network_io']['tx_bytes'] += network_data.get('tx_bytes', 0)

                        # Parse block I/O
                        blkio_stats = stats.get('blkio_stats', {})
                        io_service_bytes_recursive = blkio_stats.get('io_service_bytes_recursive', [])
                        for io_stat in io_service_bytes_recursive:
                            if io_stat.get('op') == 'Read':
                                resource_usage['block_io']['read_bytes'] += io_stat.get('value', 0)
                            elif io_stat.get('op') == 'Write':
                                resource_usage['block_io']['write_bytes'] += io_stat.get('value', 0)

        logger.info(f"Calculated resource usage for service with {resource_usage['active_containers']} active containers")

    except Exception as e:
        logger.error(f"Error calculating service resource usage: {str(e)}")

    return resource_usage


def calculate_cpu_percentage(cpu_stats: Dict, precpu_stats: Dict) -> float:
    """Calculate CPU usage percentage from Docker stats."""
    try:
        cpu_delta = cpu_stats.get('cpu_usage', {}).get('total_usage', 0) - precpu_stats.get('cpu_usage', {}).get('total_usage', 0)
        system_delta = cpu_stats.get('system_cpu_usage', 0) - precpu_stats.get('system_cpu_usage', 0)

        if system_delta > 0 and cpu_delta > 0:
            cpu_count = len(cpu_stats.get('cpu_usage', {}).get('percpu_usage', [1]))
            return (cpu_delta / system_delta) * cpu_count * 100.0
    except Exception as e:
        logger.error(f"Error calculating CPU percentage: {str(e)}")

    return 0.0


def get_service_dependencies(secrets, server_key: str) -> Dict[str, List[str]]:
    """Map service dependencies based on labels and network connections."""
    dependencies = {}

    try:
        # Get all services
        services_data = portainer_api_request(secrets, server_key, 'services')
        if not services_data:
            return dependencies

        # Build dependency map
        for service in services_data:
            service_name = service.get('Spec', {}).get('Name', '')
            if service_name.startswith('sch_'):
                deps = []

                # Check labels for dependencies
                labels = service.get('Spec', {}).get('Labels', {})
                if 'gleaner.depends_on' in labels:
                    deps.extend(labels['gleaner.depends_on'].split(','))

                # Check networks for connections
                networks = service.get('Spec', {}).get('TaskTemplate', {}).get('Networks', [])
                for network in networks:
                    network_name = network.get('Target', '')
                    if network_name:
                        # Find other services on the same network
                        for other_service in services_data:
                            other_name = other_service.get('Spec', {}).get('Name', '')
                            if other_name != service_name:
                                other_networks = other_service.get('Spec', {}).get('TaskTemplate', {}).get('Networks', [])
                                for other_network in other_networks:
                                    if other_network.get('Target') == network_name:
                                        if other_name not in deps:
                                            deps.append(other_name)

                dependencies[service_name] = deps

        logger.info(f"Mapped dependencies for {len(dependencies)} services")

    except Exception as e:
        logger.error(f"Error mapping service dependencies: {str(e)}")

    return dependencies


def format_bytes(bytes_value: int) -> str:
    """Format bytes into human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"


def parse_service_info(service_data: Dict[str, Any]) -> Dict[str, Any]:
    """Parse service data into a standardized format."""
    try:
        spec = service_data.get('Spec', {})
        status = service_data.get('UpdateStatus', {})

        return {
            'id': service_data.get('ID', ''),
            'name': spec.get('Name', 'Unknown'),
            'image': spec.get('TaskTemplate', {}).get('ContainerSpec', {}).get('Image', ''),
            'replicas': {
                'desired': spec.get('Mode', {}).get('Replicated', {}).get('Replicas', 0),
                'running': len([t for t in service_data.get('Tasks', []) if t.get('Status', {}).get('State') == 'running'])
            },
            'created': service_data.get('CreatedAt', ''),
            'updated': service_data.get('UpdatedAt', ''),
            'status': status.get('State', 'unknown'),
            'labels': spec.get('Labels', {}),
            'tasks': service_data.get('Tasks', [])
        }
    except Exception as e:
        logger.error(f"Error parsing service info: {str(e)}")
        return {}


def parse_task_info(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """Parse task data into a standardized format."""
    try:
        status = task_data.get('Status', {})
        container_status = status.get('ContainerStatus', {})

        return {
            'id': task_data.get('ID', ''),
            'service_id': task_data.get('ServiceID', ''),
            'node_id': task_data.get('NodeID', ''),
            'state': status.get('State', 'unknown'),
            'message': status.get('Message', ''),
            'timestamp': status.get('Timestamp', ''),
            'container_id': container_status.get('ContainerID', ''),
            'pid': container_status.get('PID', 0),
            'exit_code': container_status.get('ExitCode', 0),
            'created_at': task_data.get('CreatedAt', ''),
            'updated_at': task_data.get('UpdatedAt', '')
        }
    except Exception as e:
        logger.error(f"Error parsing task info: {str(e)}")
        return {}
