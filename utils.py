import docker
import requests
from dagster_graphql import DagsterGraphQLClient
from minio import Minio
def servers(secrets ):
    keys = secrets.keys()
    servers = list( filter(lambda k: k.startswith("GLEANER_"), keys) )
    return servers

# docker
# api: https://docs.docker.com/engine/api/v1.45/#tag/Service/operation/ServiceUpdate
# list services
# get service logs

def docker_server_client(secrets, server_key ):
    p_url = secrets[server_key].PORTAINER_API_URL
    p_api_key = secrets[server_key].PORTAINER_API_KEY
    headers = {'X-API-Key': p_api_key}
    client = docker.DockerClient(base_url=p_url, version="1.43")
    # client = docker.APIClient(base_url=URL, version="1.35")
  #  get_dagster_logger().info(f"create docker client")
    if (client.api._general_configs):
        client.api._general_configs["HttpHeaders"] = headers
    else:
        client.api._general_configs = {"HttpHeaders": headers}
    client.api.headers['X-API-Key'] = p_api_key
   # get_dagster_logger().info(f" docker version {client.version()}")

    return client


def graph_status(secrets,server_key ):
    url = f"{secrets[server_key].GRAPH_SERVER_URL}/status?showQueries"
    r = requests.get(url)
    if r.status_code == 200:
       # get_dagster_logger().info(f'graph load response: {str(r.text)} ')
        # '<?xml version="1.0"?><data modified="0" milliseconds="7"/>'
       return True
    else:
       return False

# def graphql_client(secrets,server_key ):
#     client = DagsterGraphQLClient(secrets[server_key].GRAPH_SERVER_URL)
#     return client

def graph_ql(secrets,server_key, query ):
    url = f"{secrets[server_key].DAGSTER_GRAPHQL_URL}"
    operation = {"operationName":"FilteredRunsQuery",
               "variables":{},
               "query":f"{query}"}
    r = requests.post(url, json=operation)
    if r.status_code == 200:
        # get_dagster_logger().info(f'graph load response: {str(r.text)} ')
        # '<?xml version="1.0"?><data modified="0" milliseconds="7"/>'
        return r.json()
    else:
        return {}

def s3_client(secrets,server_key ):
    #S3_SECRETS_KEY = "7d775d3ff3b2477099872570d0067077"
    #S3_ACCESS_KEY = "aa58777b74cc48af932797762a22e8cc"
    #S3_ENDPOINT = "oss.geocodes-aws-dev.earthcube.org"
    #S3__PORT = 443
    #S3__USE_SSL = true
    #S3__BUCKET = "test"
    s3_endpoint = secrets[server_key].S3_ENDPOINT
    s3_access_key = secrets[server_key].S3_ACCESS_KEY
    s3_secret_key = secrets[server_key].S3_SECRETS_KEY
    s3_port = secrets[server_key].S3_PORT
    s3_use_ssl = secrets[server_key].S3_USE_SSL
    s3_bucket = secrets[server_key].S3_BUCKET

    client = Minio(
        f"{s3_endpoint}:{s3_port}",
#        access_key=s3_access_key,
#        secret_key=s3_secret_key,
        secure=s3_use_ssl,
    )

    return client
