import docker
import requests
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
