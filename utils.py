import docker
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
