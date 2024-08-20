import streamlit as st
import utils
import datetime
st.write("# Presently Issues with a Non Environment Admin (aka read only)")

servers  = utils.servers(st.secrets)

print([k for k in servers])

# just using 'GLEANER_aws_dev'


for server in servers:
    client = utils.docker_server_client(st.secrets,server )
    services = client.services.list()
    services = list(filter(lambda s: s.name.startswith("sch_"), services ))
    if len(services) == 0:
        st.success(f"# {server} no running gleaner schedule containers! ", icon="🌈")
    else:
        for s in services:
            st.write(f"# server {server}")
            c1= st.container(height=200, border=2)
            service_name = s.name
            c1.write(f"## service {service_name}")
            for t in s.tasks():
                # for debugging
                # with c1.expander(f"{server} Service ID id:{s.id}"):
                #     c1.write(t)
                state = t["Status"]["State"]
                message = t["Status"]["Message"]
                timestamp = t["Status"]["Timestamp"]
                started = datetime.datetime.fromisoformat(timestamp)
                c1.write(f"running since {started.isoformat()}")
                if state == "running":
                    c1.success(f"container running")
                else:
                    c1.info(f"container issue")
                    c1.write(message)

