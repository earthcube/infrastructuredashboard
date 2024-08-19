import streamlit as st
import utils

servers  = utils.servers(st.secrets)

print([k for k in servers])

# just using 'GLEANER_aws_dev'

client = utils.docker_server_client(st.secrets,'GLEANER_aws_dev' )

services = client.services.list()


for s in services:
 #   if s.name.startswith('sch_'):
        st.write(f"id:{s.id}")
        for t in s.tasks():
            st.write(t)
