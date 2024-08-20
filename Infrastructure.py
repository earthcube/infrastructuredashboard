import streamlit as st

import utils

st.set_page_config(
    page_title="Status",
    page_icon="👋",
)

st.write("# Welcome to Geocodes! 👋")

st.sidebar.success("Select a Component.")

servers = utils.servers(st.secrets)

for server in servers:
    if utils.graph_status(st.secrets, server):
        st.success(f"# {server} Triplestore  is Up! ", icon="🌈")
    else:
        st.success(f"# {server} Triplestore  is down! ", icon="🔥")
