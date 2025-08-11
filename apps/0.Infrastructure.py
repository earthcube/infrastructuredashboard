import streamlit as st
import utils
from datetime import datetime

st.set_page_config(
    page_title="Infrastructure",
    page_icon="🏗️",
)

st.write("# Infrastructure Dashboard 🏗️")
st.write("Monitor EarthCube/Geocodes infrastructure components and data sources")

servers = utils.servers(st.secrets)

if not servers:
    st.error("No GLEANERIO_ servers found in configuration!")
    st.stop()

# Create tabs for different views
tab1, tab2, tab3 = st.tabs(["🔧 Services Health", "📊 Data Sources", "🗂️ Active Sources"])

with tab1:
    st.header("Service Health Monitoring")
    
    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        
        with st.container():
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.subheader(f"🖥️ {server_name}")
            
            # Check triplestore health
            health = utils.check_triplestore_health(st.secrets, server)
            
            with col2:
                if health['status'] == 'up':
                    st.success(f"✅ Online", icon="🟢")
                elif health['status'] == 'degraded':
                    st.warning(f"⚠️ Issues", icon="🟡")
                else:
                    st.error(f"❌ Down", icon="🔴")
            
            # Show details
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Response Time", f"{health['response_time']:.2f}s")
            with col2:
                st.metric("Status Code", health.get('status_code', 'N/A'))
            with col3:
                st.metric("Last Check", health['timestamp'].strftime("%H:%M:%S"))
            
            with st.expander("📋 Details"):
                st.code(health['details'])
            
            st.divider()

with tab2:
    st.header("Data Sources Configuration")
    
    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        
        with st.expander(f"📂 {server_name} - Data Sources"):
            try:
                # Get gleaner config
                gleaner_config = utils.get_gleaner_config(st.secrets, server)
                
                if gleaner_config:
                    sources = utils.extract_sources_from_gleaner_config(gleaner_config)
                    
                    if sources:
                        st.write(f"**Found {len(sources)} configured sources:**")
                        
                        for source in sources:
                            col1, col2, col3 = st.columns([2, 1, 1])
                            
                            with col1:
                                st.write(f"**{source['name']}**")
                                if source['description']:
                                    st.caption(source['description'])
                                if source['url']:
                                    st.link_button("🔗 Visit Source", source['url'])
                            
                            with col2:
                                if source['active']:
                                    st.success("Active", icon="✅")
                                else:
                                    st.error("Inactive", icon="❌")
                            
                            with col3:
                                st.caption(f"Type: {source['type']}")
                            
                            st.divider()
                    else:
                        st.info("No sources found in gleaner configuration")
                else:
                    st.error("Could not load gleaner configuration")
                    
            except Exception as e:
                st.error(f"Error loading data sources: {str(e)}")

with tab3:
    st.header("Active Sources by Tenant")
    
    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        
        with st.expander(f"🎯 {server_name} - Active Sources"):
            try:
                # Get tenant config
                tenant_config = utils.get_tenant_config(st.secrets, server)
                
                if tenant_config:
                    active_sources = utils.extract_sources_from_tenant_config(tenant_config)
                    
                    if active_sources:
                        st.write(f"**Found {len(active_sources)} active sources:**")
                        
                        # Group by tenant
                        tenants = {}
                        for source in active_sources:
                            tenant = source['tenant']
                            if tenant not in tenants:
                                tenants[tenant] = []
                            tenants[tenant].append(source['name'])
                        
                        for tenant, sources in tenants.items():
                            st.write(f"**Tenant: {tenant}**")
                            for source in sources:
                                col1, col2 = st.columns([3, 1])
                                with col1:
                                    st.write(f"• {source}")
                                with col2:
                                    if st.button(f"📊 View Report", key=f"report_{server}_{tenant}_{source}"):
                                        st.info(f"Report functionality for {source} coming soon...")
                            st.divider()
                    else:
                        st.info("No active sources found in tenant configuration")
                else:
                    st.error("Could not load tenant configuration")
                    
            except Exception as e:
                st.error(f"Error loading active sources: {str(e)}")
