import streamlit as st
import utils
import datetime

st.write("# 🐳 Gleaner Ingest In Progress")
st.info("Monitoring Docker containers via enhanced Portainer API integration")

servers = utils.servers(st.secrets)

if not servers:
    st.error("No GLEANERIO_ servers found in configuration!")
    st.stop()

# Create tabs for different views
tab1, tab2 = st.tabs(["🔄 Active Services", "📊 Resource Monitoring"])

with tab1:
    st.header("Active Gleaner Services")
    
    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        
        with st.expander(f"🖥️ {server_name}", expanded=True):
            try:
                # Get services using new requests-based API
                services = utils.get_portainer_services(st.secrets, server)
                
                if not services:
                    st.success("✅ No running gleaner schedule containers!", icon="🌈")
                    continue
                
                st.write(f"**Found {len(services)} active services:**")
                
                for service_data in services:
                    service_info = utils.parse_service_info(service_data)
                    
                    if not service_info:
                        continue
                        
                    # Service header
                    col1, col2, col3 = st.columns([2, 1, 1])
                    
                    with col1:
                        st.subheader(f"🔧 {service_info['name']}")
                        st.caption(f"Image: {service_info['image']}")
                    
                    with col2:
                        if service_info['status'] in ['complete', 'running']:
                            st.success("✅ Running", icon="🟢")
                        else:
                            st.warning(f"⚠️ {service_info['status']}", icon="🟡")
                    
                    with col3:
                        desired = service_info['replicas']['desired']
                        running = service_info['replicas']['running']
                        st.metric("Replicas", f"{running}/{desired}")
                    
                    # Service details
                    if service_info['created']:
                        try:
                            created_time = datetime.datetime.fromisoformat(service_info['created'].replace('Z', '+00:00'))
                            st.caption(f"🕒 Created: {created_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                        except:
                            st.caption(f"🕒 Created: {service_info['created']}")
                    
                    # Get and display tasks
                    tasks = service_data.get('Tasks', [])
                    if tasks:
                        st.write("**Task Status:**")
                        
                        for task_data in tasks[-3:]:  # Show last 3 tasks
                            task_info = utils.parse_task_info(task_data)
                            
                            task_col1, task_col2, task_col3 = st.columns([2, 1, 1])
                            
                            with task_col1:
                                if task_info['timestamp']:
                                    try:
                                        task_time = datetime.datetime.fromisoformat(task_info['timestamp'].replace('Z', '+00:00'))
                                        st.write(f"Task: {task_info['id'][:12]}...")
                                        st.caption(f"Since: {task_time.strftime('%H:%M:%S')}")
                                    except:
                                        st.write(f"Task: {task_info['id'][:12]}...")
                                        st.caption(f"Since: {task_info['timestamp']}")
                            
                            with task_col2:
                                state = task_info['state']
                                if state == "running":
                                    st.success("Running", icon="✅")
                                elif state == "complete":
                                    st.info("Complete", icon="ℹ️")
                                elif state in ["failed", "rejected"]:
                                    st.error(f"{state.title()}", icon="❌")
                                else:
                                    st.warning(f"{state.title()}", icon="⚠️")
                            
                            with task_col3:
                                if task_info['container_id']:
                                    st.caption(f"Container: {task_info['container_id'][:12]}...")
                                if task_info['exit_code'] != 0:
                                    st.caption(f"Exit: {task_info['exit_code']}")
                            
                            if task_info['message'] and task_info['state'] != 'running':
                                st.caption(f"💬 {task_info['message']}")
                    
                    st.divider()
                    
            except Exception as e:
                st.error(f"❌ Error connecting to {server_name}: {str(e)}")

with tab2:
    st.header("Resource Monitoring")
    st.info("🚧 Container resource monitoring (CPU, memory) - Coming soon!")
    
    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        
        with st.expander(f"📊 {server_name} Resources"):
            st.write("**Planned Features:**")
            st.write("• Real-time CPU and memory usage")
            st.write("• Historical resource trends")
            st.write("• Resource utilization alerts")
            st.write("• Container performance metrics")
            
            # Placeholder for future resource monitoring
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("CPU Usage", "N/A", help="Coming soon")
            with col2:
                st.metric("Memory Usage", "N/A", help="Coming soon")  
            with col3:
                st.metric("Network I/O", "N/A", help="Coming soon")