import streamlit as st
import utils
from datetime import datetime, timedelta, timezone
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def get_all_jobs_by_status(server, status, created_after_timestamp=None):
    """Get ALL jobs by status without pipeline name filtering - copied from Scheduler."""
    from string import Template
    
    query_template = '''
    query FilteredRunsQuery($$cursor: String) {
      runsOrError(
        filter: {
          statuses: [${STATUS}]
          ${TIMESTAMP_FILTER}
        }
        cursor: $$cursor
        limit: 50
      ) {
        __typename
        ... on Runs {
          results {
            runId
            jobName
            pipelineName
            status
            runConfigYaml
            startTime
            endTime
            creationTime
          }
        }
      }
    }
    '''

    # Add timestamp filter if provided
    timestamp_filter = f"createdAfter: {created_after_timestamp}" if created_after_timestamp else ""

    query = Template(query_template).safe_substitute(
        STATUS=status,
        TIMESTAMP_FILTER=timestamp_filter
    )

    try:
        data = utils.graph_ql(st.secrets, server, query)
        if data and 'data' in data and 'runsOrError' in data['data']:
            if data['data']['runsOrError'].get('__typename') == 'Runs':
                results = data['data']['runsOrError'].get('results', [])
                logger.info(f"Found {len(results)} {status} jobs (all pipelines)")
                return results
        else:
            logger.warning(f"No valid data returned for {status} jobs query: {data}")
    except Exception as e:
        logger.error(f"Error fetching {status} jobs for {server}: {str(e)}")

    return []

def get_active_sources_count(server):
    """Get count of active sources for a server."""
    try:
        # Get from tenant config (these are the active sources)
        tenant_config = utils.get_tenant_config(st.secrets, server)
        if tenant_config:
            active_sources = utils.extract_sources_from_tenant_config(tenant_config)
            return len(active_sources)
        
        # Fallback to gleaner config
        gleaner_config = utils.get_gleaner_config(st.secrets, server)
        if gleaner_config:
            sources = utils.extract_sources_from_gleaner_config(gleaner_config)
            # Count only active sources
            active_sources = [s for s in sources if s.get('active', True)]
            return len(active_sources)
        
        return 0
    except Exception as e:
        logger.error(f"Error getting active sources count for {server}: {str(e)}")
        return 0

def check_queued_jobs_alert(server):
    """Check if queued jobs exceed active sources and return alert info."""
    try:
        # Get recent timestamp (last hour for current queued jobs)
        now = pd.Timestamp('now', tzinfo=timezone.utc)
        recent_timestamp = int((now - pd.Timedelta(hours=1)).timestamp())
        
        # Get queued jobs count
        queued_jobs = get_all_jobs_by_status(server, "QUEUED", recent_timestamp)
        queued_count = len(queued_jobs)
        
        # Get active sources count
        active_sources_count = get_active_sources_count(server)
        
        alert_info = {
            'queued_jobs': queued_count,
            'active_sources': active_sources_count,
            'has_alert': queued_count > active_sources_count and active_sources_count > 0,
            'ratio': queued_count / max(active_sources_count, 1)
        }
        
        return alert_info
        
    except Exception as e:
        logger.error(f"Error checking queued jobs alert for {server}: {str(e)}")
        return {
            'queued_jobs': 0,
            'active_sources': 0,
            'has_alert': False,
            'ratio': 0,
            'error': str(e)
        }

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

# Check for queued jobs alerts across all servers
st.header("🚨 System Alerts")
alerts_found = False

for server in servers:
    server_name = st.secrets[server].get('NAME', server)
    alert_info = check_queued_jobs_alert(server)
    
    if 'error' in alert_info:
        st.warning(f"⚠️ Could not check alerts for {server_name}: {alert_info['error']}")
        continue
    
    if alert_info['has_alert']:
        alerts_found = True
        ratio = alert_info['ratio']
        
        if ratio >= 3.0:
            alert_level = "🔴 CRITICAL"
            alert_color = "error"
        elif ratio >= 2.0:
            alert_level = "🟠 HIGH"
            alert_color = "error"  
        elif ratio >= 1.5:
            alert_level = "🟡 MEDIUM"
            alert_color = "warning"
        else:
            alert_level = "🟢 LOW"
            alert_color = "warning"
        
        with st.container():
            if alert_color == "error":
                st.error(f"""
**{alert_level} ALERT - {server_name}**

🔄 **Queued Jobs:** {alert_info['queued_jobs']}  
🎯 **Active Sources:** {alert_info['active_sources']}  
📊 **Ratio:** {ratio:.1f}x more queued jobs than sources

**Issue:** Job queue is backing up! This may indicate:
- Scheduler performance issues
- Source processing bottlenecks  
- Resource constraints
- Pipeline failures preventing job completion

**Recommendation:** Check the Scheduler dashboard and investigate running jobs.
                """, icon="🚨")
            else:
                st.warning(f"""
**{alert_level} ALERT - {server_name}**

🔄 **Queued Jobs:** {alert_info['queued_jobs']}  
🎯 **Active Sources:** {alert_info['active_sources']}  
📊 **Ratio:** {ratio:.1f}x more queued jobs than sources

Job queue is elevated but manageable.
                """, icon="⚠️")

if not alerts_found:
    st.success("✅ No queue alerts detected. All systems operating normally.", icon="🟢")

st.divider()

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
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Response Time", f"{health['response_time']:.2f}s")
            with col2:
                st.metric("Status Code", health.get('status_code', 'N/A'))
            with col3:
                st.metric("Last Check", health['timestamp'].strftime("%H:%M:%S"))
            with col4:
                # Show queue status
                alert_info = check_queued_jobs_alert(server)
                if 'error' not in alert_info:
                    if alert_info['has_alert']:
                        st.metric("Queue Status", "⚠️ Alert", delta=f"{alert_info['queued_jobs']} queued")
                    else:
                        st.metric("Queue Status", "✅ OK", delta=f"{alert_info['queued_jobs']} queued")
                else:
                    st.metric("Queue Status", "❓ Unknown")
            
            # Additional metrics row
            col1, col2, col3, col4 = st.columns(4)
            if 'error' not in alert_info:
                with col1:
                    st.metric("Active Sources", alert_info['active_sources'])
                with col2:
                    st.metric("Queued Jobs", alert_info['queued_jobs'])
                with col3:
                    if alert_info['active_sources'] > 0:
                        st.metric("Queue Ratio", f"{alert_info['ratio']:.1f}x")
                    else:
                        st.metric("Queue Ratio", "N/A")
                with col4:
                    # Running jobs for context
                    try:
                        now = pd.Timestamp('now', tzinfo=timezone.utc)
                        recent_timestamp = int((now - pd.Timedelta(hours=1)).timestamp())
                        running_jobs = get_all_jobs_by_status(server, "STARTED", recent_timestamp)
                        st.metric("Running Jobs", len(running_jobs))
                    except:
                        st.metric("Running Jobs", "N/A")
            
            with st.expander("📋 Details"):
                st.code(health['details'])
            
            st.divider()

with tab2:
    st.header("Data Sources Configuration")
    
    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        
        with st.expander(f"📂 {server_name} - Data Sources"):
            try:
                # Add download link for gleanerconfig.yaml
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write("**Configuration File:**")
                with col2:
                    try:
                        client = utils.s3_client(st.secrets, server)
                        bucket = st.secrets[server]["S3_BUCKET"]
                        config_path = st.secrets[server]["S3_GLEANERCONFIG"]
                        
                        # Get the file for download
                        response = client.get_object(bucket, config_path)
                        config_content = response.read()
                        
                        st.download_button(
                            label="📁 Download gleanerconfig.yaml",
                            data=config_content,
                            file_name="gleanerconfig.yaml",
                            mime="application/x-yaml",
                            key=f"download_gleaner_{server}",
                            help="Download the gleaner configuration file"
                        )
                    except Exception as e:
                        st.caption(f"❌ Could not access gleanerconfig.yaml: {str(e)}")
                
                st.divider()
                
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
                # Add download link for tenant.yaml
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write("**Configuration File:**")
                with col2:
                    try:
                        client = utils.s3_client(st.secrets, server)
                        bucket = st.secrets[server]["S3_BUCKET"]
                        config_path = st.secrets[server]["S3_TENNANTCONFIG"]
                        
                        # Get the file for download
                        response = client.get_object(bucket, config_path)
                        config_content = response.read()
                        
                        st.download_button(
                            label="📁 Download tenant.yaml",
                            data=config_content,
                            file_name="tenant.yaml",
                            mime="application/x-yaml",
                            key=f"download_tenant_{server}",
                            help="Download the tenant configuration file"
                        )
                    except Exception as e:
                        st.caption(f"❌ Could not access tenant.yaml: {str(e)}")
                
                st.divider()
                
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
