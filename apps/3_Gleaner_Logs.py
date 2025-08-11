from datetime import datetime, timedelta, timezone
import streamlit as st
import pandas as pd
import utils
import re
import io
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Configuration
LOG_PATH = 'scheduler/logs/'
PREVIEW_LINES = 50  # Number of lines to show in preview
MAX_LOG_SIZE_MB = 10  # Maximum log size to allow full download

def get_filter_timestamps():
    """Get timestamps for filtering logs by time period."""
    now = pd.Timestamp('now', tzinfo=timezone.utc).floor('D')
    return {
        'today': now,
        'yesterday': now + pd.Timedelta(-1, unit='D'),
        'last_week': now + pd.Timedelta(-7, unit='D'),
        'last_month': now + pd.Timedelta(-30, unit='D')
    }

def get_known_sources(servers, secrets) -> Dict[str, List[str]]:
    """Get all known sources from gleaner configurations."""
    all_sources = {}
    
    for server in servers:
        sources = []
        try:
            # Get sources from gleaner config
            gleaner_config = utils.get_gleaner_config(secrets, server)
            if gleaner_config:
                gleaner_sources = utils.extract_sources_from_gleaner_config(gleaner_config)
                sources.extend([s['name'] for s in gleaner_sources])
            
            # Get active sources from tenant config
            tenant_config = utils.get_tenant_config(secrets, server)
            if tenant_config:
                tenant_sources = utils.extract_sources_from_tenant_config(tenant_config)
                sources.extend([s['name'] for s in tenant_sources])
            
            # Remove duplicates and sort
            all_sources[server] = sorted(list(set(sources)))
            
        except Exception as e:
            logger.error(f"Error getting sources for {server}: {str(e)}")
            all_sources[server] = []
    
    return all_sources

def parse_log_filename(filename: str, known_sources: List[str] = None) -> Dict[str, Any]:
    """Parse log filename to extract source, service, and date information."""
    info = {
        'source': 'unknown',
        'service': 'unknown',
        'date': None,
        'log_type': 'general',
        'original_name': filename,
        'timestamp': None
    }
    
    # Remove path prefix
    basename = filename.replace(LOG_PATH, '').split('/')[-1].lower()
    
    # Try to extract source name from known sources
    if known_sources:
        for source in known_sources:
            source_lower = source.lower()
            # Check if source name appears in filename
            if source_lower in basename:
                info['source'] = source
                break
        
        # Additional patterns for source extraction
        if info['source'] == 'unknown':
            # Try common source name patterns
            source_patterns = [
                r'([a-zA-Z0-9_]+)[\._-](?:gleaner|nabu|summon|scheduler)',
                r'([a-zA-Z0-9_]+)[\._-](?:log|error|debug)',
                r'^([a-zA-Z0-9_]+)[\._-]',  # Start of filename
            ]
            
            for pattern in source_patterns:
                match = re.search(pattern, basename)
                if match:
                    potential_source = match.group(1)
                    # Check if this matches any known source (fuzzy match)
                    for source in known_sources:
                        if potential_source in source.lower() or source.lower() in potential_source:
                            info['source'] = source
                            break
                    if info['source'] != 'unknown':
                        break
    
    # Try to extract service name patterns
    if 'gleaner' in basename:
        info['service'] = 'gleaner'
    elif 'nabu' in basename:
        info['service'] = 'nabu'
    elif 'scheduler' in basename:
        info['service'] = 'scheduler'
    elif 'summon' in basename:
        info['service'] = 'summon'
    elif 'release' in basename:
        info['service'] = 'release'
    
    # Try to extract date from filename
    date_patterns = [
        r'(\d{4}-\d{2}-\d{2})',
        r'(\d{4}_\d{2}_\d{2})',
        r'(\d{8})',
        r'(\d{4}\d{2}\d{2})'
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, basename)
        if match:
            date_str = match.group(1)
            try:
                if '_' in date_str:
                    info['date'] = datetime.strptime(date_str, '%Y_%m_%d').date()
                elif len(date_str) == 8:
                    info['date'] = datetime.strptime(date_str, '%Y%m%d').date()
                else:
                    info['date'] = datetime.strptime(date_str, '%Y-%m-%d').date()
                break
            except:
                continue
    
    # Try to extract timestamp (more precise than date)
    timestamp_patterns = [
        r'(\d{4}-\d{2}-\d{2}[T_]\d{2}[-_:]\d{2}[-_:]\d{2})',
        r'(\d{4}\d{2}\d{2}[T_]\d{6})',
    ]
    
    for pattern in timestamp_patterns:
        match = re.search(pattern, basename)
        if match:
            ts_str = match.group(1).replace('_', '-').replace('-', '-', 1).replace('-', ':')
            try:
                if 'T' in ts_str:
                    info['timestamp'] = datetime.fromisoformat(ts_str.replace('T', ' '))
                else:
                    # Handle various timestamp formats
                    ts_str = ts_str.replace('_', ' ')
                    info['timestamp'] = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
                break
            except:
                continue
    
    # Determine log type
    if any(x in basename for x in ['error', 'err', 'exception']):
        info['log_type'] = 'error'
    elif any(x in basename for x in ['access', 'request', 'http']):
        info['log_type'] = 'access'
    elif any(x in basename for x in ['debug', 'trace', 'verbose']):
        info['log_type'] = 'debug'
    elif any(x in basename for x in ['info', 'information']):
        info['log_type'] = 'info'
    elif any(x in basename for x in ['warn', 'warning']):
        info['log_type'] = 'warning'
    
    return info

def get_log_preview(client, bucket: str, log_path: str, lines: int = PREVIEW_LINES) -> str:
    """Get a preview of log content without downloading the entire file."""
    try:
        response = client.get_object(bucket, log_path)
        content = response.read()
        
        # Handle different encodings
        try:
            text_content = content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                text_content = content.decode('latin-1')
            except UnicodeDecodeError:
                text_content = content.decode('utf-8', errors='ignore')
        
        # Split into lines and take first N lines
        all_lines = text_content.split('\n')
        preview_lines = all_lines[:lines]
        
        preview = '\n'.join(preview_lines)
        if len(all_lines) > lines:
            preview += f"\n\n... (showing first {lines} lines of {len(all_lines)} total lines)"
        
        return preview
        
    except Exception as e:
        logger.error(f"Error getting log preview for {log_path}: {str(e)}")
        return f"Error reading log: {str(e)}"

def search_log_content(client, bucket: str, log_path: str, search_term: str, max_results: int = 100) -> List[str]:
    """Search for specific content within a log file."""
    try:
        response = client.get_object(bucket, log_path)
        content = response.read()
        
        try:
            text_content = content.decode('utf-8')
        except UnicodeDecodeError:
            text_content = content.decode('utf-8', errors='ignore')
        
        lines = text_content.split('\n')
        matching_lines = []
        
        search_lower = search_term.lower()
        for i, line in enumerate(lines, 1):
            if search_lower in line.lower():
                matching_lines.append(f"Line {i}: {line}")
                if len(matching_lines) >= max_results:
                    matching_lines.append(f"... (stopped after {max_results} matches)")
                    break
        
        return matching_lines
        
    except Exception as e:
        logger.error(f"Error searching log content for {log_path}: {str(e)}")
        return [f"Error searching log: {str(e)}"]

def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

# Main UI
st.write("# 📋 Enhanced Log Management")
st.info("Advanced log viewing with search, filtering, and preview capabilities")

servers = utils.servers(st.secrets)
timestamps = get_filter_timestamps()

if not servers:
    st.error("No GLEANERIO_ servers found in configuration!")
    st.stop()

# Get all known sources from configurations
known_sources_by_server = get_known_sources(servers, st.secrets)

# Sidebar filters
with st.sidebar:
    st.header("🔍 Log Filters")
    
    # Time period filter
    time_filter = st.selectbox(
        "Time Period",
        options=["All time", "Today", "Yesterday", "Last 7 days", "Last 30 days"],
        index=2
    )
    
    # Source filter (dynamically populated)
    all_sources = set()
    for sources in known_sources_by_server.values():
        all_sources.update(sources)
    
    source_options = ["All sources"] + sorted(list(all_sources))
    source_filter = st.selectbox(
        "Source Name",
        options=source_options,
        index=0
    )
    
    # Service filter
    service_filter = st.selectbox(
        "Service Type",
        options=["All services", "gleaner", "nabu", "scheduler", "summon", "release", "unknown"],
        index=0
    )
    
    # Log type filter
    log_type_filter = st.selectbox(
        "Log Type",
        options=["All types", "error", "info", "warning", "access", "debug", "general"],
        index=0
    )
    
    # Search functionality
    st.header("🔎 Content Search")
    search_term = st.text_input("Search in logs", placeholder="Enter search term...")
    search_enabled = st.checkbox("Enable content search", value=False)

# Create tabs for different views
tab1, tab2, tab3, tab4 = st.tabs(["🔗 Latest by Source", "📁 Log Browser", "🔍 Search Results", "📊 Log Analytics"])

with tab1:
    st.header("Latest Logs by Source")
    st.info("Quick access to the most recent logs for each configured source")
    
    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        known_sources = known_sources_by_server.get(server, [])
        
        if not known_sources:
            continue
            
        with st.expander(f"🖥️ {server_name} - Latest Source Logs", expanded=True):
            try:
                client = utils.s3_client(st.secrets, server)
                bucket = st.secrets[server]["S3_BUCKET"]
                
                # Get all log files
                logs = list(client.list_objects(bucket, prefix=LOG_PATH))
                
                if not logs:
                    st.info("No log files found")
                    continue
                
                # Process logs to find latest for each source
                logs_by_source = {}
                
                for log in logs:
                    if log.is_dir:
                        continue
                    
                    info = parse_log_filename(log.object_name, known_sources)
                    source = info['source']
                    
                    # Group by source and service
                    key = f"{source}_{info['service']}"
                    
                    if key not in logs_by_source:
                        logs_by_source[key] = []
                    
                    logs_by_source[key].append({
                        'log_object': log,
                        'info': info,
                        'sort_key': info['timestamp'] or log.last_modified
                    })
                
                # Sort each group and get the latest
                source_summary = {}
                for key, log_list in logs_by_source.items():
                    log_list.sort(key=lambda x: x['sort_key'], reverse=True)
                    latest = log_list[0]
                    
                    source = latest['info']['source']
                    service = latest['info']['service']
                    
                    if source not in source_summary:
                        source_summary[source] = {}
                    
                    source_summary[source][service] = {
                        'log': latest['log_object'],
                        'info': latest['info'],
                        'total_logs': len(log_list)
                    }
                
                # Display by source
                for source in sorted(source_summary.keys()):
                    if source == 'unknown':
                        continue  # Skip unknown sources in this view
                        
                    st.subheader(f"🔗 {source}")
                    
                    services = source_summary[source]
                    cols = st.columns(min(len(services), 4))
                    
                    for i, (service, data) in enumerate(sorted(services.items())):
                        with cols[i % len(cols)]:
                            log_obj = data['log']
                            info = data['info']
                            
                            # Service card
                            st.write(f"**{service.title()}**")
                            
                            # Basic info
                            log_name = log_obj.object_name.replace(LOG_PATH, '')
                            st.caption(f"📄 {log_name[:30]}{'...' if len(log_name) > 30 else ''}")
                            st.caption(f"📅 {log_obj.last_modified.strftime('%Y-%m-%d %H:%M')}")
                            st.caption(f"💾 {format_file_size(log_obj.size)}")
                            
                            if data['total_logs'] > 1:
                                st.caption(f"📊 {data['total_logs']} total logs")
                            
                            # Action buttons
                            col_a, col_b = st.columns(2)
                            
                            with col_a:
                                if st.button(f"👁️ Preview", key=f"latest_preview_{server}_{source}_{service}"):
                                    with st.spinner("Loading..."):
                                        preview = get_log_preview(client, bucket, log_obj.object_name)
                                        st.text_area(
                                            f"Preview: {source} - {service}",
                                            preview,
                                            height=200,
                                            key=f"latest_preview_content_{server}_{source}_{service}"
                                        )
                            
                            with col_b:
                                if log_obj.size < MAX_LOG_SIZE_MB * 1024 * 1024:
                                    if st.button(f"⬇️ Download", key=f"latest_download_{server}_{source}_{service}"):
                                        try:
                                            response = client.get_object(bucket, log_obj.object_name)
                                            content = response.read()
                                            
                                            st.download_button(
                                                label=f"💾 Get {log_name}",
                                                data=content,
                                                file_name=log_name,
                                                mime="text/plain",
                                                key=f"latest_download_btn_{server}_{source}_{service}"
                                            )
                                        except Exception as e:
                                            st.error(f"Download error: {str(e)}")
                
                # Show unknown source logs if any
                if 'unknown' in source_summary:
                    with st.expander("❓ Unrecognized Log Files", expanded=False):
                        unknown_services = source_summary['unknown']
                        for service, data in unknown_services.items():
                            log_obj = data['log']
                            log_name = log_obj.object_name.replace(LOG_PATH, '')
                            st.caption(f"📄 {log_name} ({service}) - {format_file_size(log_obj.size)}")
                
            except Exception as e:
                st.error(f"Error accessing latest logs for {server_name}: {str(e)}")
                logger.error(f"Latest logs error for {server}: {str(e)}")

with tab2:
    st.header("Log File Browser")
    
    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        
        with st.expander(f"🖥️ {server_name} - Log Files", expanded=True):
            try:
                client = utils.s3_client(st.secrets, server)
                bucket = st.secrets[server]["S3_BUCKET"]
                
                # Get log files
                logs = list(client.list_objects(bucket, prefix=LOG_PATH))
                
                if not logs:
                    st.info("No log files found")
                    continue
                
                # Process logs and create enhanced dataframe
                log_data = []
                for log in logs:
                    if log.is_dir:
                        continue
                        
                    info = parse_log_filename(log.object_name)
                    log_data.append({
                        'name': log.object_name.replace(LOG_PATH, ''),
                        'full_path': log.object_name,
                        'service': info['service'],
                        'log_type': info['log_type'],
                        'date': info['date'],
                        'last_modified': log.last_modified,
                        'size': log.size,
                        'size_formatted': format_file_size(log.size)
                    })
                
                if not log_data:
                    st.info("No valid log files found")
                    continue
                
                log_df = pd.DataFrame(log_data)
                
                # Apply filters
                filtered_df = log_df.copy()
                
                # Time filter
                if time_filter != "All time":
                    filter_date = timestamps[time_filter.lower().replace(' ', '_')]
                    filtered_df = filtered_df[filtered_df['last_modified'] > filter_date]
                
                # Service filter
                if service_filter != "All services":
                    filtered_df = filtered_df[filtered_df['service'] == service_filter]
                
                # Log type filter
                if log_type_filter != "All types":
                    filtered_df = filtered_df[filtered_df['log_type'] == log_type_filter]
                
                # Sort by most recent first
                filtered_df = filtered_df.sort_values('last_modified', ascending=False)
                
                st.write(f"**Found {len(filtered_df)} log files** (filtered from {len(log_df)} total)")
                
                # Display logs with interactive features
                for _, log_row in filtered_df.iterrows():
                    with st.container():
                        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                        
                        with col1:
                            st.write(f"**📄 {log_row['name']}**")
                            st.caption(f"Service: `{log_row['service']}` | Type: `{log_row['log_type']}`")
                        
                        with col2:
                            st.caption(f"**Size:** {log_row['size_formatted']}")
                            if log_row['date']:
                                st.caption(f"**Date:** {log_row['date']}")
                        
                        with col3:
                            st.caption(f"**Modified:** {log_row['last_modified'].strftime('%Y-%m-%d %H:%M')}")
                        
                        with col4:
                            # Preview button
                            if st.button(f"Preview", key=f"preview_{server}_{log_row['name']}"):
                                with st.spinner("Loading preview..."):
                                    preview = get_log_preview(client, bucket, log_row['full_path'])
                                    st.text_area(
                                        f"Preview of {log_row['name']}",
                                        preview,
                                        height=300,
                                        key=f"preview_content_{server}_{log_row['name']}"
                                    )
                            
                            # Download button for smaller files
                            if log_row['size'] < MAX_LOG_SIZE_MB * 1024 * 1024:
                                if st.button(f"Download", key=f"download_{server}_{log_row['name']}"):
                                    try:
                                        response = client.get_object(bucket, log_row['full_path'])
                                        content = response.read()
                                        
                                        st.download_button(
                                            label=f"💾 Download {log_row['name']}",
                                            data=content,
                                            file_name=log_row['name'],
                                            mime="text/plain",
                                            key=f"download_btn_{server}_{log_row['name']}"
                                        )
                                    except Exception as e:
                                        st.error(f"Error downloading file: {str(e)}")
                            else:
                                st.caption(f"File too large ({log_row['size_formatted']}) for download")
                        
                        st.divider()
                
            except Exception as e:
                st.error(f"Error accessing logs for {server_name}: {str(e)}")
                logger.error(f"Log access error for {server}: {str(e)}")

with tab3:
    st.header("Search Results")
    
    if search_enabled and search_term:
        st.info(f"Searching for: **{search_term}**")
        
        for server in servers:
            server_name = st.secrets[server].get('NAME', server)
            
            with st.expander(f"🔍 Search Results - {server_name}"):
                try:
                    client = utils.s3_client(st.secrets, server)
                    bucket = st.secrets[server]["S3_BUCKET"]
                    
                    logs = list(client.list_objects(bucket, prefix=LOG_PATH))
                    
                    search_results = {}
                    for log in logs:
                        if log.is_dir or log.size > MAX_LOG_SIZE_MB * 1024 * 1024:
                            continue
                        
                        matches = search_log_content(client, bucket, log.object_name, search_term)
                        if matches:
                            search_results[log.object_name] = matches
                    
                    if search_results:
                        for log_path, matches in search_results.items():
                            log_name = log_path.replace(LOG_PATH, '')
                            st.subheader(f"📄 {log_name}")
                            st.write(f"Found {len(matches)} matches:")
                            
                            for match in matches[:20]:  # Limit display
                                st.code(match, language=None)
                            
                            if len(matches) > 20:
                                st.caption(f"... showing first 20 of {len(matches)} matches")
                            
                            st.divider()
                    else:
                        st.info("No matches found")
                
                except Exception as e:
                    st.error(f"Error searching logs for {server_name}: {str(e)}")
    else:
        st.info("Enable content search and enter a search term to see results")

with tab4:
    st.header("Log Analytics")
    
    analytics_data = []
    
    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        
        try:
            client = utils.s3_client(st.secrets, server)
            bucket = st.secrets[server]["S3_BUCKET"]
            
            logs = list(client.list_objects(bucket, prefix=LOG_PATH))
            
            for log in logs:
                if log.is_dir:
                    continue
                
                info = parse_log_filename(log.object_name)
                analytics_data.append({
                    'server': server_name,
                    'service': info['service'],
                    'log_type': info['log_type'],
                    'date': info['date'],
                    'size': log.size,
                    'last_modified': log.last_modified
                })
        
        except Exception as e:
            st.error(f"Error analyzing logs for {server_name}: {str(e)}")
    
    if analytics_data:
        analytics_df = pd.DataFrame(analytics_data)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Service Distribution")
            service_counts = analytics_df['service'].value_counts()
            st.bar_chart(service_counts)
        
        with col2:
            st.subheader("📈 Log Type Distribution")
            type_counts = analytics_df['log_type'].value_counts()
            st.bar_chart(type_counts)
        
        # Storage metrics
        col3, col4 = st.columns(2)
        
        with col3:
            total_size = analytics_df['size'].sum()
            st.metric("Total Log Size", format_file_size(total_size))
        
        with col4:
            avg_size = analytics_df['size'].mean()
            st.metric("Average File Size", format_file_size(avg_size))
        
        # Recent activity
        st.subheader("📅 Recent Log Activity")
        recent_df = analytics_df[analytics_df['last_modified'] > timestamps['last_week']]
        
        if not recent_df.empty:
            recent_grouped = recent_df.groupby([recent_df['last_modified'].dt.date, 'service']).size().reset_index(name='count')
            recent_pivot = recent_grouped.pivot(index='last_modified', columns='service', values='count').fillna(0)
            st.line_chart(recent_pivot)
        else:
            st.info("No recent log activity in the last week")
    
    else:
        st.info("No log data available for analytics")
