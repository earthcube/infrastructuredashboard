import streamlit as st
import utils
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
import logging
import re

logger = logging.getLogger(__name__)

def get_filter_timestamps():
    """Get timestamps for filtering jobs by time period."""
    now = pd.Timestamp('now', tzinfo=timezone.utc)
    return {
        'today': now.floor('D'),
        'yesterday': now.floor('D') + pd.Timedelta(-1, unit='D'),
        'last_week': now.floor('D') + pd.Timedelta(-7, unit='D'),
        'last_month': now.floor('D') + pd.Timedelta(-30, unit='D'),
        'last_quarter': now.floor('D') + pd.Timedelta(-90, unit='D')
    }

def query_with_partitions(server, cursor=None, limit=100):
    """Get runs with partition information using the query from partition_exploration.py"""
    
    query_template = '''
    query RunsWithPartitions($cursor: String) {
      runsOrError(
        limit: %d
        cursor: $cursor
      ) {
        __typename
        ... on Runs {
          results {
            runId
            jobName  
            pipelineName
            status
            startTime
            endTime
            creationTime
            tags {
              key
              value
            }
            runConfigYaml
            assets {
              key {
                path
              }
            }
          }
        }
      }
    }
    ''' % limit

    try:
        variables = {"cursor": cursor} if cursor else {}
        data = utils.graph_ql(st.secrets, server, query_template, variables)
        
        if data and 'data' in data and 'runsOrError' in data['data']:
            if data['data']['runsOrError'].get('__typename') == 'Runs':
                return data['data']['runsOrError'].get('results', [])
        
        logger.warning(f"No valid data returned for runs query: {data}")
        return []
        
    except Exception as e:
        logger.error(f"Error querying runs with partitions: {str(e)}")
        return []

def get_all_jobs_by_status(server, status, created_after_timestamp=None):
    """Get ALL jobs by status without pipeline name filtering."""
    from string import Template

    query_template = '''
    query FilteredRunsQuery($cursor: String) {
      runsOrError(
        filter: {
          statuses: [${STATUS}]
          ${TIMESTAMP_FILTER}
        }
        cursor: $cursor
        limit: 100
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
            tags {
              key
              value
            }
            assets {
              key {
                path
              }
            }
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
                logger.info(f"Found {len(results)} {status} jobs")
                return results
        else:
            logger.warning(f"No valid data returned for {status} jobs query: {data}")
    except Exception as e:
        logger.error(f"Error fetching {status} jobs for {server}: {str(e)}")

    return []

def extract_source_from_tags(tags):
    """Extract source information from run tags"""
    source_info = {}
    
    for tag in tags:
        key = tag.get('key', '').lower()
        value = tag.get('value', '')
        
        # Look for common source-related tags
        if 'source' in key or 'provider' in key:
            source_info['source'] = value
        elif 'tenant' in key:
            source_info['tenant'] = value
        elif 'partition' in key:
            source_info['partition'] = value
            
    return source_info

def extract_source_from_pipeline_name(pipeline_name: str, known_sources: List[str] = None) -> str:
    """Extract source name from pipeline name using various patterns."""
    if not pipeline_name:
        return 'unknown'

    pipeline_lower = pipeline_name.lower()

    # First try exact matches with known sources
    if known_sources:
        for source in known_sources:
            source_lower = source.lower()
            if source_lower in pipeline_lower:
                return source

    # Try common pipeline patterns
    patterns = [
        r'^([a-zA-Z0-9_]+)_summon_and_release',
        r'^([a-zA-Z0-9_]+)_pipeline',
        r'^([a-zA-Z0-9_]+)_job',
        r'^([a-zA-Z0-9_]+)_ingest',
        r'gleaner_([a-zA-Z0-9_]+)',
        r'^([a-zA-Z0-9_]+)[_-]'
    ]

    for pattern in patterns:
        match = re.search(pattern, pipeline_lower)
        if match:
            potential_source = match.group(1)

            # If we have known sources, try fuzzy matching
            if known_sources:
                for source in known_sources:
                    if (potential_source in source.lower() or
                        source.lower() in potential_source or
                        abs(len(potential_source) - len(source.lower())) <= 2):
                        return source

            return potential_source

    return 'unknown'

def calculate_job_duration(start_time, end_time):
    """Calculate job duration in seconds."""
    if not start_time or not end_time:
        return None
    try:
        return end_time - start_time
    except:
        return None

def format_duration(duration_seconds):
    """Format duration in human readable format."""
    if duration_seconds is None:
        return "N/A"

    if duration_seconds < 60:
        return f"{duration_seconds:.1f}s"
    elif duration_seconds < 3600:
        return f"{duration_seconds/60:.1f}m"
    else:
        return f"{duration_seconds/3600:.1f}h"

def get_source_job_statistics(servers, secrets, time_filter='last_week'):
    """Get comprehensive job statistics organized by source."""
    timestamps = get_filter_timestamps()
    filter_timestamp = int(timestamps[time_filter].timestamp())

    # Get all known sources
    known_sources_by_server = {}
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

            # Remove duplicates
            known_sources_by_server[server] = sorted(list(set(sources)))

        except Exception as e:
            logger.error(f"Error getting sources for {server}: {str(e)}")
            known_sources_by_server[server] = []

    all_statistics = {}

    for server in servers:
        server_name = secrets[server].get('NAME', server)
        known_sources = known_sources_by_server.get(server, [])

        # Get all jobs using the partition query and status-specific queries
        try:
            # Use the query_with_partitions for comprehensive job information
            partition_jobs = query_with_partitions(server, limit=200)
            
            # Also get status-specific jobs for completeness
            success_jobs = get_all_jobs_by_status(server, "SUCCESS", filter_timestamp)
            failed_jobs = get_all_jobs_by_status(server, "FAILURE", filter_timestamp)
            running_jobs = get_all_jobs_by_status(server, "STARTED", filter_timestamp)
            queued_jobs = get_all_jobs_by_status(server, "QUEUED", filter_timestamp)

            # Combine all job sources, prioritizing partition query results
            all_jobs = partition_jobs + success_jobs + failed_jobs + running_jobs + queued_jobs
            
            # Remove duplicates based on runId
            seen_run_ids = set()
            unique_jobs = []
            for job in all_jobs:
                run_id = job.get('runId')
                if run_id and run_id not in seen_run_ids:
                    seen_run_ids.add(run_id)
                    unique_jobs.append(job)
            
            all_jobs = unique_jobs

            # Process jobs by source
            source_stats = {}

            for job in all_jobs:
                pipeline_name = job.get('pipelineName', 'unknown')
                
                # Try to extract source from tags first (more reliable)
                tags = job.get('tags', [])
                tag_source_info = extract_source_from_tags(tags)
                
                if tag_source_info.get('source'):
                    source = tag_source_info['source']
                else:
                    # Fall back to pipeline name extraction
                    source = extract_source_from_pipeline_name(pipeline_name, known_sources)

                if source not in source_stats:
                    source_stats[source] = {
                        'source_name': source,
                        'server': server_name,
                        'total_jobs': 0,
                        'success_jobs': 0,
                        'failed_jobs': 0,
                        'running_jobs': 0,
                        'queued_jobs': 0,
                        'durations': [],
                        'pipeline_names': set(),
                        'job_details': []
                    }

                stats = source_stats[source]
                stats['total_jobs'] += 1
                stats['pipeline_names'].add(pipeline_name)

                # Track by status
                status = job.get('status', 'UNKNOWN')
                if status == 'SUCCESS':
                    stats['success_jobs'] += 1
                    # Calculate duration for successful jobs
                    duration = calculate_job_duration(job.get('startTime'), job.get('endTime'))
                    if duration is not None:
                        stats['durations'].append(duration)
                elif status == 'FAILURE':
                    stats['failed_jobs'] += 1
                elif status == 'STARTED':
                    stats['running_jobs'] += 1
                elif status == 'QUEUED':
                    stats['queued_jobs'] += 1

                # Store job details for analysis
                stats['job_details'].append({
                    'job_id': job.get('runId', 'unknown'),
                    'pipeline_name': pipeline_name,
                    'status': status,
                    'start_time': job.get('startTime'),
                    'end_time': job.get('endTime'),
                    'duration': calculate_job_duration(job.get('startTime'), job.get('endTime'))
                })

            # Calculate derived metrics
            for source, stats in source_stats.items():
                durations = stats['durations']
                total = stats['total_jobs']

                # Success rate
                stats['success_rate'] = (stats['success_jobs'] / total * 100) if total > 0 else 0
                stats['failure_rate'] = (stats['failed_jobs'] / total * 100) if total > 0 else 0

                # Duration statistics
                if durations:
                    stats['avg_duration'] = sum(durations) / len(durations)
                    stats['min_duration'] = min(durations)
                    stats['max_duration'] = max(durations)
                    stats['median_duration'] = sorted(durations)[len(durations)//2]
                else:
                    stats['avg_duration'] = None
                    stats['min_duration'] = None
                    stats['max_duration'] = None
                    stats['median_duration'] = None

                # Convert pipeline names set to list
                stats['pipeline_names'] = list(stats['pipeline_names'])

            all_statistics[server] = source_stats

        except Exception as e:
            logger.error(f"Error getting statistics for {server}: {str(e)}")
            all_statistics[server] = {}

    return all_statistics

def display_source_metrics(source_stats, source_name):
    """Display metrics for a single source."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Jobs", source_stats['total_jobs'])
        st.metric("Success Rate", f"{source_stats['success_rate']:.1f}%")

    with col2:
        st.metric("Successful", source_stats['success_jobs'])
        st.metric("Failed", source_stats['failed_jobs'])

    with col3:
        st.metric("Running", source_stats['running_jobs'])
        st.metric("Queued", source_stats['queued_jobs'])

    with col4:
        if source_stats['avg_duration'] is not None:
            st.metric("Avg Duration", format_duration(source_stats['avg_duration']))
            st.metric("Min Duration", format_duration(source_stats['min_duration']))
        else:
            st.metric("Avg Duration", "N/A")
            st.metric("Min Duration", "N/A")
    
    # Add a second row for additional timing metrics if we have duration data
    if source_stats['avg_duration'] is not None and source_stats['success_jobs'] > 0:
        st.write("**📊 Successful Run Timing Analysis:**")
        timing_col1, timing_col2, timing_col3, timing_col4 = st.columns(4)
        
        with timing_col1:
            st.metric("Max Duration", format_duration(source_stats['max_duration']))
        
        with timing_col2:
            st.metric("Median Duration", format_duration(source_stats['median_duration']))
        
        with timing_col3:
            if len(source_stats['durations']) > 1:
                duration_range = source_stats['max_duration'] - source_stats['min_duration']
                st.metric("Duration Range", format_duration(duration_range))
            else:
                st.metric("Duration Range", "N/A")
        
        with timing_col4:
            st.metric("Successful Runs", f"{len(source_stats['durations'])}/{source_stats['success_jobs']}")
            if len(source_stats['durations']) != source_stats['success_jobs']:
                st.caption("⚠️ Some successful runs missing timing data")

def create_source_performance_chart(all_statistics):
    """Create performance chart data for visualization."""
    chart_data = []

    for server, source_stats in all_statistics.items():
        for source, stats in source_stats.items():
            if stats['total_jobs'] > 0:  # Only include sources with jobs
                chart_data.append({
                    'source': source,
                    'server': stats['server'],
                    'total_jobs': stats['total_jobs'],
                    'success_rate': stats['success_rate'],
                    'failure_rate': stats['failure_rate'],
                    'avg_duration': stats['avg_duration'] or 0,
                    'min_duration': stats['min_duration'] or 0,
                    'max_duration': stats['max_duration'] or 0,
                    'median_duration': stats['median_duration'] or 0,
                    'success_jobs': stats['success_jobs'],
                    'failed_jobs': stats['failed_jobs'],
                    'duration_samples': len(stats['durations']) if stats['durations'] else 0
                })

    return pd.DataFrame(chart_data)

# Streamlit UI
st.set_page_config(
    page_title="Source Statistics",
    page_icon="📊",
    layout="wide"
)

st.write("# 📊 Source Runtime Statistics")
st.info("Comprehensive job performance metrics organized by source")

servers = utils.servers(st.secrets)

if not servers:
    st.error("No GLEANERIO_ servers found in configuration!")
    st.stop()

# Sidebar filters
with st.sidebar:
    st.header("⚙️ Analysis Settings")

    # Time period filter
    time_filter = st.selectbox(
        "Analysis Period",
        options=["today", "yesterday", "last_week", "last_month", "last_quarter"],
        index=2,
        format_func=lambda x: x.replace('_', ' ').title()
    )

    # Source filter
    source_filter = st.selectbox(
        "Source Filter",
        options=["All Sources", "Active Sources Only", "Sources with Jobs Only"],
        index=2
    )

    # Metrics to show
    st.header("📈 Metrics Display")
    show_duration_analysis = st.checkbox("Show Duration Analysis", value=True)
    show_failure_analysis = st.checkbox("Show Failure Analysis", value=True)
    show_pipeline_details = st.checkbox("Show Pipeline Details", value=False)

# Get statistics
with st.spinner("Analyzing job performance data..."):
    all_statistics = get_source_job_statistics(servers, st.secrets, time_filter)

# Create tabs
tab1, tab2, tab3, tab4 = st.tabs(["📊 Source Overview", "🎯 Detailed Statistics", "📈 Performance Charts", "🔍 Job Analysis"])

with tab1:
    st.header("Source Performance Overview")

    # Create summary statistics
    total_sources = 0
    total_jobs = 0
    active_sources = 0

    for server, source_stats in all_statistics.items():
        total_sources += len(source_stats)
        for source, stats in source_stats.items():
            total_jobs += stats['total_jobs']
            if stats['total_jobs'] > 0:
                active_sources += 1

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Sources", total_sources)
    with col2:
        st.metric("Active Sources", active_sources)
    with col3:
        st.metric("Total Jobs Analyzed", total_jobs)
    with col4:
        active_rate = (active_sources / total_sources * 100) if total_sources > 0 else 0
        st.metric("Activity Rate", f"{active_rate:.1f}%")

    # Server-by-server summary
    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        source_stats = all_statistics.get(server, {})

        if not source_stats:
            continue

        with st.expander(f"🖥️ {server_name} - Summary", expanded=True):
            # Server metrics
            server_total_jobs = sum(stats['total_jobs'] for stats in source_stats.values())
            server_active_sources = sum(1 for stats in source_stats.values() if stats['total_jobs'] > 0)
            server_success_jobs = sum(stats['success_jobs'] for stats in source_stats.values())
            server_failed_jobs = sum(stats['failed_jobs'] for stats in source_stats.values())

            col_a, col_b, col_c, col_d = st.columns(4)
            with col_a:
                st.metric("Sources", len(source_stats))
                st.metric("Active Sources", server_active_sources)
            with col_b:
                st.metric("Total Jobs", server_total_jobs)
                server_success_rate = (server_success_jobs / server_total_jobs * 100) if server_total_jobs > 0 else 0
                st.metric("Success Rate", f"{server_success_rate:.1f}%")
            with col_c:
                st.metric("Successful Jobs", server_success_jobs)
                st.metric("Failed Jobs", server_failed_jobs)
            with col_d:
                server_running = sum(stats['running_jobs'] for stats in source_stats.values())
                server_queued = sum(stats['queued_jobs'] for stats in source_stats.values())
                st.metric("Running Jobs", server_running)
                st.metric("Queued Jobs", server_queued)

            # Top performing sources
            if server_active_sources > 0:
                st.write("**Top Performing Sources:**")
                sorted_sources = sorted(
                    [(name, stats) for name, stats in source_stats.items() if stats['total_jobs'] > 0],
                    key=lambda x: x[1]['success_rate'],
                    reverse=True
                )[:3]

                for i, (source_name, stats) in enumerate(sorted_sources, 1):
                    st.write(f"{i}. **{source_name}** - {stats['success_rate']:.1f}% success ({stats['total_jobs']} jobs)")

with tab2:
    st.header("Detailed Source Statistics")

    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        source_stats = all_statistics.get(server, {})

        if not source_stats:
            continue

        with st.expander(f"🖥️ {server_name} - Source Details", expanded=True):
            # Filter sources based on selection
            filtered_sources = source_stats.items()
            if source_filter == "Sources with Jobs Only":
                filtered_sources = [(name, stats) for name, stats in source_stats.items() if stats['total_jobs'] > 0]

            if not filtered_sources:
                st.info("No sources match the selected filter")
                continue

            # Sort sources by total jobs
            sorted_sources = sorted(filtered_sources, key=lambda x: x[1]['total_jobs'], reverse=True)

            for source_name, stats in sorted_sources:
                if stats['total_jobs'] == 0 and source_filter == "Sources with Jobs Only":
                    continue

                st.subheader(f"📈 {source_name}")

                # Basic metrics
                display_source_metrics(stats, source_name)

                # Duration analysis
                if show_duration_analysis and stats['avg_duration'] is not None:
                    st.write("**Duration Analysis:**")
                    dur_col1, dur_col2, dur_col3 = st.columns(3)
                    with dur_col1:
                        st.write(f"• **Average:** {format_duration(stats['avg_duration'])}")
                        st.write(f"• **Minimum:** {format_duration(stats['min_duration'])}")
                    with dur_col2:
                        st.write(f"• **Maximum:** {format_duration(stats['max_duration'])}")
                        st.write(f"• **Median:** {format_duration(stats['median_duration'])}")
                    with dur_col3:
                        if len(stats['durations']) > 1:
                            duration_range = stats['max_duration'] - stats['min_duration']
                            st.write(f"• **Range:** {format_duration(duration_range)}")
                            st.write(f"• **Jobs with Duration:** {len(stats['durations'])}")

                # Pipeline details
                if show_pipeline_details:
                    st.write("**Pipeline Information:**")
                    for pipeline in stats['pipeline_names']:
                        st.caption(f"• {pipeline}")

                # Recent job failures
                if show_failure_analysis and stats['failed_jobs'] > 0:
                    st.write("**Failure Analysis:**")
                    failed_jobs = [job for job in stats['job_details'] if job['status'] == 'FAILURE']

                    if failed_jobs:
                        st.write(f"Recent failures ({len(failed_jobs[:3])} of {len(failed_jobs)}):")
                        for job in failed_jobs[:3]:
                            job_id = job['job_id'][:8] if job['job_id'] else 'Unknown'
                            pipeline = job['pipeline_name']
                            st.caption(f"• {job_id}... | {pipeline}")

                st.divider()

with tab3:
    st.header("Performance Charts")

    # Create chart data
    chart_df = create_source_performance_chart(all_statistics)

    if not chart_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Success Rate by Source")
            success_chart = chart_df.set_index('source')['success_rate']
            st.bar_chart(success_chart)

        with col2:
            st.subheader("Total Jobs by Source")
            jobs_chart = chart_df.set_index('source')['total_jobs']
            st.bar_chart(jobs_chart)

        # Additional timing charts
        if show_duration_analysis:
            duration_data = chart_df[chart_df['avg_duration'] > 0]
            
            if not duration_data.empty:
                st.subheader("Duration Analysis by Source")
                
                # Create side-by-side duration comparison charts
                dur_col1, dur_col2 = st.columns(2)
                
                with dur_col1:
                    st.write("**Average Duration**")
                    avg_duration_chart = duration_data.set_index('source')['avg_duration']
                    st.bar_chart(avg_duration_chart)
                
                with dur_col2:
                    st.write("**Duration Range (Min-Max)**")
                    range_data = duration_data[['source', 'min_duration', 'max_duration']].set_index('source')
                    st.bar_chart(range_data)
                
                # Median duration chart
                st.subheader("Median vs Average Duration Comparison")
                comparison_data = duration_data[['source', 'avg_duration', 'median_duration']].set_index('source')
                st.bar_chart(comparison_data)
                
                # Duration variability analysis
                st.subheader("Duration Statistics Summary")
                timing_summary = duration_data[['source', 'server', 'avg_duration', 'min_duration', 'max_duration', 'median_duration', 'duration_samples']].copy()
                
                # Format durations for display
                for col in ['avg_duration', 'min_duration', 'max_duration', 'median_duration']:
                    timing_summary[f'{col}_formatted'] = timing_summary[col].apply(lambda x: format_duration(x) if x > 0 else 'N/A')
                
                # Calculate variability coefficient
                timing_summary['variability'] = (timing_summary['max_duration'] - timing_summary['min_duration']) / timing_summary['avg_duration']
                timing_summary['variability'] = timing_summary['variability'].round(2)
                
                display_timing_df = timing_summary[['source', 'server', 'avg_duration_formatted', 'min_duration_formatted', 'max_duration_formatted', 'median_duration_formatted', 'duration_samples', 'variability']]
                display_timing_df.columns = ['Source', 'Server', 'Avg Duration', 'Min Duration', 'Max Duration', 'Median Duration', 'Samples', 'Variability Ratio']
                
                st.dataframe(display_timing_df.sort_values('Variability Ratio', ascending=False), use_container_width=True, hide_index=True)
                st.caption("⚠️ Higher variability ratio indicates more inconsistent run times")
                
            else:
                st.info("No duration data available for charts")

        # Performance correlation
        st.subheader("Performance Overview")
        st.write("**Sources sorted by total activity:**")
        display_df = chart_df.sort_values('total_jobs', ascending=False)[['source', 'server', 'total_jobs', 'success_rate', 'failed_jobs']]
        display_df['success_rate'] = display_df['success_rate'].round(1)
        st.dataframe(display_df, use_container_width=True)

    else:
        st.info("No data available for charts")

with tab4:
    st.header("Individual Job Analysis")

    # Source selection for detailed analysis
    all_sources = []
    for server, source_stats in all_statistics.items():
        for source_name in source_stats.keys():
            if source_stats[source_name]['total_jobs'] > 0:
                all_sources.append(f"{source_name} ({st.secrets[server].get('NAME', server)})")

    if all_sources:
        selected_source_display = st.selectbox("Select source for detailed job analysis:", all_sources)

        # Parse the selection
        source_name = selected_source_display.split(' (')[0]
        server_name = selected_source_display.split('(')[-1].rstrip(')')

        # Find the actual server key
        selected_server = None
        for server in servers:
            if st.secrets[server].get('NAME', server) == server_name:
                selected_server = server
                break

        if selected_server and source_name in all_statistics.get(selected_server, {}):
            stats = all_statistics[selected_server][source_name]

            st.subheader(f"Job Details for {source_name}")

            # Summary for this source
            display_source_metrics(stats, source_name)

            # Job details table
            if stats['job_details']:
                st.write("**Recent Job History:**")

                # Convert to DataFrame for better display
                job_df_data = []
                for job in sorted(stats['job_details'], key=lambda x: x['start_time'] or 0, reverse=True):
                    job_df_data.append({
                        'Job ID': job['job_id'][:12] + '...' if job['job_id'] else 'Unknown',
                        'Pipeline': job['pipeline_name'],
                        'Status': job['status'],
                        'Duration': format_duration(job['duration']) if job['duration'] else 'N/A',
                        'Start Time': datetime.fromtimestamp(job['start_time']).strftime('%Y-%m-%d %H:%M') if job['start_time'] else 'N/A'
                    })

                job_df = pd.DataFrame(job_df_data)
                st.dataframe(job_df, use_container_width=True)

                # Status breakdown
                status_counts = {}
                for job in stats['job_details']:
                    status = job['status']
                    status_counts[status] = status_counts.get(status, 0) + 1

                st.write("**Status Breakdown:**")
                status_cols = st.columns(len(status_counts))
                for i, (status, count) in enumerate(status_counts.items()):
                    with status_cols[i]:
                        st.metric(status, count)

    else:
        st.info("No sources with job data available for analysis")

