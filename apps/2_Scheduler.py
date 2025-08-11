import streamlit as st
import utils
from datetime import datetime, timedelta
from string import Template
import logging

logger = logging.getLogger(__name__)

# Calculate dynamic timestamps for filtering
def get_filter_timestamps():
    """Get timestamps for filtering jobs by time period."""
    now = datetime.now()
    return {
        'week_ago': int((now - timedelta(days=7)).timestamp()),
        'two_weeks_ago': int((now - timedelta(days=14)).timestamp()),
        'month_ago': int((now - timedelta(days=30)).timestamp())
    }

def calculate_job_duration(start_time, end_time):
    """Calculate job duration in human readable format."""
    if not start_time or not end_time:
        return "N/A"
    try:
        duration_seconds = end_time - start_time
        if duration_seconds < 60:
            return f"{duration_seconds:.1f}s"
        elif duration_seconds < 3600:
            return f"{duration_seconds/60:.1f}m"
        else:
            return f"{duration_seconds/3600:.1f}h"
    except:
        return "N/A"

def format_timestamp(timestamp):
    """Format timestamp to human readable date."""
    if not timestamp:
        return "N/A"
    try:
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return "N/A"

def extract_prefix_from_docker_service(service_name):
    """Extract gleaner prefix from docker service name."""
    if service_name.startswith('sch_') and service_name.endswith('_magic_gleaner'):
        # Extract prefix from pattern: sch_{prefix}_magic_gleaner
        parts = service_name.split('_')
        if len(parts) >= 3:
            # Join middle parts in case prefix contains underscores
            return '_'.join(parts[1:-2]) if len(parts) > 3 else parts[1]
    return None

def get_all_jobs_by_status(server, status, created_after_timestamp=None):
    """Get ALL jobs by status without pipeline name filtering."""
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

def get_jobs_by_status(server, prefix, status, created_after_timestamp):
    """Get jobs by status - keeping for backwards compatibility but now calls get_all_jobs_by_status."""
    return get_all_jobs_by_status(server, status, created_after_timestamp)

def display_job_summary(jobs, job_type):
    """Display a summary of jobs with metrics."""
    if not jobs:
        st.info(f"No {job_type} jobs found")
        return

    st.metric(f"{job_type.title()} Jobs", len(jobs))

    # Show recent jobs
    for job in jobs[:3]:  # Show top 3 most recent
        with st.container():
            job_id = job.get('runId', 'Unknown')[:8]
            start_time = job.get('startTime', 0)
            end_time = job.get('endTime', 0)

            col_a, col_b = st.columns([2, 1])

            with col_a:
                st.caption(f"🔧 {job_id}...")
                if start_time:
                    st.caption(f"Started: {format_timestamp(start_time)}")

            with col_b:
                duration = calculate_job_duration(start_time, end_time)
                if duration != "N/A":
                    st.caption(f"⏱️ {duration}")

    if len(jobs) > 3:
        st.caption(f"... and {len(jobs) - 3} more")

def display_all_jobs_summary(jobs, job_type):
    """Display a summary of ALL jobs with pipeline names included."""
    if not jobs:
        st.info(f"No {job_type} jobs found")
        return

    st.metric(f"{job_type.title()} Jobs", len(jobs))

    # Show recent jobs with pipeline names
    for job in jobs[:5]:  # Show top 5 most recent
        with st.container():
            job_id = job.get('runId', 'Unknown')[:8]
            pipeline_name = job.get('pipelineName', 'Unknown')
            start_time = job.get('startTime', 0)
            end_time = job.get('endTime', 0)

            col_a, col_b = st.columns([3, 1])

            with col_a:
                st.caption(f"🔧 {job_id}... | `{pipeline_name}`")
                if start_time:
                    st.caption(f"Started: {format_timestamp(start_time)}")

            with col_b:
                duration = calculate_job_duration(start_time, end_time)
                if duration != "N/A":
                    st.caption(f"⏱️ {duration}")

    if len(jobs) > 5:
        st.caption(f"... and {len(jobs) - 5} more")

def check_failure_alerts(failed_jobs, prefix, server_name):
    """Check for failure alerts and display warnings."""
    if len(failed_jobs) >= 3:  # Alert threshold
        st.error(
            f"⚠️ **ALERT**: {len(failed_jobs)} failed {prefix.upper()} jobs in the last week on {server_name}!",
            icon="🚨"
        )
    elif len(failed_jobs) >= 1:
        st.warning(
            f"⚠️ {len(failed_jobs)} failed {prefix.upper()} job(s) in the last week on {server_name}",
            icon="⚠️"
        )

st.write("# 📅 Scheduler Dashboard")
st.info("Monitoring Dagster job execution with enhanced metrics and alerts")

servers = utils.servers(st.secrets)

if not servers:
    st.error("No GLEANERIO_ servers found in configuration!")
    st.stop()

# Get filter timestamps
timestamps = get_filter_timestamps()

# Create tabs for different views
tab1, tab2, tab3 = st.tabs(["📊 Job Overview", "⚠️ Failed Jobs", "📈 Performance Metrics"])

with tab1:
    st.header("Recent Job Activity")

    # Add debugging section to show Docker services vs Dagster jobs correlation
    with st.expander("🔍 Debug: GraphQL Query Analysis", expanded=False):
        st.write("**Debugging GraphQL queries and job discovery:**")

        for server in servers:
            server_name = st.secrets[server].get('NAME', server)
            dagster_prefixes = st.secrets[server].get('DAGSTER_INGEST_PREFIXES', [])

            st.subheader(f"🖥️ {server_name}")
            st.write(f"**Dagster GraphQL URL:** `{st.secrets[server]['DAGSTER_GRAPHQL_URL']}`")
            st.write(f"**Configured Prefixes:** {dagster_prefixes}")

            # Get active Docker services
            try:
                docker_services = utils.get_portainer_services(st.secrets, server)
                st.write(f"**Active Docker Services ({len(docker_services)}):**")
                for service in docker_services:
                    service_name = service.get('Spec', {}).get('Name', 'Unknown')
                    prefix = extract_prefix_from_docker_service(service_name)
                    st.write(f"- {service_name} → prefix: `{prefix}`")

            except Exception as e:
                st.error(f"Error fetching Docker services: {str(e)}")

            # Test raw GraphQL queries for each prefix
            st.write("**GraphQL Query Tests:**")
            for prefix in dagster_prefixes:
                st.write(f"**Testing prefix: `{prefix}`**")

                # Test query without date filter first
                raw_query_template = '''
                query FilteredRunsQuery($cursor: String) {
                  runsOrError(
                    filter: {
                      pipelineName: "${PREFIX}_summon_and_release_job"
                    }
                    cursor: $cursor
                    limit: 5
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
                      }
                    }
                    ... on InvalidPipelineRunsFilterError {
                      message
                    }
                    ... on PythonError {
                      message
                    }
                  }
                }
                '''

                query = Template(raw_query_template).safe_substitute(PREFIX=prefix)

                col1, col2 = st.columns(2)

                with col1:
                    st.write(f"Pipeline name: `{prefix}_summon_and_release_job`")

                    try:
                        data = utils.graph_ql(st.secrets, server, query)

                        if data and 'data' in data and 'runsOrError' in data['data']:
                            runs_or_error = data['data']['runsOrError']
                            typename = runs_or_error.get('__typename')

                            if typename == 'Runs':
                                results = runs_or_error.get('results', [])
                                st.success(f"✅ Found {len(results)} total jobs")

                                if results:
                                    # Show job statuses
                                    statuses = {}
                                    for job in results:
                                        status = job.get('status', 'UNKNOWN')
                                        statuses[status] = statuses.get(status, 0) + 1

                                    st.write("Status breakdown:")
                                    for status, count in statuses.items():
                                        st.caption(f"  {status}: {count}")

                                    # Show most recent job
                                    recent_job = results[0]
                                    st.caption(f"Most recent: {recent_job.get('runId', 'N/A')[:8]}... ({recent_job.get('status', 'N/A')})")

                            elif typename in ['InvalidPipelineRunsFilterError', 'PythonError']:
                                st.error(f"❌ GraphQL Error: {runs_or_error.get('message', 'Unknown error')}")
                            else:
                                st.warning(f"⚠️ Unexpected response type: {typename}")
                        else:
                            st.error(f"❌ Invalid response structure: {data}")

                    except Exception as e:
                        st.error(f"❌ Query failed: {str(e)}")

                with col2:
                    # Try alternative pipeline name patterns
                    st.write("**Testing alternative patterns:**")

                    alternative_patterns = [
                        f"{prefix}_summon_and_release",
                        f"{prefix}_summon_and_release_workflow",
                        f"{prefix}_ingest_job",
                        f"{prefix}_pipeline",
                        f"gleaner_{prefix}_job"
                    ]

                    for alt_pattern in alternative_patterns:
                        alt_query = Template(raw_query_template).safe_substitute(PREFIX=prefix.replace('_summon_and_release_job', ''))
                        alt_query = alt_query.replace(f'{prefix}_summon_and_release_job', alt_pattern)

                        try:
                            alt_data = utils.graph_ql(st.secrets, server, alt_query)
                            if alt_data and 'data' in alt_data and 'runsOrError' in alt_data['data']:
                                alt_results = alt_data['data']['runsOrError']
                                if alt_results.get('__typename') == 'Runs' and alt_results.get('results'):
                                    st.success(f"✅ `{alt_pattern}`: {len(alt_results['results'])} jobs")
                                else:
                                    st.caption(f"❌ `{alt_pattern}`: no jobs")
                            else:
                                st.caption(f"❌ `{alt_pattern}`: invalid response")
                        except:
                            st.caption(f"❌ `{alt_pattern}`: query error")

                st.divider()

            # Test generic query to see all available pipelines
            st.write("**Available Pipelines Discovery:**")
            discovery_query = '''
            query AllPipelines {
              runsOrError(
                limit: 10
              ) {
                __typename
                ... on Runs {
                  results {
                    pipelineName
                    jobName
                    status
                  }
                }
              }
            }
            '''

            try:
                discovery_data = utils.graph_ql(st.secrets, server, discovery_query)
                if discovery_data and 'data' in discovery_data:
                    results = discovery_data['data']['runsOrError'].get('results', [])

                    # Get unique pipeline names
                    pipeline_names = set()
                    for job in results:
                        pipeline_name = job.get('pipelineName')
                        if pipeline_name:
                            pipeline_names.add(pipeline_name)

                    st.write(f"**Found {len(pipeline_names)} unique pipeline names:**")
                    for pipeline in sorted(pipeline_names):
                        st.caption(f"- `{pipeline}`")

            except Exception as e:
                st.error(f"Discovery query failed: {str(e)}")

            st.divider()

    for server in servers:
        server_name = st.secrets[server].get('NAME', server)

        with st.expander(f"🖥️ {server_name} - All Jobs", expanded=True):
            st.subheader("🔄 All Jobs (No Prefix Filtering)")

            # Create columns for different job statuses
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.write("**🟠 Queued Jobs**")
                queued_jobs = get_all_jobs_by_status(server, "QUEUED", timestamps['week_ago'])
                display_all_jobs_summary(queued_jobs, "queued")

            with col2:
                st.write("**🟡 Running Jobs**")
                running_jobs = get_all_jobs_by_status(server, "STARTED", timestamps['week_ago'])
                display_all_jobs_summary(running_jobs, "running")

            with col3:
                st.write("**🟢 Recent Successes**")
                success_jobs = get_all_jobs_by_status(server, "SUCCESS", timestamps['week_ago'])
                display_all_jobs_summary(success_jobs, "success")

            with col4:
                st.write("**🔴 Recent Failures**")
                failed_jobs = get_all_jobs_by_status(server, "FAILURE", timestamps['week_ago'])
                display_all_jobs_summary(failed_jobs, "failure")

            # Show pipeline breakdown
            all_jobs = (queued_jobs + running_jobs + success_jobs + failed_jobs)
            if all_jobs:
                st.write("**📊 Pipeline Breakdown:**")
                pipeline_counts = {}
                for job in all_jobs:
                    pipeline = job.get('pipelineName', 'Unknown')
                    pipeline_counts[pipeline] = pipeline_counts.get(pipeline, 0) + 1

                # Show pipeline counts
                pipeline_cols = st.columns(min(len(pipeline_counts), 4))
                for i, (pipeline, count) in enumerate(sorted(pipeline_counts.items())):
                    with pipeline_cols[i % len(pipeline_cols)]:
                        st.metric(pipeline, count)

            # Alert for excessive failures
            if len(failed_jobs) >= 3:
                st.error(f"⚠️ **ALERT**: {len(failed_jobs)} failed jobs in the last week on {server_name}!", icon="🚨")
            elif len(failed_jobs) >= 1:
                st.warning(f"⚠️ {len(failed_jobs)} failed job(s) in the last week on {server_name}", icon="⚠️")

            st.divider()

with tab2:
    st.header("⚠️ Failed Jobs Analysis")
    st.write("Detailed view of failed jobs with filtering and analysis")

    # Time filter selection
    time_filter = st.selectbox(
        "Show failures from:",
        options=["Last 7 days", "Last 14 days", "Last 30 days"],
        index=0
    )

    filter_mapping = {
        "Last 7 days": timestamps['week_ago'],
        "Last 14 days": timestamps['two_weeks_ago'],
        "Last 30 days": timestamps['month_ago']
    }

    selected_timestamp = filter_mapping[time_filter]

    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        dagster_prefixes = st.secrets[server].get('DAGSTER_INGEST_PREFIXES', [])

        if not dagster_prefixes:
            continue

        with st.expander(f"🖥️ {server_name} - Failed Jobs"):
            for prefix in dagster_prefixes:
                st.subheader(f"❌ {prefix.upper()} Failures")

                failed_jobs = get_jobs_by_status(server, prefix, "FAILURE", selected_timestamp)

                if not failed_jobs:
                    st.success(f"✅ No failed {prefix.upper()} jobs in {time_filter.lower()}!")
                    continue

                st.error(f"Found {len(failed_jobs)} failed jobs in {time_filter.lower()}")

                # Display detailed failed jobs
                for job in failed_jobs:
                    with st.container():
                        job_id = job.get('runId', 'Unknown')
                        start_time = job.get('startTime', 0)
                        end_time = job.get('endTime', 0)

                        col1, col2, col3 = st.columns([2, 1, 1])

                        with col1:
                            st.write(f"**🔧 Job ID:** {job_id[:12]}...")
                            if start_time:
                                st.caption(f"Failed: {format_timestamp(start_time)}")

                        with col2:
                            duration = calculate_job_duration(start_time, end_time)
                            if duration != "N/A":
                                st.write(f"**⏱️ Duration:** {duration}")
                            else:
                                st.write("**⏱️ Duration:** Running")

                        with col3:
                            if st.button(f"Details", key=f"details_{server}_{prefix}_{job_id[:8]}"):
                                st.json(job)

                        st.divider()

with tab3:
    st.header("📈 Performance Metrics")
    st.write("Job performance analytics and trends")

    for server in servers:
        server_name = st.secrets[server].get('NAME', server)
        dagster_prefixes = st.secrets[server].get('DAGSTER_INGEST_PREFIXES', [])

        if not dagster_prefixes:
            continue

        with st.expander(f"📊 {server_name} - Performance Analysis"):
            for prefix in dagster_prefixes:
                st.subheader(f"📈 {prefix.upper()} Performance")

                # Get jobs from different time periods
                success_jobs = get_jobs_by_status(server, prefix, "SUCCESS", timestamps['week_ago'])
                failed_jobs = get_jobs_by_status(server, prefix, "FAILURE", timestamps['week_ago'])
                running_jobs = get_jobs_by_status(server, prefix, "STARTED", timestamps['week_ago'])
                queued_jobs = get_jobs_by_status(server, prefix, "QUEUED", timestamps['week_ago'])

                # Calculate metrics
                total_jobs = len(success_jobs) + len(failed_jobs)
                success_rate = (len(success_jobs) / total_jobs * 100) if total_jobs > 0 else 0

                # Display metrics in two rows
                met_col1, met_col2, met_col3, met_col4 = st.columns(4)

                with met_col1:
                    st.metric("Success Rate", f"{success_rate:.1f}%")

                with met_col2:
                    st.metric("Total Jobs (7d)", total_jobs)

                with met_col3:
                    st.metric("Currently Running", len(running_jobs))

                with met_col4:
                    st.metric("Failed Jobs", len(failed_jobs))

                # Second row for additional metrics
                met_col5, met_col6, met_col7, met_col8 = st.columns(4)

                with met_col5:
                    st.metric("Queued Jobs", len(queued_jobs))

                with met_col6:
                    active_jobs = len(running_jobs) + len(queued_jobs)
                    st.metric("Active Jobs", active_jobs, help="Running + Queued")

                with met_col7:
                    st.metric("Success Jobs", len(success_jobs))

                with met_col8:
                    if total_jobs > 0:
                        failure_rate = (len(failed_jobs) / total_jobs * 100)
                        st.metric("Failure Rate", f"{failure_rate:.1f}%")
                    else:
                        st.metric("Failure Rate", "0.0%")

                # Calculate average duration for successful jobs
                durations = []
                for job in success_jobs:
                    start_time = job.get('startTime', 0)
                    end_time = job.get('endTime', 0)
                    if start_time and end_time and end_time > start_time:
                        durations.append(end_time - start_time)

                if durations:
                    avg_duration = sum(durations) / len(durations)
                    st.write(f"**Average Job Duration:** {calculate_job_duration(0, avg_duration)}")

                    # Show duration distribution
                    if len(durations) > 1:
                        min_duration = min(durations)
                        max_duration = max(durations)
                        st.write(f"**Duration Range:** {calculate_job_duration(0, min_duration)} - {calculate_job_duration(0, max_duration)}")

                # Performance trends (placeholder for future enhancement)
                st.info("📊 Historical performance charts coming soon!")

                st.divider()
