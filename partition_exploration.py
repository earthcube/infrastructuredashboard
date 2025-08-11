import streamlit as st
import utils
from string import Template
import json

def test_partition_fields_query(server):
    """Test query to explore partition-related fields in Dagster GraphQL."""

    # Query to get partition information for runs
    query_with_partitions = '''
    query RunsWithPartitions($cursor: String) {
      runsOrError(
        limit: 10
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
    '''

    try:
        data = utils.graph_ql(st.secrets, server, query_with_partitions)
        return data
    except Exception as e:
        st.error(f"Error querying partitions: {str(e)}")
        return None

def test_job_partitions_query(server):
    """Test query to explore job partition definitions."""

    # Query to get job/pipeline partition information
    job_partitions_query = '''
    query JobPartitions {
      repositoriesOrError {
        __typename
        ... on RepositoryConnection {
          nodes {
            name
            location {
              name
            }
            pipelines {
              name
              solidHandle(handleID: "root") {
                solid {
                  name
                }
              }
            }
            jobs {
              name
              
            }
          }
        }
      }
    }
    '''

    try:
        data = utils.graph_ql(st.secrets, server, job_partitions_query)
        return data
    except Exception as e:
        st.error(f"Error querying job partitions: {str(e)}")
        return None

def test_asset_partitions_query(server):
    """Test query for asset partition information."""

    asset_partitions_query = '''
    query AssetPartitions {
      assetsOrError {
        __typename
        ... on AssetConnection {
          nodes {
            key {
              path
            }
            partitionDefinition {
              name
              type
            }
            
          }
        }
      }
    }
    '''

    try:
        data = utils.graph_ql(st.secrets, server, asset_partitions_query)
        return data
    except Exception as e:
        st.error(f"Error querying asset partitions: {str(e)}")
        return None

# Streamlit UI for testing partition queries
st.title("🔍 Dagster Partition Fields Explorer")
st.info("Exploring available partition-related fields in Dagster GraphQL schema")

servers = utils.servers(st.secrets)

if not servers:
    st.error("No servers found!")
    st.stop()

# Server selection
selected_server = st.selectbox("Select server to test:", servers, format_func=lambda x: st.secrets[x].get('NAME', x))

if st.button("Test Partition Fields"):
    st.header("🎯 Run Partition Information")

    with st.spinner("Testing run partition fields..."):
        run_data = test_partition_fields_query(selected_server)

        if run_data:
            st.subheader("Raw Response (Run Partitions)")
            st.json(run_data)

            # Parse and display findings
            if 'data' in run_data and 'runsOrError' in run_data['data']:
                runs = run_data['data']['runsOrError'].get('results', [])

                st.subheader("🔍 Analysis of Run Partition Data")

                partition_info_found = False
                for run in runs:
                    tags = run.get('tags', [])
                    assets = run.get('assets', [])

                    # Check for partition-related tags
                    partition_tags = [tag for tag in tags if 'partition' in tag.get('key', '').lower()]

                    if partition_tags or assets:
                        partition_info_found = True

                        st.write(f"**Run {run['runId'][:8]}... ({run['pipelineName']})**")

                        if partition_tags:
                            st.write("Partition Tags:")
                            for tag in partition_tags:
                                st.write(f"  • {tag['key']}: {tag['value']}")

                        if assets:
                            st.write("Assets with Partition Info:")
                            for asset in assets:
                                asset_path = "/".join(asset['key']['path'])
                                partition_key = asset.get('partitionKey')
                                partition_range = asset.get('partitionKeyRange')

                                st.write(f"  • Asset: {asset_path}")
                                if partition_key:
                                    st.write(f"    - Partition Key: {partition_key}")
                                if partition_range:
                                    st.write(f"    - Partition Range: {partition_range['start']} → {partition_range['end']}")

                        st.write("---")

                if not partition_info_found:
                    st.info("No partition information found in recent runs")

    st.header("🏗️ Job/Pipeline Partition Definitions")

    with st.spinner("Testing job partition definitions..."):
        job_data = test_job_partitions_query(selected_server)

        if job_data:
            st.subheader("Raw Response (Job Partitions)")
            with st.expander("Show Raw JSON", expanded=False):
                st.json(job_data)

            # Parse job partition info
            if 'data' in job_data and 'repositoriesOrError' in job_data['data']:
                repos = job_data['data']['repositoriesOrError'].get('nodes', [])

                st.subheader("🔍 Job Partition Analysis")

                for repo in repos:
                    repo_name = repo.get('name', 'Unknown')
                    jobs = repo.get('jobs', [])

                    st.write(f"**Repository: {repo_name}**")

                    partitioned_jobs = []
                    for job in jobs:
                        job_name = job.get('name', 'Unknown')
                        partition_set = job.get('partitionSet')

                        if partition_set:
                            partitioned_jobs.append((job_name, partition_set))

                    if partitioned_jobs:
                        st.write("Jobs with Partitions:")
                        for job_name, partition_set in partitioned_jobs:
                            st.write(f"  • **{job_name}**")
                            st.write(f"    - Partition Set: {partition_set.get('name', 'N/A')}")

                            # Show partition statuses if available
                            partition_statuses = partition_set.get('partitionStatusesOrError', {})
                            if partition_statuses.get('__typename') == 'PartitionStatuses':
                                statuses = partition_statuses.get('results', [])
                                st.write(f"    - Found {len(statuses)} partitions")

                                if statuses:
                                    # Show first few partition examples
                                    for i, status in enumerate(statuses[:3]):
                                        st.write(f"      {i+1}. {status.get('partitionName', 'N/A')} → {status.get('runStatus', 'N/A')}")
                    else:
                        st.write("  No partitioned jobs found")

                    st.write("---")

    st.header("📦 Asset Partition Information")

    with st.spinner("Testing asset partition information..."):
        asset_data = test_asset_partitions_query(selected_server)

        if asset_data:
            st.subheader("Raw Response (Asset Partitions)")
            with st.expander("Show Raw JSON", expanded=False):
                st.json(asset_data)

            # Parse asset partition info
            if 'data' in asset_data and 'assetsOrError' in asset_data['data']:
                assets = asset_data['data']['assetsOrError'].get('nodes', [])

                st.subheader("🔍 Asset Partition Analysis")

                partitioned_assets = []
                for asset in assets:
                    asset_path = "/".join(asset['key']['path'])
                    partition_def = asset.get('partitionDefinition')
                    partition_stats = asset.get('partitionStats')

                    if partition_def or partition_stats:
                        partitioned_assets.append((asset_path, partition_def, partition_stats))

                if partitioned_assets:
                    st.write(f"Found {len(partitioned_assets)} partitioned assets:")

                    for asset_path, partition_def, partition_stats in partitioned_assets:
                        st.write(f"**Asset: {asset_path}**")

                        if partition_def:
                            st.write(f"  • Partition Type: {partition_def.get('type', 'N/A')}")
                            st.write(f"  • Partition Name: {partition_def.get('name', 'N/A')}")

                        if partition_stats:
                            st.write("  • Statistics:")
                            st.write(f"    - Total Partitions: {partition_stats.get('numPartitions', 'N/A')}")
                            st.write(f"    - Materialized: {partition_stats.get('numMaterialized', 'N/A')}")
                            st.write(f"    - Failed: {partition_stats.get('numFailed', 'N/A')}")

                        st.write("---")
                else:
                    st.info("No partitioned assets found")

if st.button("Show Enhanced Run Query with Partitions"):
    st.header("📋 Enhanced Query Template")

    enhanced_query = '''
    query EnhancedRunsWithPartitions($cursor: String) {
      runsOrError(
        filter: {
          statuses: [SUCCESS, FAILURE, STARTED, QUEUED]
        }
        cursor: $cursor
        limit: 50
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
            
            # Tags often contain partition information
            tags {
              key
              value
            }
            
            # Asset information with partition details
            assets {
              key {
                path
              }
              partitionKey
              partitionKeyRange {
                start
                end
              }
            }
            
            # Run configuration might contain partition info
            runConfigYaml
          }
        }
      }
    }
    '''

    st.subheader("Enhanced GraphQL Query Template")
    st.code(enhanced_query, language="graphql")

    st.subheader("Key Partition Fields Available:")
    st.write("""
    **From Runs:**
    - `tags[]` - Often contains partition-related metadata
    - `assets[].partitionKey` - Specific partition identifier  
    - `assets[].partitionKeyRange.start/end` - Partition range information
    - `runConfigYaml` - May contain partition configuration
    
    **From Jobs/Pipelines:**
    - `partitionSet.name` - Partition set definition name
    - `partitionSet.partitionStatusesOrError` - Status of all partitions
    
    **From Assets:**
    - `partitionDefinition.type` - Type of partitioning (time, static, etc.)
    - `partitionStats.numMaterialized` - Count of materialized partitions
    - `partitionStats.numPartitions` - Total partition count
    """)

st.sidebar.markdown("---")
st.sidebar.info("This tool explores partition-related fields available in your Dagster GraphQL API to help enhance source statistics with partition awareness.")
