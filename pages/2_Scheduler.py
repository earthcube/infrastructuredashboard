import streamlit as st
import utils
import datetime
from dagster_graphql import DagsterGraphQLClient

st.write("# Scheduler")

servers  = utils.servers(st.secrets)

for server in servers:


    st.header("Started Summon Jobs")

    query = '''
    query FilteredRunsQuery($cursor: String) {
  runsOrError(
    filter: { statuses: [STARTED],
    pipelineName: "summon_and_release_job" ,
       createdAfter: 1720000000 
       }
    cursor: $cursor
    limit: 10
    ) {
    __typename
    ... on Runs {
      results {
        runId
        jobName
        status
        runConfigYaml
        startTime
        endTime
      }
    }
  }
}
'''

    data = utils.graph_ql(st.secrets, server, query)

    st.json(data)

    st.header("Success Summon Jobs")
    query = '''
    query FilteredRunsQuery($cursor: String) {
  runsOrError(
    filter: { statuses: [SUCCESS],
    pipelineName: "summon_and_release_job" ,
       createdAfter: 1720000000 
       }
    cursor: $cursor
    limit: 10
    ) {
    __typename
    ... on Runs {
      results {
        runId
        jobName
        status
        runConfigYaml
        startTime
        endTime
      }
    }
  }
}
'''
    data = utils.graph_ql(st.secrets, server, query)
    st.json(data)

    st.header("FAILED Jobs")
query = '''
query
FilteredRunsQuery($cursor: String) {
    runsOrError(
        filter: {statuses: [FAILURE],

                 createdAfter: 1720000000
                 }
    cursor: $cursor
limit: 10
) {
    __typename
    ...on
Runs
{
    results
{
    runId
jobName
status
runConfigYaml
startTime
endTime
}
}
}
}
'''

data = utils.graph_ql(st.secrets, server, query)
st.json(data)
