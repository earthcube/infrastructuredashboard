import streamlit as st
import utils
from datetime import datetime, timedelta

# TODO calculate a timestamp about a week back.
failuresfrom = datetime.now() - timedelta(days=7)
failuresfrom_ts = failuresfrom.timestamp()

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
    part1 = '''
        query FilteredRunsQuery($cursor: String) {
      runsOrError(
            filter: { statuses: [SUCCESS],
            
    
           pipelineName: "summon_and_release_job" ,
    '''

    part2 = "createdAfter: %i" % failuresfrom_ts
    part3 = '''          }
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
    query = f"""
    {part1}
    {part2}
    {part3}
    """
    data = utils.graph_ql(st.secrets, server, query)
    st.json(data)

    st.header("FAILED Jobs")
    part1 = '''
query
FilteredRunsQuery($cursor: String) {
    runsOrError(
        filter: {statuses: [FAILURE],
'''
    part2 = "            createdAfter: %i" % failuresfrom_ts
    part3 ='''
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
