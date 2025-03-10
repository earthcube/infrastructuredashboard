import streamlit as st
import utils
from datetime import datetime, timedelta
from string import Template

# TODO calculate a timestamp about a week back.
failuresfrom = datetime.now() - timedelta(days=7)
failuresfrom_ts = failuresfrom.timestamp()

st.write("# Scheduler")

servers  = utils.servers(st.secrets)

for server in servers:
    for prefix in st.secrets[server].DAGSTER_INGEST_PREFIXES:
        with st.expander(f" {prefix} on {server}"):
            st.header(f"{server} {prefix} Started Summon Jobs ")

            query_template = '''
            query FilteredRunsQuery($$cursor: String) {
          runsOrError(
            filter: { statuses: [STARTED],
            pipelineName: "${PREFIX}_summon_and_release_job" ,
               createdAfter: 1720000000 
               }
            cursor: $$cursor
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
            query = Template(query_template).safe_substitute(PREFIX=prefix)
            data = utils.graph_ql(st.secrets, server, query)

            st.json(data)

            st.header("Successful Summon Jobs")
            st.write("need filtering, and making it useful")

            part1 = '''
                query FilteredRunsQuery($$cursor: String) {
              runsOrError(
                    filter: { statuses: [SUCCESS],
                    
            
                   pipelineName: "${PREFIX}_summon_and_release_job" ,
            '''

            part2 = "createdAfter: %i" % failuresfrom_ts
            part3 = '''          }
                    cursor: $$cursor
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
            query_template = f"""
            {part1}
            {part2}
            {part3}
            """
            query = Template(query_template).safe_substitute(PREFIX=prefix)
            data = utils.graph_ql(st.secrets, server, query)

            st.json(data)

            st.header("FAILED Jobs")
            st.write("need filtering, and making it useful. if more than a two weeks old, ignore")

            part1 = '''
        query
        FilteredRunsQuery($cursor: String) {
            runsOrError(
                filter: {statuses: [FAILURE],
                pipelineName: "${PREFIX}_summon_and_release_job" ,
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
            query_template = f"""
                    {part1}
                    {part2}
                    {part3}
                    """
            query = Template(query_template).safe_substitute(PREFIX=prefix)
            data = utils.graph_ql(st.secrets, server, query)
            st.json(data)
