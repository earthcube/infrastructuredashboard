

Portainer API Key for readonly
At present, does not seem to work

 * Created a user
 * Go to environments, add user to environment
 * log in as user
 * add an api key

it's something in docker.py that does not like not having version.
hand code up list and parsing.

Dagster:
use graphql: https://docs.dagster.io/concepts/webserver/graphql#get-a-list-of-dagster-runs
https://sched.geocodes-data-loader.earthcube.org/graphql


```
query FilteredRunsQuery($cursor: String) {
  runsOrError(
    filter: { statuses: [FAILURE] }
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
```

```
query FilteredRunsQuery($cursor: String) {
  runsOrError(
    filter: { statuses: [SUCCESS],
    pipelineName: "summon_and_release_job" }
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
```

need to calculate a timestamp for say 1 week back.
```
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
```

```
query FilteredRunsQuery($cursor: String) {
  runsOrError(
    filter: { statuses: [FAILURE],
 
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
```

issue with portainer
# Debug in Postman.
# pass a header with an X-API-Key
# docker.py add a version.
# fails:
# GET https://portainer.geocodes-aws-dev.earthcube.org/api/endpoints/2/docker/v1.43/services

# WORKS:
# GET https://portainer.geocodes-aws-dev.earthcube.org/api/endpoints/2/docker/services
#
# So need to use requests to send the information directly and parse the response

