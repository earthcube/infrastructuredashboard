from datetime import datetime, timedelta, timezone
import streamlit as st
import pandas as pd
import utils
from dataclasses_json import dataclass_json
# path to logs
# scheduler/logs
log_path = 'scheduler/logs/'

#https://stackoverflow.com/questions/51827134/comparison-between-datetime-and-datetime64ns-in-pandas
last_week=pd.Timestamp('now',tzinfo=timezone.utc ).floor('D') + pd.Timedelta(-7, unit='D')

st.header("Logs")
st.write("Needs to open logs when clicked, and filter to most recent only. ")
servers  = utils.servers(st.secrets)

for server in servers:
    st.header("Log for server {}".format(server))
    client = utils.s3_client(st.secrets, server)
    bucket = st.secrets[server]["S3_BUCKET"]
    logs = list(client.list_objects(bucket, prefix=log_path,
               #                     recursive=True
                                    ))
    #logs_df = pd.DataFrame.from_dict(logs)
    log_df = pd.DataFrame()
    json_logs = []

    for log in logs:
        name = log.object_name
        last_modified = log.last_modified
        is_dir = log.is_dir
        size = log.size

       # log_df.concat (log)
        json_logs.append({"name":name, "last_modified":last_modified, "is_dir":is_dir, "size":size})
    log_df = pd.DataFrame(json_logs)
    log_df.sort_values(by=['last_modified'], ascending=True, inplace=True)
    this_week= log_df[ log_df['last_modified'] > last_week]
    st.dataframe(this_week)
