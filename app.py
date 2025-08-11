import streamlit as st



# Define the pages
main_page = st.Page("apps/0.Infrastructure.py", title="Infrastructure", icon="🎈")
page_2 = st.Page("apps/1_Gleaner_Ingest_In_Progress.py", title="Gleaner_Ingest_In_Progress 2", icon="❄️")
page_3 = st.Page("apps/2_Scheduler.py", title="Scheduler", icon="🎉")
page_4 = st.Page("apps/3_Gleaner_Logs.py", title="Gleaner Logs", icon="🎉")
page_5 = st.Page("apps/4_Source_Statistics.py", title="Source Statistics", icon="📊")

# Set up navigation
pg = st.navigation([main_page, page_2, page_3, page_4, page_5])

# Run the selected page
pg.run()
