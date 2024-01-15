# Tutorial: https://blog.streamlit.io/how-to-build-a-real-time-live-dashboard-with-streamlit/

import time  # to simulate a real time data, time loop
import streamlit as st
import numpy as np  # np mean, np random
import pandas as pd  # read csv, df manipulation
import plotly.express as px  # interactive charts
import streamlit as st  # 🎈 data web app development
from snowflake.snowpark.context import get_active_session


session = get_active_session()

st.set_page_config(
    page_title="Real-Time Data Science Dashboard",
    page_icon="✅",
    layout="wide",
)

# read csv from a github repo
# dataset_url = "https://raw.githubusercontent.com/Lexie88rus/bank-marketing-analysis/master/bank.csv"

# read csv from a URL
@st.cache_data
def get_data() -> pd.DataFrame:
    return session.sql("SELECT * FROM HOL_DB.PUBLIC.BANK_MARKETING_ANALYSIS").to_pandas()

df = get_data()

# dashboard title
st.title("Real-Time / Live Data Science Dashboard")

# top-level filters
job_filter = st.selectbox("Select the Job", pd.unique(df["JOB"]))

# creating a single-element container
placeholder = st.empty()

# dataframe filter
df = df[df["JOB"] == job_filter]

# near real-time / live feed simulation
for seconds in range(200):

    df["AGE_NEW"] = df["AGE"] * np.random.choice(range(1, 5))
    df["BALANCE_NEW"] = df["BALANCE"] * np.random.choice(range(1, 5))

    # creating KPIs
    avg_age = np.mean(df["AGE_NEW"])

    count_married = int(
        df[(df["MARITAL"] == "MARRIED")]["MARITAL"].count()
        + np.random.choice(range(1, 30))
    )

    balance = np.mean(df["BALANCE_NEW"])

    with placeholder.container():

        # create three columns
        kpi1, kpi2, kpi3 = st.columns(3)

        # fill in those three columns with respective metrics or KPIs
        kpi1.metric(
            label="Age ⏳",
            value=round(avg_age),
            delta=round(avg_age) - 10,
        )
        
        kpi2.metric(
            label="Married Count 💍",
            value=int(count_married),
            delta=-10 + count_married,
        )
        
        kpi3.metric(
            label="A/C Balance ＄",
            value=f"$ {round(balance,2)} ",
            delta=-round(balance / count_married) * 100,
        )

        # create two columns for charts
        fig_col1, fig_col2 = st.columns(2)
        with fig_col1:
            st.markdown("### First Chart")
            fig = px.density_heatmap(
                data_frame=df, y="AGE_NEW", x="MARITAL"
            )
            st.write(fig)
            
        with fig_col2:
            st.markdown("### Second Chart")
            fig2 = px.histogram(data_frame=df, x="AGE_NEW")
            st.write(fig2)

        st.markdown("### Detailed Data View")
        st.dataframe(df)
        time.sleep(1)