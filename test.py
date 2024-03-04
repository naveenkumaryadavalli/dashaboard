import streamlit as st
import altair as alt
import pandas as pd
import mysql.connector
import plotly.express as px
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Connect to a SQLite database (you can replace this with your specific database connection details)
from numpy import size

conn = sqlite3.connect(":memory:")  # Use an in-memory database for this example


# Function to connect to MySQL and fetch data
def fetch_data_from_mysql(host, user, password, database, table):
    connection = mysql.connector.connect(
        host=host, user=user, password=password, database=database
    )
    cursor = connection.cursor()
    # Fetch data from MySQL
    query = f"SELECT * FROM {table}"
    cursor.execute(query)
    data = cursor.fetchall()
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    # Close the connection
    cursor.close()
    connection.close()
    return df

def make_heatmap(input_df, input_y, input_x):
    heatmap = alt.Chart(input_df).mark_rect().encode(
        y=alt.Y(f'{input_y}:O',
                axis=alt.Axis(title="", titleFontSize=18, titlePadding=50, titleFontWeight=100, labelAngle=0)),
        x=alt.X(f'{input_x}:O', axis=alt.Axis(title="", titleFontSize=18, titlePadding=50, titleFontWeight=900)),
        color=alt.Color(legend=None,
                        scale=alt.Scale(scheme='greens')),
        stroke=alt.value('black'),
        strokeWidth=alt.value(0.25),
    ).properties(width=500,height=100
                 ).configure_axis(
        labelFontSize=12,
        titleFontSize=12
    )
    # height=300
    return heatmap

# Streamlit app
def main():
    st.set_page_config(
        page_title="Data Quality Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    alt.themes.enable("dark")
    # Display the title at the top center without extra space
    st.markdown("<h1 style='text-align: center;color: grey; font-size: 30px;'>Data Quality Dashboard</h1>", unsafe_allow_html=True)
    # Light Gray Background for the Panel
    st.markdown(
        """
                    <style>
                        .stApp {
                            background: #f2f2f2;
                            padding: 20px;
                            border-radius: 10px;
                        }
                    </style>
                """,
        unsafe_allow_html=True,
    )

    # MySQL Connection details
    host = "35.187.158.251"
    user = "beat_dq_readonly"
    password = "Gspann@123"
    database = "beat_results_dev"
    table_name = "vw_dv_summary_rpt"

    # Fetch data from MySQL
    try:
        # Read data from mysql server into DF
        df = fetch_data_from_mysql(host, user, password, database, table_name)
        # Create a temporary table in the database
        df.to_sql("vw_dv_summary_rpt", conn, index=False, if_exists="replace")
        # Query data from the temporary table and read it into a DataFrame
        query = """SELECT * ,substr(exec_start_date,1,10) as "Execution Date",
                          dag_id as "ETL Process (DAG) ID",
                          task_id as "ETL Process (DAG) Task ID",
                          test_case_status as "Test Case Status" FROM vw_dv_summary_rpt"""
        df = pd.read_sql_query(query, conn)
        # Display raw data
        # st.dataframe(df)
        # Initialize session_state
        # Get unique categories
        All_ed = ['All'] + df['Execution Date'].unique().tolist()
        All_dag = ['All'] + df['ETL Process (DAG) ID'].unique().tolist()
        All_task = ['All'] + df['ETL Process (DAG) Task ID'].unique().tolist()
        All_task = ['All'] + df['Test Case Status'].unique().tolist()

        if "selected_execution_date" not in st.session_state:
            st.session_state.selected_execution_date = df["Execution Date"].unique()
        if "selected_etl_process_id" not in st.session_state:
            st.session_state.selected_etl_process_id = df[
                "ETL Process (DAG) ID"
            ].unique()
        if "selected_etl_task_id" not in st.session_state:
            st.session_state.selected_etl_task_id = df[
                "ETL Process (DAG) Task ID"
            ].unique()
        if "selected_test_status" not in st.session_state:
            st.session_state.selected_test_status = df["Test Case Status"].unique()

        # Create a single row for all filters
        col1, col2, col3, col4 = st.columns(4)

        # Execution Date filter
        with col1:
            selected_execution_date = st.selectbox(
                "Execution Date", ['(All)'] + df["Execution Date"].unique().tolist(), index=0
            )
            if selected_execution_date == '(All)':
                st.session_state.selected_execution_date = df["Execution Date"].unique().tolist()
            else:
                st.session_state.selected_execution_date = [selected_execution_date]
        # ETL Process (DAG) ID filter
        with col2:
            if st.session_state.selected_execution_date:
                etl_process_ids = df[
                    df["Execution Date"].isin(st.session_state.selected_execution_date)
                ]["ETL Process (DAG) ID"].unique().tolist()
                #st.dataframe(etl_process_ids)
                selected_etl_process_id = st.selectbox(
                    "ETL Process (DAG) ID", ['(All)'] + etl_process_ids, index=0
                )
                if selected_etl_process_id == '(All)':
                    st.session_state.selected_etl_process_id = df["ETL Process (DAG) ID"].unique().tolist()
                else:
                    st.session_state.selected_etl_process_id = [selected_etl_process_id]
        # ETL Process (DAG) Task ID filter
        with col3:
            if st.session_state.selected_etl_process_id:
                etl_task_ids = df[
                    (df["Execution Date"].isin(st.session_state.selected_execution_date))
                    & (
                        df["ETL Process (DAG) ID"].isin(st.session_state.selected_etl_process_id)
                    )
                ]["ETL Process (DAG) Task ID"].unique().tolist()
                selected_etl_task_id = st.selectbox(
                    "ETL Process (DAG) Task ID", ['(All)'] + etl_task_ids, index=0
                )
                if selected_etl_task_id == '(All)':
                    st.session_state.selected_etl_task_id = df["ETL Process (DAG) Task ID"].unique().tolist()
                else:
                    st.session_state.selected_etl_task_id = [selected_etl_task_id]
        # Test Case Status filter
        with col4:
            if st.session_state.selected_etl_task_id:
                test_statuses = df[
                    (df["Execution Date"].isin(st.session_state.selected_execution_date))
                    & (
                        df["ETL Process (DAG) ID"].isin(st.session_state.selected_etl_process_id)
                    )
                    & (
                        df["ETL Process (DAG) Task ID"].isin(st.session_state.selected_etl_task_id)
                    )
                ]["Test Case Status"].unique().tolist()
                selected_test_status = st.selectbox(
                    "Test Case Status", ['(All)'] + test_statuses, index=0
                )
                if selected_etl_task_id == '(All)':
                    st.session_state.selected_test_status = df["Test Case Status"].unique().tolist()
                else:
                    st.session_state.selected_test_status = [selected_test_status]
        # Filter DataFrame based on selected values
        filtered_df = df[
            (df["Execution Date"].isin(st.session_state.selected_execution_date))
            & (df["ETL Process (DAG) ID"].isin(st.session_state.selected_etl_process_id))
            & (df["ETL Process (DAG) Task ID"].isin(st.session_state.selected_etl_task_id))
            & (df["Test Case Status"].isin(st.session_state.selected_test_status))
        ]
        #This temporary filtered table will be used for all across the report
        filtered_df.to_sql("vw_dv_summary_rpt", conn, index=False, if_exists="replace")

        #1 Query for Test Runs Executed
        query = """SELECT count(run_id) as "Test Runs Executed" 
                       FROM vw_dv_summary_rpt"""
        df = pd.read_sql_query(query, conn)
        # st.dataframe(df)
        test_runs_count = df["Test Runs Executed"].values[0]

        #2 Query for Test Case Execution Status Completed
        query = """SELECT count(test_case_id) as "Test Case Execution Status Completed" 
                               FROM vw_dv_summary_rpt """
        df = pd.read_sql_query(query, conn)
        # st.dataframe(df)
        test_case_execution_status_completed = df[
            "Test Case Execution Status Completed"
        ].values[0]

        #3 Query for Test Case Execution Status Progress
        query = '''SELECT count(test_case_id) as "Test Case Execution Status Progress" 
                                       FROM vw_dv_summary_rpt where test_case_status = "IN Progress"'''
        df = pd.read_sql_query(query, conn)
        # st.dataframe(df)
        test_case_execution_status_progress = df[
            "Test Case Execution Status Progress"
        ].values[0]

        #4 Query for Test Case Execution Status Failed
        query = '''SELECT count(test_case_id) as "Test Case Execution Status Failed" 
                                               FROM vw_dv_summary_rpt where test_case_status = "FAIL"'''
        df = pd.read_sql_query(query, conn)
        # st.dataframe(df)
        test_case_execution_status_failed = df[
            "Test Case Execution Status Failed"
        ].values[0]

        #5 Query for Test Case Execution Status By Criticality
        query = """SELECT tc_criticality as "Test Case Criticality" , count(test_case_id) as "Test Case Status By Criticality" 
                                                               FROM vw_dv_summary_rpt group by tc_criticality"""
        df_bar = pd.read_sql_query(query, conn)

        heatmap = make_heatmap(df_bar, 'Test Case Criticality', 'Test Case Status By Criticality')

        st.markdown(
            "<h3 style='color: black; font-size: 18px;'>Execution Summary</h3>",
            unsafe_allow_html=True,
        )

        # Dashboard Main Panel
        col = st.columns((1, 1.5, 3), gap='medium')

        with col[0]:
        # HTML code for a table with two cells
            execution_summary_html_code0 = f"""
                <table>
                    <tr>
                        <td style="vertical-align: top;">
                            <div style="background: white;width: 100%; padding: 10px; border-radius: 5px; margin: 10px;">
                                <h6>Test Runs Executed</h6>
                                <p style='color: black; font-size: 30px; padding: 0px;margin: 0;'>{test_runs_count}</p>
                                <p style='color: black; font-size: 10px; margin: 0; padding: 0;'>till date</p>
                            </div>
                        </td>                        
                        </tr>                    
                </table>
            """
        # Display the HTML code using st.markdown
            st.markdown(execution_summary_html_code0, unsafe_allow_html=True)
        with col[1]:
            # HTML code for a table with two cells
            execution_summary_html_code1 = f"""
                <table>
                    <tr>                        
                        <td style="vertical-align: top;">
                            <div class="status-pair" style="background: white; width: 100%; padding: 10px; border-radius: 5px; margin: 10px;">
                            <h6>Test Case Execution Status</h6>
                                <span class="status-number"> <p style='color: black; display: inline; font-size: 30px; padding: 20px;margin: 0;'>{test_case_execution_status_completed} </p></span>
                                <span class="status-number"><p style='color: blue; display: inline; font-size: 30px; padding: 20px;margin: 0;'>{test_case_execution_status_progress}</p></span>
                                <span class="status-number"><p style='color: red; display: inline; font-size: 30px; padding: 20px;margin: 0;'>{test_case_execution_status_failed}</p></span>
                                <p> </p>
                                <span class="status-number"> <p style='color: black;display: inline; font-size: 10px; margin: 0; padding: 0px;'>completed</p></span>
                                <span class="status-number"> <p style='color: blue;display: inline; font-size: 10px; margin: 0; padding: 20px;'>In Progress</p></span>
                                <span class="status-number"> <p style='color: red; display: inline; font-size: 10px; margin: 0; padding: 0px;'>Failed</p></span>
                            </div>
                        </td>                        
                    </tr>
                </table>
            """
            # Display the HTML code using st.markdown
            st.markdown(execution_summary_html_code1, unsafe_allow_html=True)
        with col[2]:
            col1, col2 = st.columns([1, 1], gap='small')
            # HTML code for a table with two cells
            with col1:
                execution_summary_html_code2 = f"""
                <table>
                    <tr>                        
                        <td style="vertical-align: top;">
                            <div class="status-pair" style="background: white; width: 100%; padding: 10px; border-radius: 5px; margin: 10px;">
                                <h6>Test Case Execution Status By Criticality</h6>
                                <span class="status-number"> <p style='color: black; display: inline; font-size: 30px; padding: 20px;margin: 0;'>{test_case_execution_status_completed}</p></span>
                                <span class="status"> <p style='color: black; font-size: 10px; margin: 0; padding: 10px;'>completed</p></span>                    
                           </div>
                        </td>
                    </tr>

                </table>
            """
            # Display the HTML code using st.markdown
                st.markdown(execution_summary_html_code2, unsafe_allow_html=True)
            with col2:
                st.markdown("")
                st.altair_chart(heatmap, use_container_width=True)

        # st.dataframe(df)
        col1, col2 = st.columns([3, 3], gap='small')
        with col1:
            st.markdown(
                "<h3 style='color: black; font-size: 18px;padding: 0px;'>Execution History</h3>",
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown(
                "<h3 style='color: black; font-size: 18px;padding: 0px;'>Summary Report By Scenario</h3>",
                unsafe_allow_html=True,
            )

            # 6 Query for Execution History
        Execution_history_query = """SELECT dag_id "DAG Name",substr(exec_start_date,1,10) "Execution Date",
                tc_criticality Criticality,
                sum(case when test_case_status != "IN Progress" then 1 else 0 end) as Completed,
                sum(case when test_case_status = "IN Progress" then 1 else 0 end) as "In Progress",
                sum(case when test_case_status = "PASS" then 1 else 0 end) as Passed,
                sum(case when test_case_status = "FAIL" then 1 else 0 end) as Failed
                from vw_dv_summary_rpt
                group by tc_criticality,dag_id,substr(exec_start_date,1,10)"""
        df_eh = pd.read_sql_query(Execution_history_query, conn)

        summary_report_query = """SELECT scenario_name Scenario,tc_criticality Criticality,exec_status "Execution Status",count(*) Executed,
                        sum(case when test_case_status = "IN Progress" then 1 else 0 end) as "In Progress",
                        sum(case when test_case_status = "FAIL" then 1 else 0 end) as Failed,
                        sum(case when test_case_status != "IN Progress" then 1 else 0 end) as Completed,
                        sum(case when test_case_status = "PASS" then 1 else 0 end) as Passed
                        from vw_dv_summary_rpt
                        group by scenario_name,tc_criticality,exec_status"""
        df_sr = pd.read_sql_query(summary_report_query, conn)
        # st.dataframe(df_sr)

        col = st.columns((2, 2), gap='small')

        with col[0]:
            # Radio button to select a row
            selected_option = st.radio("", df_eh['Criticality'].unique().tolist(), key='radio10')

            # Display the selected row
            selected_row = df_eh[df_eh['Criticality'] == selected_option]
            # Number of rows to display at a time
            rows_per_page_eh = 5
            component_id = "eh"
            session_state = st.session_state.get(component_id, {"current_page": 1})
            current_page_eh = session_state.get("current_page", 1)

            # Calculate the start and end index for the current page
            start_index_eh = (current_page_eh - 1) * rows_per_page_eh
            end_index_eh = min(start_index_eh + rows_per_page_eh, len(selected_row))

            # Display the subset of rows using st.dataframe
            st.dataframe(selected_row.iloc[start_index_eh:end_index_eh],hide_index=True,use_container_width=True)

            col1, col2 = st.columns((0.5, 1), gap='small')
            # "Next" button to load the next set of rows
            if col1.button("Next",key="eh1"):
                # Increment the current page number
                current_page_eh += 1
                # Save the updated page number to session state
                st.session_state[component_id] = {"current_page": current_page_eh}
            # "Previous" button to load the previous set of rows
            if col2.button("Previous",key="eh2") and current_page_eh > 1:
                # Decrement the current page number (if not on the first page)
                current_page_eh -= 1
                # Save the updated page number to session state
                st.session_state[component_id] = {"current_page": current_page_eh}

            #st.dataframe(selected_row,hide_index=True,use_container_width=True)
        with col[1]:
            # Radio button to select a row
            selected_option = st.radio("", df_sr['Criticality'].unique().tolist(),key='radio11')

            # Display the selected row
            selected_row_sr = df_sr[df_sr['Criticality'] == selected_option]
            rows_per_page_sr = 5
            component_id = "sr"
            session_state = st.session_state.get(component_id, {"current_page": 1})
            current_page_sr = session_state.get("current_page", 1)

            # Calculate the start and end index for the current page
            start_index_sr = (current_page_sr - 1) * rows_per_page_sr
            end_index_sr = min(start_index_sr + rows_per_page_sr, len(selected_row_sr))

            # Display the subset of rows using st.dataframe
            st.dataframe(selected_row_sr.iloc[start_index_sr:end_index_sr], hide_index=True, use_container_width=True)

            col1, col2 = st.columns((0.5, 1), gap='small')
            # "Next" button to load the next set of rows
            if col1.button("Next", key="sr1"):
                # Increment the current page number
                current_page_sr += 1
                # Save the updated page number to session state
                st.session_state[component_id] = {"current_page": current_page_sr}
            # "Previous" button to load the previous set of rows
            if col2.button("Previous", key="sr2") and current_page_sr > 1:
                # Decrement the current page number (if not on the first page)
                current_page_sr -= 1
                # Save the updated page number to session state
                st.session_state[component_id] = {"current_page": current_page_sr}

            #st.dataframe(selected_row,hide_index=True,use_container_width=True)

        st.markdown(
            "<h3 style='color: black; font-size: 18px;'>Detailed Report By Scenario</h3>",
            unsafe_allow_html=True,
        )

        detail_report_query = """SELECT scenario_name "Scenario Name",scenario_desc "Scenario Desc",dag_id "DAG ID",exec_status "Execution Status",tc_criticality Criticality,count(*) Executed,
                                sum(case when test_case_status != "IN Progress" then 1 else 0 end) as Completed,
                                sum(case when test_case_status = "IN Progress" then 1 else 0 end) as "In Progress",
                                sum(case when test_case_status = "PASS" then 1 else 0 end) as Passed,
                                sum(case when test_case_status = "FAIL" then 1 else 0 end) as Failed
                                from vw_dv_summary_rpt
                                group by scenario_name,scenario_desc,dag_id,tc_criticality,exec_status"""
        filtered_df_dr = pd.read_sql_query(detail_report_query, conn)

        # Number of rows to display at a time
        rows_per_page_dr = 5
        component_id = "dr"
        session_state = st.session_state.get(component_id, {"current_page": 1})
        current_page_dr = session_state.get("current_page", 1)

        # Calculate the start and end index for the current page
        start_index_dr = (current_page_dr - 1) * rows_per_page_dr
        end_index_dr = min(start_index_dr + rows_per_page_dr, len(filtered_df_dr))

        # Display the subset of rows using st.dataframe
        st.dataframe(filtered_df_dr.iloc[start_index_dr:end_index_dr],hide_index=True,use_container_width=True)

        col1, col2 = st.columns((0.1,1), gap='small')
        # "Next" button to load the next set of rows
        if col1.button("Next"):
            # Increment the current page number
            current_page_dr += 1
            # Save the updated page number to session state
            st.session_state[component_id] = {"current_page": current_page_dr}
        # "Previous" button to load the previous set of rows
        if col2.button("Previous") and current_page_dr > 1:
            # Decrement the current page number (if not on the first page)
            current_page_dr -= 1
            # Save the updated page number to session state
            st.session_state[component_id] = {"current_page": current_page_dr}

        #st.dataframe(filtered_df_dr, hide_index=True,use_container_width=True )
        # st.dataframe(df_sr)



    except Exception as e:
        st.error(f"Error: {e}")


if __name__ == "__main__":
    main()
