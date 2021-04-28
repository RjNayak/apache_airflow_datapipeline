from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from db_config import get_db_connection
import json
import os.path
from os import path

lookup_tbl_df = None
trip_data_df = None
lookup_file_name = 'zone_lookup.csv'
data_file_name = 'yellow_tripdata_2019-01.csv'
k = 5


def check_file_presense():
    """
    Checks if files is available
    """
    assert path.exists(lookup_file_name)
    assert path.exists(data_file_name)


def check_file_empty():
    """
    Checks if files has data
    """
    lookup_tbl_df = pd.read_csv(lookup_file_name)
    trip_data_df = pd.read_csv(data_file_name)

    assert not lookup_tbl_df.empty and not trip_data_df.empty


def read_data():
    """
    Reads the data from the file and store it to tables
    """
    # get the database conenctino refrence
    engine = get_db_connection()

    lookup_tbl_df = pd.read_csv(lookup_file_name)
    trip_data_df = pd.read_csv(data_file_name)
    trip_data_df = trip_data_df.head(100000)

    # store the data to staging tables
    lookup_tbl_df.to_sql('stg_lookup_data', engine, ifexist="replace")

    # store the data to staging tables
    trip_data_df.to_sql('stg_rtip_data', engine, ifexist="replace")


def process_data():
    """
    Filter and transform the data
    """
    # get the database conenctino refrence
    engine = get_db_connection()
    lookup_tbl_df = pd.read_sql_query(
        'select * from stg_lookup_data', con=engine)
    trip_data_df = pd.read_sql_query('select * from stg_rtip_data', con=engine)

    # merge lookup table to trip data to get the pick up location name
    trip_data_df = pd.merge(trip_data_df, lookup_tbl_df[['Zone', 'LocationID']],
                            left_on='PULocationID', right_on='LocationID')
    # pickmrgdf = pickmrgdf.head(100)

    # merge lookup table to trip data to get the drop off location name
    trip_data_df = pd.merge(trip_data_df, lookup_tbl_df[['Zone', 'LocationID']],
                            left_on='DOLocationID', right_on='LocationID')

    # re-naming column names
    trip_data_df = trip_data_df.rename(
        columns={"Zone_x": "pick_up", "Zone_y": "drop_off"})

    print(trip_data_df.head(10))

    # get the aggreated passenger count grouped by pickup location for each drop-off lcoation
    trip_data_df = trip_data_df.groupby(['pick_up', 'drop_off']).agg(
        {'passenger_count': 'sum'})

    print(trip_data_df.head(10))

    # using rank function to rank the drop off location based on the passanger count
    trip_data_df['Rank'] = trip_data_df['passenger_count'].rank(
        method='dense', ascending=False)

    # ordering the data based on pickup lcoation and rank
    trip_data_df = trip_data_df.sort_values(
        by=['pick_up', 'Rank'])

    # gettiing the month value from file name
    month_value = data_file_name.split('_')[-1].split('.')[0]

    # add the month value to the month column in the dataframe
    trip_data_df.insert(loc=0, column='month', value=month_value)
    # get the database conenctino refrence
    engine = get_db_connection()

    # store the data to database
    trip_data_df.to_sql('analysed_data', engine, ifexists="replace")


def verify_data_table():
    """
    Verify data availabe at destination table
    """
    # get the database conenctino refrence
    engine = get_db_connection()

    a = pd.read_sql_query('SELECT * FROM analysed_data', con=engine)

    assert not a.empty


def populate_history_table():
    """
    Populate history table with top k trends
    """
    # get the database conenctino refrence
    engine = get_db_connection()
    data_query = 'select pick_up,drop_off,"month","Rank_No" from ( SELECT pick_up, drop_off, "month", passenger_count, rank() over (partition by pick_up order by dat."Rank") as "Rank_No" FROM public.analysed_data dat ) q where q."Rank_No" <=' + str(k)

    data = pd.read_sql_query(
        data_query, con=engine)
    data.to_sql('history_table', con=engine, if_exists="append", index=False)


dag = DAG('process_tlc_data_etl', description='To process TLC Data and Store in Database',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

test_data_source = PythonOperator(
    task_id='check_files', python_callable=check_file_presense, dag=dag)

test_data = PythonOperator(
    task_id='check_files_empty', python_callable=check_file_empty, dag=dag)


read_operator = PythonOperator(
    task_id='read_data', python_callable=read_data, dag=dag)

process_operator = PythonOperator(
    task_id='process_data', python_callable=process_data, dag=dag)


verify_tables = PythonOperator(
    task_id='check_data_table', python_callable=verify_data_table, dag=dag)

poupulate_history_operator = PythonOperator(
    task_id='populate_history', python_callable=populate_history_table, dag=dag)

test_data_source >> test_data >> read_operator >> process_operator >> verify_tables >> poupulate_history_operator
