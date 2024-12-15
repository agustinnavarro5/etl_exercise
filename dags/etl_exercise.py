from datetime import datetime
import logging

from airflow.decorators import dag, task
import pandas as pd # DAG and task decorators for interfacing with the TaskFlow API

import os

from psycopg2 import IntegrityError

import helpers as H
from plotting import generate_time_series_report
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run monthly
    schedule='@monthly',
    # This DAG is set to run for the first time on January 1, 2021. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on scheduler_interval
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    # When catchup=False, your DAG will only run for the latest schedule_interval. In this case, this means
    # that tasks will not be run between January 1, 2021 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the schedule_interval
    catchup=False,
    default_args={
        "retries": 2, # If a task fails, it will retry 2 times.
    },
    tags=['example']) # If set, this tag is shown in the DAG view of the Airflow UI
def etl_exercise():

    @task()
    def extract():
        """
        Extracts raw transaction data from the data source.

        This function fetches or retrieves transaction data from an external data source 
        (e.g., a file, database, API, etc.). The data is returned in a dictionary format
        for further processing. It is usually the first step in an ETL pipeline.

        Returns
        -------
        dict
            A dictionary containing the raw transaction data with relevant keys (e.g., 
            user_id, item_id, item_category, item_price, quantity, transaction_date).
        """
        # Define the path to the data folder
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        DATA_DIR = os.path.join(BASE_DIR, 'data')
        CSV_FILE = os.path.join(DATA_DIR, 'transactions.csv')
        df = pd.read_csv(CSV_FILE)
        return df.to_dict()

    @task() 
    def cleansing_data(transactions_data: dict):
        """
        Cleanses and preprocesses the raw transaction data.

        This function performs data cleaning operations such as handling missing values, 
        removing duplicates, removing outliers and converting data types. It prepares the raw transaction 
        data for further analysis or transformation.

        Parameters
        ----------
        transactions_data : dict
            A dictionary containing raw transaction data. The keys typically include 
            'user_id', 'item_id', 'item_category', 'item_price', 'quantity', and 'transaction_date'.

        Returns
        -------
        dict
            A cleaned version of the transaction data, where any invalid or missing values 
            are handled, and the data is in the appropriate format for further processing.
        """
        df = pd.DataFrame(transactions_data)
        
        # Drop missing values
        df.dropna(inplace=True)

        # Convert transaction_date to datetime
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])

        # Apply outliers managements zscore over columns
        df = H.remove_outliers_zscore(df, 'item_price')
        df = H.remove_outliers_zscore(df, 'quantity')
        return df.to_dict()


    @task()
    def transform_data(transactions_data: dict):
        """
        Transforms the cleansed transaction data for further analysis or storage.

        This function performs data transformations, such as aggregating or deriving new 
        features, calculating metrics like total sales, or grouping the data based on certain 
        parameters.

        Parameters
        ----------
        transactions_data : dict
            A dictionary containing the cleansed transaction data.

        Returns
        -------
        dict
            A dictionary containing the transformed data, with additional calculated columns 
            or grouped summaries for analysis or reporting.
        """
        df = pd.DataFrame(transactions_data)

        # Add a new column `total_amount`
        df['total_amount'] = df['item_price'] * df['quantity']

        # Group df by `user_id` to calculate total amount spent
        grouped_df = df.groupby('user_id', as_index=True).agg(
            total_spent=pd.NamedAgg(column='total_amount', aggfunc='sum')
        )
        logging.info(grouped_df)

        # Generate time series report for total sales per day
        time_series = df.groupby(df['transaction_date'].dt.date).agg(
            total_sales=pd.NamedAgg(column='total_amount', aggfunc='sum')
        )
        time_series.reset_index(inplace=True)
        logging.info(time_series)
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        REPORTS_DIR = os.path.join(BASE_DIR, 'reports/time_series')
        generate_time_series_report(time_series, REPORTS_DIR)

        # DATA ANALYSIS
        df_top = df.copy()
        top_3_categories = H.top_categories(df_top, top_n=3)
        logging.info("Top 3 Categories Based on Total Sales:")
        logging.info(top_3_categories)

        # Customer Segmentation
        user_segments = H.customer_segmentation(df)
        logging.info("\nCustomer Segmentation:")
        logging.info(user_segments[['user_id', 'segment']])

        # Retention analysis
        df_retention_analysis = df.copy()
        cohort_retention = H.cohort_retention_analysis(df_retention_analysis, time_period='M')
        logging.info("\nCohort retention analysis:")
        logging.info(cohort_retention)

        return df.to_dict()

    @task
    def save_to_postgres(transactions_data: dict):
        """
        Saves the transaction data to a PostgreSQL database.

        This function takes the transformed transaction data and inserts it into a PostgreSQL 
        database table. It uses the PostgresHook to interact with the database and supports 
        appending data to the specified table.

        Parameters
        ----------
        transactions_data : dict
            A dictionary containing the transaction data, which could be raw, cleansed, or 
            transformed, depending on the task.

        Returns
        -------
        None

        Raises
        ------
        Exception
            If there is an error during the data insertion process, an exception is raised.
        """
        df = pd.DataFrame(transactions_data)
        # Initialize the PostgresHook to get the connection
        pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
        engine = pg_hook.get_sqlalchemy_engine()

        try:
            df.to_sql('transactions', engine, if_exists='append', index=False)
            logging.info("Data successfully inserted into the 'transactions' table.")
        except IntegrityError:
            logging.info("Data already added, check scheduling configuration!")

    save_to_postgres(transform_data(cleansing_data(extract())))

etl_exercise_dag = etl_exercise()