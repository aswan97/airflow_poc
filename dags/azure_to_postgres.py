from airflow.decorators import dag, task 
from datetime import datetime 
from include.data_utils import fetch_and_transform, load_to_postgres

@dag(start_date=datetime(2025, 12, 26), schedule=None, catchup=False)

def azure_to_postgres():
    
    @task
    def extract_and_validate():
        return fetch_and_transform(
            wasb_conn_id='wasb_default',
            container='apple-data',
            blob_name='sales.csv'
        )
    
    @task 
    def load_data(df):
        load_to_postgres(
            df=df,
            postgres_conn_id='postgres_conn',
            table_name='sales'
        )

    load_data(extract_and_validate())

azure_to_postgres()