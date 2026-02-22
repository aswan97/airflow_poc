# Importing necessary modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

# Creating a function to fetch data from an api call
def get_book_data_from_API(ti):

    # Our source url with the json payload
    api_url = 'https://openlibrary.org/subjects/science.json'

    # Defining our response object 
    response = requests.get(api_url)

    # Decoding the json reponse 
    book_data = response.json()  

    # Data is pushed to the XCom system of Airflow (Allows data to pass between tasks)
    ti.xcom_push(key='book_data', value=book_data)


# Creating a function to transform the data
def transform_data(ti):
     
    # Create an object to store the transformed data in a list
    transformed_data = []

    # Pull the source data from the XCom system
    book_data = ti.xcom_pull(key='book_data', task_ids='get_book_records')

    # Pull the title, author, and publish data
    for book in book_data.get('works', []):
        title = book.get('title')
        author = book['authors'][0]['name'] if book.get('authors') else None 
        publish_date = book.get('first_publish_year') 

        # Append the transformed data
        transformed_data.append((title, author, publish_date))

    ti.xcom_push(key='transformed_data', value=transformed_data)


# Function to load the data into Postgres
def load_data(ti):

    # Creating a PostgresHook (Handles connection details and provides high-level methods (get_conn, run_sql) for tasks)
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow_test')

    # Creating our connection object using the airflow hook
    connection = pg_hook.get_conn()

    # Creating our cursor object to execute the query of creating the table 
    cursor = connection.cursor()

    # Pulling out the transformed data from XCom
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_book_records')

    # Creating the table if it doesn't already exist
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS airflow_test.books_table (title VARCHAR, author VARCHAR, publish_date INT)"
    )

    # Inserting each record into the table 
    for record in transformed_data:
        cursor.execute(
            "INSERT INTO airflow_test.books_table (title, author, publish_date) VALUES (%s, %s, %s)",
            record
        )

    # Committing the transactions
    connection.commit()

    # Closing the cursor and connection
    cursor.close()
    connection.close()

# Creating the actual DAG
with DAG(
    dag_id = 'postgres_etl_test',
    start_date=datetime(2025,12,25),
    schedule=None,
    catchup=False
) as dag:
    
    get_books = PythonOperator(
        task_id = 'get_book_records',
        python_callable = get_book_data_from_API,
    )

    transform_books = PythonOperator(
        task_id = 'transform_book_records',
        python_callable = transform_data,
    )

    load_books = PythonOperator(
        task_id = 'load_book_records',
        python_callable = load_data,
    )

    get_books >> transform_books >> load_books