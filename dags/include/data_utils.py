import pandas as pd
import pandera as pa
from io import StringIO
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Define the data quality schema
schema = pa.DataFrameSchema({
    "sale_id": pa.Column(str, unique=True)  
})

# Defining the function to fetch and transform the data
def fetch_and_transform(wasb_conn_id, container, blob_name):

    # Creating the hook
    wasb_hook = WasbHook(wasb_conn_id=wasb_conn_id)

    # Fetching the content directly into memory 
    blob_data = wasb_hook.read_file(container, blob_name)
    df = pd.read_csv(StringIO(blob_data))

    # Transformations
    # Change any column with date to a datetime64[ns] dtype
    for col in df.columns:
        if 'date' in col.lower():
            df[col] = df[col].astype('datetime64[ns]')
        else:
            next

    # Change the column names to lowercase
    df.columns = df.columns.str.lower()

    # Adding the new column for a load data
    df['load_date'] = pd.Timestamp.now()

    # Run the data quality checks
    try:
        schema.validate(df, lazy=True) # lazy=True reports all errors, not just first

    except pa.errors.SchemaErrors as err:
        print(f"Data Quality Failed: {err}")
        raise  # Fails the Airflow task
        
    return df

def load_to_postgres(df, postgres_conn_id, table_name):
    
    # Create the postgres hook
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # Convert the df to a csv in memory to COPY INTO postgres
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # Execute the COPY INTO
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.copy_from(buffer, table_name, sep=",")
    conn.commit()
    cursor.close()
    conn.close()