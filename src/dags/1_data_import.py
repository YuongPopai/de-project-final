from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2
import vertica_python
import csv
import io

pg_config = {
    'host': 'rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net',
    'port': 6432,
    'database': 'db1',
    'user': 'student',
    'password': 'de_student_112022'
}

vertica_config = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'database': 'dwh',
    'user': 'stv2024111143',
    'password': 'I3FhLQrF0s2dUzo',
    'autocommit': True,
    'tlsmode': 'disable'  
}

def execute_pg_query(query):
    with psycopg2.connect(**pg_config) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

def execute_vertica_query(query, params=None):
    with vertica_python.connect(**vertica_config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)

def load_transactions(**kwargs):
    date = kwargs['execution_date'].strftime('%Y-%m-%d')
    query = f"""
        SELECT operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt
        FROM public.transactions
        WHERE transaction_dt::date = '{date}';
    """
    data = execute_pg_query(query)

    insert_query = """
        INSERT INTO STV2024111143__STAGING.transactions (operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    batch_size = 1000  
    with vertica_python.connect(**vertica_config) as conn:
        with conn.cursor() as cur:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cur.executemany(insert_query, batch)


def load_currencies(**kwargs):
    date = kwargs['execution_date'].strftime('%Y-%m-%d')
    query = f"""
        SELECT date_update, currency_code, currency_code_with, currency_with_div
        FROM public.currencies
        WHERE date_update::date = '{date}';
    """
    data = execute_pg_query(query)

    insert_query = """
        INSERT INTO STV2024111143__STAGING.currencies (date_update, currency_code, currency_code_with, currency_with_div)
        VALUES (%s, %s, %s, %s);
    """
    batch_size = 1000  
    with vertica_python.connect(**vertica_config) as conn:
        with conn.cursor() as cur:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cur.executemany(insert_query, batch)

default_args = {
    'owner': 'stv2024111143',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_data_to_staging',
    default_args=default_args,
    description='Load data from PostgreSQL to Vertica Staging Layer',
    schedule_interval=timedelta(days=1),
    catchup=False,  
)

load_transactions_task = PythonOperator(
    task_id='load_transactions',
    python_callable=load_transactions,
    provide_context=True,
    dag=dag,
)

load_currencies_task = PythonOperator(
    task_id='load_currencies',
    python_callable=load_currencies,
    provide_context=True,
    dag=dag,
)

load_transactions_task >> load_currencies_task