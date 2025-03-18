from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import vertica_python


vertica_config = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'database': 'dwh',
    'user': 'stv2024111143',
    'password': 'I3FhLQrF0s2dUzo',
    'autocommit': True,
    'tlsmode': 'disable'  
}


def execute_vertica_query(query, params=None):
    with vertica_python.connect(**vertica_config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)


def clean_test_accounts(**kwargs):
    query = """
        DELETE FROM STV2024111143__STAGING.transactions
        WHERE account_number_from < 0 OR account_number_to < 0;
    """
    execute_vertica_query(query)


def update_global_metrics(**kwargs):
    date = kwargs['execution_date'].strftime('%Y-%m-%d')
    query = f"""
        INSERT INTO STV2024111143__DWH.global_metrics (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
        SELECT 
            '{date}' AS date_update,
            t.currency_code AS currency_from,
            SUM(t.amount * c.currency_with_div) AS amount_total,
            COUNT(t.operation_id) AS cnt_transactions,
            AVG(t.amount) AS avg_transactions_per_account,
            COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
        FROM 
            STV2024111143__STAGING.transactions t
        JOIN 
            STV2024111143__STAGING.currencies c
        ON 
            t.currency_code = c.currency_code
        WHERE 
            t.transaction_dt::date = '{date}'
        GROUP BY 
            t.currency_code;
    """
    execute_vertica_query(query)

default_args = {
    'owner': 'stv2024111143',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_global_metrics',
    default_args=default_args,
    description='Update global_metrics DWH Table',
    schedule_interval=timedelta(days=1),
    catchup=False,  
)

clean_test_accounts_task = PythonOperator(
    task_id='clean_test_accounts',
    python_callable=clean_test_accounts,
    provide_context=True,
    dag=dag,
)

update_global_metrics_task = PythonOperator(
    task_id='update_global_metrics',
    python_callable=update_global_metrics,
    provide_context=True,
    dag=dag,
)

clean_test_accounts_task >> update_global_metrics_task