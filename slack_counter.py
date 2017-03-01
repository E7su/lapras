from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks import VerticaHook
from airflow.operators.slack_operator import SlackAPIPostOperator

default_args = {
    'owner': 'alfadata',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG("vertica_monitoring",
          default_args=default_args)


def vertica_counter(**kwargs):
    table = 'table_schema.TABLE_NAME'
    cur = VerticaHook('vertica').get_cursor()
    cur.execute('select count(*) from {}'.format(table))
    counter = cur.fetchall()
    counter = counter[0][0]
    return counter


message = 'TABLE_NAME содержит {} записи.'.format(vertica_counter())

slack_monitoring = SlackAPIPostOperator(dag=dag, task_id='slack_monitoring',
                                        token="TOKEN",
                                        channel="#airflow", text=message)