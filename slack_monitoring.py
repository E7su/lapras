from datetime import datetime, timedelta
from os import environ

from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.slack_operator import SlackAPIPostOperator

default_args = {
    'owner': 'alfadata',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG("vertica_monitoring", default_args=default_args, schedule_interval='05 * * * *')

slack_token = environ.get('SLACK_TOKEN')


def get_event_status(**kwargs):
    cur = PostgresHook('airflow_db').get_cursor()

    sql = """SELECT
              to_Char(dttm, 'HH24:MI:SS'),
              dag_id,
              task_id,
              event,
              execution_date
            FROM public.log
            WHERE owner = 'airflow' AND event = 'failed'
              AND EXECUTION_DATE = current_date
              AND dttm >= current_date - INTERVAL '5 minutes'"""

    cur.execute(sql)
    result = cur.fetchall()
    return result


result = get_event_status()
if result:
    message = ''
    for strings in result:
        task_name, dag_name, time = strings
        msg = "Task '{}' in DAG '{}' was failed at {} today.".format(task_name, dag_name, time)
        message = message + '\n' + msg

        slack_monitoring = SlackAPIPostOperator(dag=dag, task_id='slack_monitoring',
                                                token=slack_token,
                                                channel="#airflow", text=message)
