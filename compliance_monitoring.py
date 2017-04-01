from datetime import datetime, timedelta
from os import environ
from airflow import DAG
from airflow.contrib.hooks.vertica_hook  import VerticaHook
from airflow.operators.slack_operator import SlackAPIPostOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email': ['@airflow.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG("compliance_monitoring", default_args=default_args, schedule_interval='2 0 * * *')

slack_token = environ.get('SLACK_TOKEN')


def get_compliance_status(**kwargs):
    cur = VerticaHook('vertica').get_cursor()
    sql = "SELECT GET_COMPLIANCE_STATUS()"
    cur.execute(sql)
    result = cur.fetchall()
    print(result[0][0])
    return result[0][0]



monitoring_compliance=SlackAPIPostOperator(dag=dag,
                                           task_id='monitoring_compliance',
                                           token=slack_token,
                                           channel="#airflow",
                                           username='vertica',
                                           text=get_compliance_status(),
                                           icon_url='https://media.glassdoor.com/sqll/106376/vertica-systems-squarelogo-1425541334714.png')
