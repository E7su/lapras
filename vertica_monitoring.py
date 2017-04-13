from datetime import datetime, timedelta
import os 
from airflow import DAG
from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.operators.slack_operator import SlackAPIPostOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email': ['@airflow'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG("vertica_monitoring",
          default_args=default_args,
          schedule_interval='/3 * * * *')

slack_token = os.environ.get('SLACK_TOKEN')


def get_vertica_status(**kwargs):
    cur = VerticaHook('vertica').get_cursor()
    sql = "SELECT 1"
    cur.execute(sql)
    result = cur.fetchall()
    return result[0][0]


result = get_vertica_status()
test_path = '/tmp/tmp_vertica_monitoring/fail.txt'

# если вертика жива и файлик существует
if result == '1':
    if os.path.isfile(test_path):
        alive_vertica = SlackAPIPostOperator(dag=dag,
                                             task_id='alive_vertica',
                                             token=slack_token,
                                             channel="#airflow",
                                             username='vertica',
                                             text='Vertica is alive :3',
                                             icon_url='https://github.com/E7su/lapras/blob/master/images/vertica.png?raw=true')
        os.remove(test_path)  # удалить файлик
    # если вертика жива и файл не существует
else:
    # если вертика прилегла и файлик существует
    if os.path.isfile(test_path):
        pass
    # если вертика прилегла и файлик НЕ существует
    else:
        open(test_path, "w")  # создать файл
        fail_vertica = SlackAPIPostOperator(dag=dag,
                                            task_id='fail_vertica',
                                            token=slack_token,
                                            channel="#airflow",
                                            username='vertica',
                                            text='Vertica was failed!!!',
                                            icon_url='https://github.com/E7su/lapras/blob/master/images/vertica.png?raw=true')
