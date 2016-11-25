#!/usr/bin/env python
# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

today = datetime.today()

default_args = {
    'owner': 'e7su',
    'depends_on_past': False,
    'start_date': today - timedelta(days=2),
    'email': ['etsu4296@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('magic_job',
          schedule_interval='@daily',
          default_args=default_args)


def print_context(**kwargs):
    print(kwargs)
    return 'Whatever you return gets printed in the logs'

t1 = BashOperator(
    task_id='bash_ls',
    bash_command=u'ls -la',
    dag=dag)

t2 = BashOperator(
    task_id='bash_echo',
    bash_command=u'echo Hi',
    dag=dag)

t2.set_upstream(t1)
