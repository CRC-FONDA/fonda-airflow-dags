from airflow import DAG
from datetime import timedelta
import time
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago


#
# functions
def sleep_5_seconds():
    time.sleep(5)
    return "slept for 5 seconds"



#
# Default args metadata
#
default_args = {
    'owner': 'FONDA S1',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': False,
    'email': ['soeren.becker@tu-berlin.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


#
# DAG declaration
#
dag = DAG(
    'example_nano_course',
    default_args=default_args,
    schedule_interval=None,
    concurrency=10
)

#
# first task
#
task_1 = DummyOperator(task_id="task_1", dag=dag)

#
# second task PythonOperator
#
task_2 = PythonOperator(
    task_id="sleep_5_seconds",
    dag=dag,
    python_callable=sleep_5_seconds,
)

#
# BashOperator
#
task_3 = BashOperator(
    task_id = "bash_say_hello",
    bash_command = 'echo "bash says hello"',
    dag = dag,
)

task_4 = KubernetesPodOperator(
    task_id = "python_say_hello",
    namespace="airflow", # this is the airflow namespace in the fonda reference stack
    image="python:3.6",
    name="python_hello",
    cmds=["python", "-c"],
    arguments=["print('python says hello')"],
    get_logs=True,
    dag=dag
)


task_1 >> task_2 >> task_3 >> task_4