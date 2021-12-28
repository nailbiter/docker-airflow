"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/nailbiter-docker-airflow/dags/tutorial_docker.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-12-28T17:38:54.100098
    REVISION: ---

==============================================================================="""

# the sample below is adapted from https://airflow.apache.org/docs/apache-airflow/1.10.11/tutorial.html
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
import logging
import urllib.request
import json

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'leon',
    'depends_on_past': False,
    'email': ['alozz1991@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    #    'retries': 1,
    #    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
DAG_ID = "tutorial_docker"
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description=f'this is a dag {DAG_ID}',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
)


def run_docker(docker_image, cmd=None):
    #    cmd = f"""curl -X POST -H 'Content-type: application/json' --data '{{"image":"{docker_image}"}}' dood_airflow:5000"""
    #    logging.info(f"cmd: {cmd}")
    #    ec, out = subprocess.getstatusoutput(cmd)
    #    logging.info(f"ec: {ec}")
    #    return out
    req = {"docker_image": docker_image}
    if cmd is not None:
        req["cmd"] = cmd
    req = urllib.request.Request("http://dood_airflow:5000", data=json.dumps(
        req).encode(), headers={"Content-type": "application/json"}, method="POST")
    resp = urllib.request.urlopen(req)
    respData = json.loads(resp.read().decode())
    assert respData["error_code"] == 0, respData["error_code"]
    return respData["output"]


with dag:
    PythonOperator(
        task_id="run_docker",
        python_callable=run_docker,
        op_kwargs={
            "docker_image": "hello-world"
        }
    )
