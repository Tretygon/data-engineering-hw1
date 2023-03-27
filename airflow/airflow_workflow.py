from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.operators import empty
import os
import sys

#ugly hack to make importing from a sibling-directory possible
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from src import population
from src import care_providers
sys.path.pop()


# each cube is a single task
# splitting into more subtasks would require copying data across computation-nodes, 
# which seems like a lot of redundant work

@task
def care_providers_cube(**kwargs):
    out_p = './'
    out_p = kwargs['dag_run'].conf.get("output_path", "./")
    care_providers.main(out_p)
@task
def population_cube(**kwargs):
    out_p = './'
    out_p = kwargs['dag_run'].conf.get("output_path", "./")
    population.main(out_p)

dag_args = {
    "email": ["tomaszasadil@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    'retry_delay': timedelta(minutes=15),
    'output_path':'./'
}


        
with DAG(
    dag_id="data-cubes",
    description='cubes',
    default_args=dag_args,
    start_date=datetime(2023, 3, 3),
    schedule=None,
    catchup=False,
    tags=["NDBI046"],   
    # output_path='./' 
) as dag:   
    # no reason for any ordering of the tasks, so they are just called 
    population_cube()
    care_providers_cube()
    

    # task_a = empty.EmptyOperator(task_id="task_a")
    # task_b = empty.EmptyOperator(task_id="task_b")
    # task_a >> task_b
    