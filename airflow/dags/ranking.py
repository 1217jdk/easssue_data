from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization


today = datetime.now() + timedelta(hours=9)
today = today.strftime('%Y-%m-%d')

default_args = {
    'owner': 'owner-name',
    'depends_on_past': False,
    # 'email': ['your-email@g.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
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
with DAG(
        dag_id='ranking_to_db',
        default_args=default_args,
        description='ranking crawling',
        schedule_interval='30 * * * *',  # every hour:30
        start_date=datetime(2023, 2, 9, 0, 0, 0),
        catchup=False,
        tags=[f'{today}'],
        dagrun_timeout=timedelta(minutes=120),

) as dag:
    ranking = BashOperator(
        task_id='ranking',
        depends_on_past=False,
        bash_command='python3 /root/Data/py/ranking_crawling.py',
    )

    ranking

