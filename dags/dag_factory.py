from airflow import DAG
from airflow.utils.module_loading import import_string
from datetime import datetime
from importlib import import_module
import yaml


def read_config(config_filepath):
    """
    Reads the config file
    :param config_filepath:
    :return:
    """
    return yaml.safe_load(
        stream=open(config_filepath, "r"),
    )


def import_modules(module_path):
    """
    Imports the modules that are defined in the config file
    :param module_path:
    :return:
    """
    module_name, class_name = module_path.rsplit(".", 1)
    module = import_module(module_name)
    _callable = getattr(module, class_name)
    return _callable


def create_task(task_config, task_mapper, dag):
    """
    Creates tasks to be instantiated in a DAG

    :param task_config:
    :param task_mapper:
    :param dag:
    :return:
    """
    task_id = task_config['task_id']
    operator_path = task_config['operator']
    params = task_config['params']
    upstream_tasks = task_config.get('upstream', [])

    operator_class = import_modules(operator_path)
    _callable = params.get('python_callable')
    if _callable:
        params['python_callable'] = import_string(_callable)

    task = operator_class(
        task_id=task_id,
        dag=dag,
        **params
    )

    for upstream_task_id in upstream_tasks:
        upstream_task = task_mapper.get(upstream_task_id)
        if upstream_task:
            task.set_upstream(upstream_task)
    return task


def create_dag(config_filepath, globals) -> DAG:
    cfg = read_config(config_filepath)

    dag_id = cfg['airflow']['dag_id']
    schedule_interval = cfg['airflow']['schedule_interval']
    tags = cfg['airflow']['tags']

    default_args = {
        'owner': cfg['airflow']['owner'],
        'start_date': str(cfg['airflow']['start_date'])
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        tags=tags
    )

    task_mapper = {}

    for task_config in cfg['tasks']:
        task = create_task(task_config, task_mapper, dag)
        task_mapper[task.task_id] = task

    globals[dag.dag_id] = dag
    print(dag)
    return dag
