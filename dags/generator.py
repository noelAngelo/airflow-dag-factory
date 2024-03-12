from airflow.decorators import dag
from dag_factory import create_dag
import os

cfg_dir = '/home/noel/PycharmProjects/dagFactory/configs'
for file in os.listdir(cfg_dir):
    dag_gen = create_dag(os.path.join(cfg_dir, file), globals())
