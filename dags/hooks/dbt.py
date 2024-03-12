from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook
from airflow.models import Variable


def get_dbt_job_id(dbt_cloud_conn_id: str, job_name: str):
    hook = DbtCloudHook(dbt_cloud_conn_id=dbt_cloud_conn_id)
    account_id = hook.connection.login
    project_id = int(Variable.get("DBT_PROJECT_ID"))
    jobs = hook.list_jobs()

    results = list(
        filter(lambda job:
               job['name'] == job_name and
               job['account_id'] == int(account_id) and
               job['project_id'] == project_id,
               jobs))
    if results:
        return results[0].get('id')



