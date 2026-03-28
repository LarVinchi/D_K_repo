from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'nyc_taxi_k8s_pipeline',
    default_args=default_args,
    description='Run ETL natively on K8s every 2 minutes',
    schedule_interval='*/2 * * * *', 
    start_date=datetime(2026, 3, 1), # Will backfill from this date
    catchup=True,
    max_active_runs=1
) as dag:

    # Injects the secret we created in k8s/secrets.yaml as an environment variable
    db_url_secret = k8s.V1EnvVar(
        name='DATABASE_URL',
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name='db-credentials',
                key='DATABASE_URL'
            )
        )
    )

    run_etl = KubernetesPodOperator(
        namespace='default',
        image='nyc-etl-image:v1', # The image we will build inside Minikube
        image_pull_policy='Never', # Tells K8s to use the local image
        name="taxi-etl-pod",
        task_id="extract_transform_load",
        is_delete_operator_pod=True, # Cleans up the pod after it finishes
        env_vars=[db_url_secret],
        get_logs=True
    )