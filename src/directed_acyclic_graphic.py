from airflow import models
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from libs.airflow import  log
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator,ClusterGenerator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": log,
    "on_success_callback": log,
    "on_retry_callback":   log,
    "sla_miss_callback":   log,
}


dag = DAG(
    "directed_acyclic_graphic",
    schedule_interval=None,
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc_airflow"],
)


# Create a dummy start and end task.
start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", trigger_rule="all_done", dag=dag)

CLUSTER_NAME = 'dtp-cluster'
REGION='us-central1' # Região do projeto
PROJECT_ID='project-id' #ID do projeto
PYSPARK_URI=f'gs://cloud-storage-bucket/{CLUSTER_NAME}/main.py' # caminho do script em pyspark no bucket do cloud storage

CLUSTER_CONFIG = ClusterGenerator(
    project_id="sandbox-multib",
    region="us-central1",
    cluster_name= CLUSTER_NAME,
    tags=["dataproc"],
    storage_bucket="bucket-id",
    num_masters=1,
    num_workers=2,
    master_machine_type="n2-standard-8",
    master_disk_type="pd-standard",
    master_disk_size=1024,
    worker_machine_type="n2-standard-8",
    worker_disk_type="pd-standard",
    worker_disk_size=1024,
    image_version = "2.0.42-debian10",
    service_account_scopes=['https://www.googleapis.com/auth/cloud-platform'],
    properties={
        "spark:spark.jars" : "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar",
    },
    metadata={"PIP_PACKAGES" : 'google-api-core==2.17.1 google-cloud-bigquery==3.10.0 pyspark==3.5.1 google-cloud-storage==2.8.0 apache-airflow apache-airflow-providers-google'}
).make()


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag,
)

submit_job = DataprocSubmitJobOperator(
    task_id="pyspark_ingest_to_bq", 
    job=PYSPARK_JOB, 
    region=REGION, 
    project_id=PROJECT_ID,
    dag=dag,
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster", 
    project_id=PROJECT_ID, 
    cluster_name=CLUSTER_NAME, 
    region=REGION,
    dag=dag,
)

start >> create_cluster >> submit_job >> delete_cluster >> end
