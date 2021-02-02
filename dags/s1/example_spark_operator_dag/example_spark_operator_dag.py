from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

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


spark_application_name = "spark-wordcount-py-{{ ds }}-{{ task_instance.try_number }}"

compute_resources = {
    'request_cpu': '200m',
    'request_memory': '512Mi',
    'limit_cpu': '500m',
    'limit_memory': '1Gi'
}

volume = k8s.V1Volume(
    name="spark-data",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='spark-pvc')
)

# this describes where to mount the volume in the pod
volume_mount = k8s.V1VolumeMount(
    name="spark-data",
    mount_path="/mnt1",
    sub_path=None,
    read_only=False
)

dag = DAG(
    'example_spark_wordcount_dag',
    default_args=default_args,
    description='Simple wordcount spark job which uses the spark-k8s-operator in a Kubernetes cluster',
    schedule_interval=timedelta(days=1),
)

download_txtfile = KubernetesPodOperator(
    name="download_txtfile",
    namespace="airflow",
    image="cirrusci/wget",
    cmds=["/bin/sh","-c",
          "mkdir -p /mnt1/data &&  mkdir -p /mnt1/results && wget https://norvig.com/big.txt -O /mnt1/data/big.txt"
          ],
    task_id="download_txtfile",
    resources=compute_resources,
    volumes=[volume],
    volume_mounts=[volume_mount],
    get_logs=True,
    dag=dag
)

spark_task = SparkKubernetesOperator(
    task_id="spark-wordcount",
    namespace="airflow",
    application_file="spark-wordcount.yaml",
    kubernetes_conn_id="kubernetes_default",
    dag=dag,
)

spark_sensor = SparkKubernetesSensor(
    task_id="spark-wordcount-monitor",
    namespace="airflow",
    application_name=spark_application_name,
    attach_log=True,
    kubernetes_conn_id="kubernetes_default",
    dag=dag
)

download_txtfile >> spark_task >> spark_sensor
