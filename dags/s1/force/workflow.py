from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

#
# Kubernetes config: namespace, resources, volume and volume_mounts
#
namespace = "default"
compute_resources = {
    'request_cpu': '200m',
    'request_memory': '512Mi',
    'limit_cpu': '500m',
    'limit_memory': '1Gi'
}

default_args = {
    'owner': 'FONDA S1',
    'depends_on_past': False,
    'email': ['vasilis.bountris@informatik.hu-berlin.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=100),
}

# Working environment variables
HOST_DATA_PATH = '/home/vasilis/airflow/data/'
REMOTE_DATA_PATH = '/data/'

# HYPERPARAMETERS AND RUN SPECIFIC PARAMETERS
# TODO: Move those accordingly

# What sensors we're getting the lvl1 data from
sensors_level1 = 'LT04,LT05,LE07,S2A'

start_date = "20060601"
end_date = "20061231"
daterange = start_date + ',' + end_date
aoi_path = REMOTE_DATA_PATH + '/input/vector/aoi.gpkg'

volume = k8s.V1Volume(
   name="force-volume",
   host_path=k8s.V1HostPathVolumeSource(
       path=HOST_DATA_PATH,
       type="DirectoryOrCreate"
   )
)

volume_mount = k8s.V1VolumeMount(
        name = "force-volume",
        mount_path=REMOTE_DATA_PATH,
        sub_path=None,
        read_only=False
)

with DAG(
        'force',
        default_args=default_args,
        description='Airflow implementation of a FORCE workflow',
        schedule_interval='@once',
        start_date=days_ago(2),
        tags=['force'],
        max_active_runs=1,
        concurrency=10
) as dag:

    # Downloads auxiliary data
    download_auxiliary = KubernetesPodOperator(
        name='download_auxiliary',
        namespace=namespace,
        task_id='download_auxiliary',
        image='bash',
        cmds=["/bin/sh","-c"],
        arguments=['wget -O auxiliary.tar.gz https://box.hu-berlin.de/f/eb61444bd97f4c738038/?dl=1 && tar -xzf auxiliary.tar.gz && cp -r EO-01/input/ /home/vasilis/airflow/data'],
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        dag=dag
        )

    
    # Downloads level 1 data
    download_level_1 = KubernetesPodOperator(
        name='download_level_1',
        namespace=namespace,
        image='davidfrantz/force',
        task_id='download_level_1',
        cmds=["/bin/sh","-c"],
        arguments=[
            f'mkdir metadata && force-level1-csd -u -s {sensors_level1} metadata && mkdir data && force-level1-csd -s {sensors_level1} -d {daterange} -c 0,70 metadata/ data/ queue.txt {aoi_path}'],
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        dag=dag
        )

    generate_allowed_tiles = KubernetesPodOperator(
        name='generate_allowed_tiles',
        namespace=namespace,
        image='davidfrantz/force',
        task_id='generate_allowed_tiles',
        cmds=["/bin/sh","-c"],
        arguments=["sleep 10000"],
        # arguments=['force-tile-extent {aoi_path} tmp/ tileAllow.txt && rm -r tmp'],
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        dag=dag
        )

        # Maybe I have to dynamically create volumes for each workflow run?

    # download_auxiliary >> download_level_1
    generate_allowed_tiles


    