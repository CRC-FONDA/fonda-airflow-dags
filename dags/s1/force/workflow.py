from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s
from airflow.operators.dummy_operator import DummyOperator

# Working environment variables
MOUNT_DATA_PATH = '/data'

# HYPERPARAMETERS AND RUN SPECIFIC PARAMETERS
# What sensors we're getting the lvl1 data from
sensors_level1 = 'LT04,LT05,LE07,S2A'

start_date = "20060601"
end_date = "200120831"
daterange = start_date + ',' + end_date
aoi_filepath = MOUNT_DATA_PATH + '/input/vector/aoi.gpkg'
datacube_folderpath = MOUNT_DATA_PATH + '/input/grid'
datacube_filepath = datacube_folderpath + '/' + 'datacube-definition.prj'
image_folderpath = MOUNT_DATA_PATH + '/image_data'
image_metadata_folderpath = MOUNT_DATA_PATH + '/image_metadata'
allowed_tiles_filepath = MOUNT_DATA_PATH + '/allowed_tiles.txt'
dem_folderpath = MOUNT_DATA_PATH + '/input/dem'
wvdb = MOUNT_DATA_PATH + '/input/wvdb'
masks_folderpath = MOUNT_DATA_PATH + '/masks'
endmember_filepath = MOUNT_DATA_PATH + '/input/endmember/hostert-2003.txt'

mask_resolution = 30


# Kubernetes config: namespace, resources, volume and volume_mounts
namespace = "default"

compute_resources = {
    'request_cpu': '1000m',
    'request_memory': '512Mi',
    'limit_cpu': '2000m',
    'limit_memory': '5Gi'
}

volume = k8s.V1Volume(
    name="force-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='force-pvc')
)

volume_mount = k8s.V1VolumeMount(
        name = "force-volume",
        mount_path=MOUNT_DATA_PATH,
        sub_path=None,
        read_only=False
)

security_context = k8s.V1SecurityContext(run_as_user=0)

# DAG

default_args = {
    'owner': 'FONDA S1',
    'depends_on_past': False,
    'email': ['vasilis.bountris@informatik.hu-berlin.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=100),
}


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
        arguments=[f'wget -O auxiliary.tar.gz https://box.hu-berlin.de/f/eb61444bd97f4c738038/?dl=1 && tar -xzf auxiliary.tar.gz && cp -r EO-01/input {MOUNT_DATA_PATH}'],
        resources=compute_resources,
        volumes=[volume],
        security_context=security_context,
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
            f'mkdir {image_metadata_folderpath} && \
              force-level1-csd -u -s {sensors_level1} {image_metadata_folderpath} && \
              mkdir {image_folderpath} && \
              force-level1-csd -s {sensors_level1} -d {daterange} -c 0,70 {image_metadata_folderpath} {image_folderpath} queue.txt {aoi_filepath}'],
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        security_context=security_context,
        get_logs=True,
        dag=dag
        )

    generate_allowed_tiles = KubernetesPodOperator(
        name='generate_allowed_tiles',
        namespace=namespace,
        image='davidfrantz/force',
        task_id='generate_allowed_tiles',
        cmds=["/bin/sh","-c"],
        arguments=[f'force-tile-extent {aoi_filepath} {datacube_folderpath} {allowed_tiles_filepath}'],
        security_context=security_context,
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        dag=dag
        )

    generate_analysis_mask = KubernetesPodOperator(
        name='generate_analysis_mask',
        namespace=namespace,
        image='davidfrantz/force',
        task_id='generate_analysis_mask',
        cmds=["/bin/sh","-c"],
        arguments=[f'mkdir {masks_folderpath} && cp {datacube_folderpath}/datacube-definition.prj {masks_folderpath} && force-cube {aoi_filepath} {masks_folderpath} rasterize {mask_resolution}'],
        security_context=security_context,
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        dag=dag
        )

    preprocess_level2 = KubernetesPodOperator(
        name='preprocess_level2',
        namespace=namespace,
        image='davidfrantz/force',
        task_id='preprocess_level2',
        cmds=["/bin/sh","-c"],
        arguments=["""\
        mkdir level2_ard
        mkdir level2_log
        mkdir level2_tmp
        force-parameter . LEVEL2 0
        mv LEVEL2-skeleton.prm $PARAM
        # read grid definition
        CRS=$(sed '1q;d' $CUBEFILE)
        ORIGINX=$(sed '2q;d' $CUBEFILE)
        ORIGINY=$(sed '3q;d' $CUBEFILE)
        TILESIZE=$(sed '6q;d' $CUBEFILE)
        BLOCKSIZE=$(sed '7q;d' $CUBEFILE)
        # set parameters
        # sed -i "/^PARALLEL_READS /cPARALLEL_READS = TRUE" $PARAM
        # sed -i "/^DELAY /cDELAY = 2" $PARAM
        # sed -i "/^NPROC /cNPROC = 56" $PARAM
        sed -i "/^FILE_QUEUE /cFILE_QUEUE = /data/queue.txt" $PARAM
        sed -i "/^DIR_LEVEL2 /cDIR_LEVEL2 = level2_ard/" $PARAM
        sed -i "/^DIR_LOG /cDIR_LOG = level2_log/" $PARAM
        sed -i "/^DIR_TEMP /cDIR_TEMP = level2_tmp/" $PARAM
        sed -i "/^FILE_DEM /cFILE_DEM = $DEM/global_srtm-aster.vrt" $PARAM
        sed -i "/^DIR_WVPLUT /cDIR_WVPLUT = $WVDB" $PARAM
        sed -i "/^FILE_TILE /cFILE_TILE = $TILE" $PARAM
        sed -i "/^TILE_SIZE /cTILE_SIZE = $TILESIZE" $PARAM
        sed -i "/^BLOCK_SIZE /cBLOCK_SIZE = $BLOCKSIZE" $PARAM
        sed -i "/^ORIGIN_LON /cORIGIN_LON = $ORIGINX" $PARAM
        sed -i "/^ORIGIN_LAT /cORIGIN_LAT = $ORIGINY" $PARAM
        sed -i "/^PROJECTION /cPROJECTION = $CRS" $PARAM
        sed -i "/^NTHREAD /cNTHREAD = 2" $PARAM
        echo "STARTING LEVEL2 PROCESSING"
        date
        force-level2 $PARAM
        cp -r level2_ard /data/level2_ard
        cp -r level2_log /data/level2_log
        cp ard.prm /data/ard.prm
        echo "DONE"
        date
            """],

        security_context=security_context,
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars={
            'DATA':image_folderpath,
            'CUBEFILE':datacube_filepath,
            'DEM':dem_folderpath,
            'WVDB':wvdb,
            'TILE':allowed_tiles_filepath,
            'NTHREAD':'2',
            'PARAM':"ard.prm"
            },
        get_logs=True,
        dag=dag
        )

    [download_auxiliary >> download_level_1] >> [generate_allowed_tiles >> generate_analysis_mask] >> level_2_process

