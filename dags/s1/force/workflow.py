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

start_date = "20060101"
end_date = "20120101"
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
queue_filepath = MOUNT_DATA_PATH + '/queue.txt'

mask_resolution = 30


# Kubernetes config: namespace, resources, volume and volume_mounts
namespace = "default"

compute_resources = {
    'request_cpu': '500m',
    'request_memory': '512Mi',
    'limit_cpu': '500m',
    'limit_memory': '2Gi'
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
            f'mkdir -p {image_metadata_folderpath} && \
            force-level1-csd -u -s {sensors_level1} {image_metadata_folderpath} && \
              mkdir -p {image_folderpath} && \
              force-level1-csd -s {sensors_level1} -d {daterange} -c 0,70 {image_metadata_folderpath} {image_folderpath} {queue_filepath} {aoi_filepath}'],
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
        arguments=[f'mkdir -p {masks_folderpath} && cp {datacube_folderpath}/datacube-definition.prj {masks_folderpath} && force-cube {aoi_filepath} {masks_folderpath} rasterize {mask_resolution}'],
        security_context=security_context,
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        dag=dag
        )

    prepare_level2 = KubernetesPodOperator(
        name='prepare_level2',
        namespace=namespace,
        image='davidfrantz/force',
        task_id='prepare_level2',
        cmds=["/bin/sh","-c"],
        arguments=[f"""mkdir -p /data/queue_files
        split -l$((`wc -l < /data/queue.txt`/10)) --numeric-suffixes=0 /data/queue.txt /data/queue_files/queue_ --additional-suffix=.txt
        mkdir -p /data/param_files
        mkdir -p /data/level2_ard
        mkdir -p /data/level2_log
        mkdir -p /data/level2_tmp
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
        sed -i "/^DIR_LEVEL2 /cDIR_LEVEL2 = /data/level2_ard/" $PARAM
        sed -i "/^NPROCE /cNPROC = 2" $PARAM
        sed -i "/^DIR_LOG /cDIR_LOG = /data/level2_log/" $PARAM
        sed -i "/^DIR_TEMP /cDIR_TEMP = /data/level2_tmp/" $PARAM
        sed -i "/^FILE_DEM /cFILE_DEM = $DEM/dem.vrt" $PARAM
        sed -i "/^DIR_WVPLUT /cDIR_WVPLUT = $WVDB" $PARAM
        sed -i "/^FILE_TILE /cFILE_TILE = $TILE" $PARAM
        sed -i "/^TILE_SIZE /cTILE_SIZE = $TILESIZE" $PARAM
        sed -i "/^BLOCK_SIZE /cBLOCK_SIZE = $BLOCKSIZE" $PARAM
        sed -i "/^ORIGIN_LON /cORIGIN_LON = $ORIGINX" $PARAM
        sed -i "/^ORIGIN_LAT /cORIGIN_LAT = $ORIGINY" $PARAM
        sed -i "/^PROJECTION /cPROJECTION = $CRS" $PARAM
        sed -i "/^NTHREAD /cNTHREAD = 2" $PARAM
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
            'PARAM':"/data/param_files/ard.prm"
            },
        get_logs=True,
        dag=dag
        )

    preprocess_level2_tasks = []
    for i in range(10):
        index = str(i)
        preprocess_level2_task = KubernetesPodOperator(
            name='preprocess_level2_' + index,
            namespace=namespace,
            image='davidfrantz/force',
            task_id='preprocess_level2_' + index,
            cmds=["/bin/sh","-c"],
            arguments=["""\
            echo "STARTING LEVEL2 PROCESSING"
            cp $GLOBAL_PARAM $PARAM
            sed -i "/^FILE_QUEUE /cFILE_QUEUE = $QUEUE_FILE" $PARAM
            date
            force-level2 $PARAM
            echo "DONE"
            date
                """],
            # TODO: $PARAM here is useless, we'll need context specific parameter file
            security_context=security_context,
            resources=compute_resources,
            volumes=[volume],
            volume_mounts=[volume_mount],
            env_vars={
                'GLOBAL_PARAM': '/data/param_files/ard.prm',
                'PARAM':f"/data/param_files/ard_0{index}.prm",
                'QUEUE_FILE': f"/data/queue_files/queue_0{index}.txt",
                },
            get_logs=True,
            dag=dag
            )
        preprocess_level2_tasks.append(preprocess_level2_task)

    # process_tsa=KubernetesPodOperator(
        # name='process_tsa',
        # namespace=namespace,
        # image='davidfrantz/force',
        # task_id='process_tsa',
        # cmds=["/bin/sh","-c"],
        # arguments=["""\
        # force-parameter . TSA 0
        # mv LEVEL2-skeleton.prm $PARAM
        # sleep 1000000
        # # processing extent
        # # https://github.com/CRC-FONDA/B5-Workflow-Earth-Observation/blob/main/EO-01/level2processing/processHigherLevel.nf

        # XMIN=$(sed '1d' $TILE | sed 's/[XY]//g' | cut -d '_' -f 1 | sort | head -n 1)
        # XMAX=$(sed '1d' $TILE | sed 's/[XY]//g' | cut -d '_' -f 1 | sort | tail -n 1)
        # YMIN=$(sed '1d' $TILE | sed 's/[XY]//g' | cut -d '_' -f 2 | sort | head -n 1)
        # YMAX=$(sed '1d' $TILE | sed 's/[XY]//g' | cut -d '_' -f 2 | sort | tail -n 1)
        # # pathes
        # sed -i "/^DIR_LOWER /cDIR_LOWER = ard/" $PARAM
        # sed -i "/^DIR_HIGHER /cDIR_HIGHER = trend/" $PARAM
        # sed -i "/^DIR_MASK /cDIR_MASK = mask/" $PARAM
        # sed -i "/^BASE_MASK /cBASE_MASK = aoi.tif" $PARAM
        # sed -i "/^FILE_ENDMEM /cFILE_ENDMEM = $ENDMEMBER" $PARAM
        # sed -i "/^FILE_TILE /cFILE_TILE = $TILE" $PARAM

        # # threading
        # sed -i "/^NTHREAD_READ /cNTHREAD_READ = 4" $PARAM
        # sed -i "/^NTHREAD_COMPUTE /cNTHREAD_COMPUTE = 104" $PARAM
        # sed -i "/^NTHREAD_WRITE /cNTHREAD_WRITE = 4" $PARAM

        # # extent and resolution
        # sed -i "/^X_TILE_RANGE /cX_TILE_RANGE = $XMIN $XMAX" $PARAM
        # sed -i "/^Y_TILE_RANGE /cY_TILE_RANGE = $YMIN $YMAX" $PARAM
        # sed -i "/^RESOLUTION /cRESOLUTION = 30" $PARAM

        # # sensors
        # sed -i "/^SENSORS /cSENSORS = LND04 LND05 LND07" $PARAM

        # # date range
        # # TODO: dates dont have the same format
        # sed -i "/^DATE_RANGE /cDATE_RANGE = $START_DATE $END_DATE" $PARAM

        # # spectral index
        # sed -i "/^INDEX /cINDEX = SMA" $PARAM

        # # interpolation
        # sed -i "/^INT_DAY /cINT_DAY = 8" $PARAM
        # sed -i "/^OUTPUT_TSI /cOUTPUT_TSI = TRUE" $PARAM

        # # polar metrics
        # sed -i "/^POL /cPOL = VPS VBL VSA" $PARAM
        # sed -i "/^OUTPUT_POL /cOUTPUT_POL = TRUE" $PARAM
        # sed -i "/^OUTPUT_TRO /cOUTPUT_TRO = TRUE" $PARAM
        # sed -i "/^OUTPUT_CAO /cOUTPUT_CAO = TRUE" $PARAM

        # date
        # echo "DONE"
        # date
            # """],

        # security_context=security_context,
        # resources=compute_resources,
        # volumes=[volume],
        # volume_mounts=[volume_mount],
        # env_vars={
            # 'DATA':image_folderpath,
            # 'CUBEFILE':datacube_filepath,
            # 'DEM':dem_folderpath,
            # 'WVDB':wvdb,
            # 'TILE':allowed_tiles_filepath,
            # 'NTHREAD':'2',
            # 'PARAM':"tsa.prm"
            # 'ENDMEMBER':'
            # },
        # get_logs=True,
        # dag=dag
        # )

    dag_start = DummyOperator(task_id='Start', dag=dag)
    wait_for_downloads = DummyOperator(task_id='wait_for_downloads', dag=dag)
    
    # dag_start >> download_auxiliary >> download_level_1 >> wait_for_downloads >> generate_allowed_tiles >> generate_analysis_mask >> preprocess_level2

    # TODO: Make a separate download dag with common environment variables
    # TODO: Check thread and cpu assignment

    

    wait_for_downloads.set_upstream(dag_start)
    generate_allowed_tiles.set_upstream(wait_for_downloads)
    generate_analysis_mask.set_upstream(wait_for_downloads)
    prepare_level2.set_upstream(generate_allowed_tiles)
    prepare_level2.set_upstream(generate_analysis_mask)
    for task in preprocess_level2_tasks:
        task.set_upstream(prepare_level2)
