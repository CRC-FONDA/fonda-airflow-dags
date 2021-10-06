from datetime import date, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

# Working environment variables
MOUNT_DATA_PATH = "/data"
aoi_filepath = MOUNT_DATA_PATH + "/input/vector/aoi.gpkg"
datacube_folderpath = MOUNT_DATA_PATH + "/input/grid"
datacube_filepath = datacube_folderpath + "/" + "datacube-definition.prj"
image_folderpath = MOUNT_DATA_PATH + "/image_data"
image_metadata_folderpath = MOUNT_DATA_PATH + "/image_metadata"
allowed_tiles_filepath = MOUNT_DATA_PATH + "/allowed_tiles.txt"
dem_folderpath = MOUNT_DATA_PATH + "/input/dem"
wvdb = MOUNT_DATA_PATH + "/input/wvdb"
masks_folderpath = MOUNT_DATA_PATH + "/masks"
endmember_filepath = MOUNT_DATA_PATH + "/input/endmember/hostert-2003.txt"
queue_filepath = MOUNT_DATA_PATH + "/queue.txt"
ard_folderpath = MOUNT_DATA_PATH + "/level2_ard"
trends_folderpath = MOUNT_DATA_PATH + "/trends"
mosaic_folderpath = MOUNT_DATA_PATH + "/mosaic"

# HYPERPARAMETERS AND RUN SPECIFIC PARAMETERS
# What sensors we're getting the lvl1 data from
sensors_level1 = "LT04,LT05,LE07,S2A"
start_date = date(2006, 1, 1)
end_date = date(2012, 1, 1)
daterange = start_date.strftime("%Y%m%d") + "," + end_date.strftime("%Y%m%d")
mask_resolution = 30

num_of_tiles = 28
parallel_factor = (
    245  # Parallel factor is practically how many images are to be processed
)
download = False
num_of_filters = 10
num_of_pyramid_tasks_per_tile = 10
# You have to assert that the number of pyramids tasks per tile is smaller tyhan the number of the actual filters

# Kubernetes config: namespace, resources, volume and volume_mounts
namespace = "airflow"

compute_resources = {
    "request_cpu": "1000m",
    "request_memory": "3Gi",
    "limit_cpu": "2000m",
    "limit_memory": "4Gi",
}

volume = k8s.V1Volume(
    name="force-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="force-pvc"
    ),
)

volume_mount = k8s.V1VolumeMount(
    name="force-volume", mount_path=MOUNT_DATA_PATH, sub_path=None, read_only=False
)

security_context = k8s.V1SecurityContext(run_as_user=0)

# DAG
default_args = {
    "owner": "FONDA S1",
    "depends_on_past": False,
    "email": ["vasilis.bountris@informatik.hu-berlin.de"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=100),
}

with DAG(
    "force",
    default_args=default_args,
    description="Airflow implementation of a FORCE workflow",
    schedule_interval="@once",
    start_date=days_ago(2),
    tags=["force"],
    max_active_runs=1,
    concurrency=10,
) as dag:

    def will_download():
        if download == True:
            return start_downloads.task_id
        else:
            return skip_downloads.task_id

    # Decides if we'll do the downloading tasks or not
    branch_downloads = BranchPythonOperator(
        task_id="branch_downloads", python_callable=will_download
    )

    # Downloads auxiliary data
    download_auxiliary = KubernetesPodOperator(
        name="download_auxiliary",
        namespace=namespace,
        task_id="download_auxiliary",
        image="bash",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"wget -O auxiliary.tar.gz https://box.hu-berlin.de/f/eb61444bd97f4c738038/?dl=1 && tar -xzf auxiliary.tar.gz && cp -r EO-01/input {MOUNT_DATA_PATH}"
        ],
        resources=compute_resources,
        volumes=[volume],
        security_context=security_context,
        volume_mounts=[volume_mount],
        get_logs=True,
        dag=dag,
    )

    # Downloads level 1 data
    download_level_1 = KubernetesPodOperator(
        name="download_level_1",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        task_id="download_level_1",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"mkdir -p {image_metadata_folderpath} && \
            force-level1-csd -u -s {sensors_level1} {image_metadata_folderpath} && \
              mkdir -p {image_folderpath} && \
              force-level1-csd -s {sensors_level1} -d {daterange} -c 0,70 {image_metadata_folderpath} {image_folderpath} {queue_filepath} {aoi_filepath}"
        ],
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        security_context=security_context,
        get_logs=True,
        dag=dag,
    )

    generate_allowed_tiles = KubernetesPodOperator(
        name="generate_allowed_tiles",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        task_id="generate_allowed_tiles",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"force-tile-extent {aoi_filepath} {datacube_folderpath} {allowed_tiles_filepath}"
        ],
        security_context=security_context,
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        dag=dag,
    )

    generate_analysis_mask = KubernetesPodOperator(
        name="generate_analysis_mask",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        task_id="generate_analysis_mask",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"mkdir -p {masks_folderpath} && cp {datacube_folderpath}/datacube-definition.prj {masks_folderpath} && force-cube {aoi_filepath} {masks_folderpath} rasterize {mask_resolution}"
        ],
        security_context=security_context,
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        dag=dag,
    )

    prepare_level2 = KubernetesPodOperator(
        name="prepare_level2",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        task_id="prepare_level2",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"""mkdir -p /data/queue_files
        split -a 3 -l$((`wc -l < /data/queue.txt`/{parallel_factor})) --numeric-suffixes=0 /data/queue.txt /data/queue_files/queue_ --additional-suffix=.txt
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
        sed -i "/^NPROC /cNPROC = 1" $PARAM
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
            """
        ],
        security_context=security_context,
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars={
            "DATA": image_folderpath,
            "CUBEFILE": datacube_filepath,
            "DEM": dem_folderpath,
            "WVDB": wvdb,
            "TILE": allowed_tiles_filepath,
            "NTHREAD": "2",
            "PARAM": "/data/param_files/ard.prm",
        },
        get_logs=True,
        dag=dag,
    )

    preprocess_level2_tasks = []
    for i in range(parallel_factor):
        index = f"{i:03d}"
        preprocess_level2_task = KubernetesPodOperator(
            name="preprocess_level2_" + index,
            namespace=namespace,
            image="davidfrantz/force:3.6.5",
            task_id="preprocess_level2_" + index,
            cmds=["/bin/sh", "-c"],
            arguments=[
                """\
            echo "STARTING LEVEL2 PROCESSING"
            cp $GLOBAL_PARAM $PARAM
            sed -i "/^FILE_QUEUE /cFILE_QUEUE = $QUEUE_FILE" $PARAM
            date
            force-level2 $PARAM
            echo "DONE"
            date
                """
            ],
            security_context=security_context,
            resources=compute_resources,
            volumes=[volume],
            volume_mounts=[volume_mount],
            env_vars={
                "GLOBAL_PARAM": "/data/param_files/ard.prm",
                "PARAM": f"/data/param_files/ard_{index}.prm",
                "QUEUE_FILE": f"/data/queue_files/queue_{index}.txt",
            },
            get_logs=True,
            dag=dag,
            retries=5,
            retry_delay=timedelta(minutes=10),
        )
        preprocess_level2_tasks.append(preprocess_level2_task)

    prepare_tsa = KubernetesPodOperator(
        name="prepape_tsa",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        task_id="prepare_tsa",
        cmds=["/bin/sh", "-c"],
        arguments=[
            """\
        force-parameter . TSA 0
        mv TSA-skeleton.prm $PARAM
        mkdir -p $TRENDS_FOLDER

        # paths
        sed -i "/^DIR_LOWER /cDIR_LOWER = $ARD_FOLDER" $PARAM
        sed -i "/^DIR_HIGHER /cDIR_HIGHER = $TRENDS_FOLDER" $PARAM
        sed -i "/^DIR_MASK /cDIR_MASK = $MASKS_FOLDER" $PARAM
        sed -i "/^FILE_ENDMEM /cFILE_ENDMEM = $ENDMEMBER" $PARAM
        sed -i "/^FILE_TILE /cFILE_TILE = $TILE" $PARAM

        # threading
        sed -i "/^NTHREAD_READ /cNTHREAD_READ = 1" $PARAM
        sed -i "/^NTHREAD_COMPUTE /cNTHREAD_COMPUTE = 2" $PARAM
        sed -i "/^NTHREAD_WRITE /cNTHREAD_WRITE = 1" $PARAM

        # resolution
        sed -i "/^RESOLUTION /cRESOLUTION = 30" $PARAM

        # sensors
        sed -i "/^SENSORS /cSENSORS = LND04 LND05 LND07" $PARAM

        # date range
        sed -i "/^DATE_RANGE /cDATE_RANGE = $START_DATE $END_DATE" $PARAM

        # spectral index
        sed -i "/^INDEX /cINDEX = SMA" $PARAM

        # interpolation
        sed -i "/^INT_DAY /cINT_DAY = 8" $PARAM
        sed -i "/^OUTPUT_TSI /cOUTPUT_TSI = TRUE" $PARAM

        # polar metrics
        sed -i "/^POL /cPOL = VPS VBL VSA" $PARAM
        sed -i "/^OUTPUT_POL /cOUTPUT_POL = TRUE" $PARAM
        sed -i "/^OUTPUT_TRO /cOUTPUT_TRO = TRUE" $PARAM
        sed -i "/^OUTPUT_CAO /cOUTPUT_CAO = TRUE" $PARAM
        
        cp $PARAM /data/param_files/

        echo "DONE"
            """
        ],
        security_context=security_context,
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars={
            "DATA": image_folderpath,
            "CUBEFILE": datacube_filepath,
            "DEM": dem_folderpath,
            "WVDB": wvdb,
            "TILE": allowed_tiles_filepath,
            "NTHREAD": "2",
            "PARAM": "tsa.prm",
            "ENDMEMBER": endmember_filepath,
            "ARD_FOLDER": ard_folderpath,
            "TRENDS_FOLDER": trends_folderpath,
            "MASKS_FOLDER": masks_folderpath,
            "AOI_PATH": aoi_filepath,
            "START_DATE": start_date.isoformat(),
            "END_DATE": end_date.isoformat(),
        },
        get_logs=True,
        dag=dag,
    )

    tsa_tasks = []
    for i in range(2, num_of_tiles + 2):
        index = f"{i:03d}"
        tsa_task = KubernetesPodOperator(
            name="tsa_task_" + index,
            namespace=namespace,
            image="davidfrantz/force:3.6.5",
            task_id="tsa_task_" + index,
            cmds=["/bin/bash", "-c"],
            arguments=[
                f"""\
              echo "STARTING TIME SERIES ANALYSIS"
              cp $GLOBAL_PARAM $PARAM
              # Get the corresponding line from the allowed tiles file
              TILE=`sed '{index}q;d' $TILE_FILE` 
              X=${{TILE:1:4}}
              Y=${{TILE:7:11}}
              sed -i "/^BASE_MASK /cBASE_MASK = aoi.tif" $PARAM
              sed -i "/^X_TILE_RANGE /cX_TILE_RANGE = $X $X" $PARAM
              sed -i "/^Y_TILE_RANGE /cY_TILE_RANGE = $Y $Y" $PARAM
              force-higher-level $PARAM
              echo "DONE"

              # Push results to xcom
              mkdir -p /airflow/xcom/
              
              # Find *.tif files and store them in a list of files
              cd /data/trends/$TILE
              files=`find *.tif | tr '\n' ','`
              # Add Brackets
              files='['$files']'
              # Make json
              echo '{{"tile":"'$TILE'", "files":"'$files'"}}' 
              echo '{{"tile":"'$TILE'", "files":"'$files'"}}' > /airflow/xcom/return.json

                  """
            ],
            security_context=security_context,
            resources=compute_resources,
            volumes=[volume],
            volume_mounts=[volume_mount],
            do_xcom_push=True,
            env_vars={
                "GLOBAL_PARAM": "/data/param_files/tsa.prm",
                "PARAM": f"/data/param_files/tsa_{index}.prm",
                "TILE_FILE": allowed_tiles_filepath,
            },
            get_logs=True,
            dag=dag,
        )
        tsa_tasks.append(tsa_task)

    pyramid_tasks_per_tile = []
    for i in range(2, num_of_tiles + 2):
        # TODO: pyramids could and should be tried in some form of batches
        pyramid_tasks_in_tile = []
        for j in range(num_of_filters):

            pyramid_task_index = i * num_of_filters + j
            file_index = str(j + 1)
            tile_index = f"{i:03d}"
            index = f"{pyramid_task_index:03d}"
            pyramid_task = KubernetesPodOperator(
                name="pyramid_task_" + index,
                namespace=namespace,
                image="davidfrantz/force:3.6.5",
                task_id="pyramid_task_" + index,
                cmds=["/bin/bash", "-c"],
                arguments=[
                    f"""\
                            TILE=\"{{{{ task_instance.xcom_pull('tsa_task_{tile_index}')[\"tile\"] }}}}\"
                            FILES=\"{{{{ task_instance.xcom_pull('tsa_task_{tile_index}')[\"files\"] }}}}\"
                            CHOSEN_FILE=`echo $FILES | sed 's/[][]//g' | cut -d "," -f $FILE_INDEX`
                            FILES_TO_DO="${{TRENDS_FOLDERPATH}}/${{TILE}}/${{CHOSEN_FILE}}"
                            force-pyramid $FILES_TO_DO
                  """
                ],
                security_context=security_context,
                resources=compute_resources,
                volumes=[volume],
                volume_mounts=[volume_mount],
                env_vars={
                    "INDEX": index,
                    "TRENDS_FOLDERPATH": trends_folderpath,
                    "FILE_INDEX": file_index,
                },
                get_logs=True,
                dag=dag,
            )
            pyramid_tasks_in_tile.append(pyramid_task)
        pyramid_tasks_per_tile.append(pyramid_tasks_in_tile)

    wait_for_trends = KubernetesPodOperator(
        name="wait_for_trends",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        task_id="wait_for_trends",
        cmds=["/bin/bash", "-c"],
        arguments=[
            """\
            cd $TRENDS_FOLDERPATH
            UNIQUE_BASENAMES=`find . -name '*.tif' -exec basename {} \; | sort | uniq`
            COUNTER=0
            for i in $UNIQUE_BASENAMES
            do
              mkdir -p $DATA_FOLDERPATH/$COUNTER
              FILES_TO_MOVE=`find . -name $i | cut -c 2-`
              for FILE in $FILES_TO_MOVE
              do
                TILE=`dirname $FILE`
                echo $TILE
                echo $FILE
                mkdir -p $DATA_FOLDERPATH/$COUNTER$TILE
                ln .$FILE $DATA_FOLDERPATH/$COUNTER$FILE
              done
              let COUNTER++
            done 
            """
        ],
        security_context=security_context,
        resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars={
            "TRENDS_FOLDERPATH": trends_folderpath,
            "DATA_FOLDERPATH": mosaic_folderpath,
        },
        get_logs=True,
        dag=dag,
    )

    mosaic_tasks = []
    for i in range(10):
        index = f"{i:01d}"
        mosaic_task = KubernetesPodOperator(
            name="mosaic_task_" + index,
            namespace=namespace,
            image="davidfrantz/force:3.6.5",
            task_id="mosaic_task_" + index,
            cmds=["/bin/bash", "-c"],
            arguments=[
                f"""\
                      force-mosaic $MOSAIC_FOLDERPATH/$INDEX
                  """
            ],
            security_context=security_context,
            resources=compute_resources,
            volumes=[volume],
            volume_mounts=[volume_mount],
            env_vars={
                "INDEX": index,
                "MOSAIC_FOLDERPATH": mosaic_folderpath,
            },
            get_logs=True,
            dag=dag,
        )
        mosaic_tasks.append(mosaic_task)

    dag_start = DummyOperator(task_id="Start", dag=dag)
    start_downloads = DummyOperator(task_id="start_downloads", dag=dag)
    skip_downloads = DummyOperator(task_id="skip_downloads", dag=dag)
    wait_for_downloads = DummyOperator(task_id="wait_for_downloads", dag=dag)
    downloads_completed = DummyOperator(
        task_id="downloads_completed", dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS
    )

    branch_downloads.set_upstream(dag_start)
    branch_downloads >> [start_downloads, skip_downloads]

    download_level_1.set_upstream(start_downloads)
    download_auxiliary.set_upstream(start_downloads)
    wait_for_downloads.set_upstream(download_level_1)
    wait_for_downloads.set_upstream(download_auxiliary)
    downloads_completed.set_upstream(wait_for_downloads)
    downloads_completed.set_upstream(skip_downloads)
    generate_allowed_tiles.set_upstream(downloads_completed)
    generate_analysis_mask.set_upstream(downloads_completed)
    prepare_level2.set_upstream(generate_allowed_tiles)
    prepare_level2.set_upstream(generate_analysis_mask)
    for task in preprocess_level2_tasks:
        task.set_upstream(prepare_level2)
        task.set_downstream(prepare_tsa)

    for tsa_task, zzz in zip(tsa_tasks, pyramid_tasks_per_tile):
        tsa_task.set_upstream(prepare_tsa)
        tsa_task.set_downstream(wait_for_trends)
        # Start pyramid batch of pyramid tasks for every tile.
        tsa_task.set_downstream(zzz)

    for task in mosaic_tasks:
        task.set_upstream(wait_for_trends)
