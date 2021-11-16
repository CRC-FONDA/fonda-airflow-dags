
    def will_download(*op_args):
        download_run_parameter = op_args[0]
        if download_run_parameter == "True":
            download = True

        if download == True:
            return start_downloads.task_id
        else:
            return skip_downloads.task_id

    # Decides if we'll do the downloading tasks or not
    branch_downloads = BranchPythonOperator(
        task_id="branch_downloads",
        python_callable=will_download,
        op_args=["{{ dag_run.conf['download'] }}"],
    )

    # Downloads auxiliary data
    download_auxiliary = KubernetesPodOperator(
        name="download_auxiliary",
        namespace=namespace,
        task_id="download_auxiliary",
        image="bash",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"wget -O auxiliary.tar.gz https://box.hu-berlin.de/f/c4d90fc5b07c4955b979/?dl=1 && tar -xzf auxiliary.tar.gz && cp -r EO-01/input {MOUNT_DATA_PATH}"
        ],
        resources=compute_resources,
        volumes=[datasets_volume, outputs_volume],
        security_context=security_context,
        volume_mounts=[datasets_volume_mount, outputs_volume_mount],
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
            sed -i -e 's=^mozilla/DST_Root_CA_X3.crt=!mozilla/DST_ROOT_CA_X3.crt=' /etc/ca-certificates.conf && \
            cd /usr/local/share/ca-certificates && \
            wget https://letsencrypt.org/certs/isrgrootx1.pem && \
            wget https://letsencrypt.org/certs/isrg-root-x2.pem && \
            update-ca-certificates --fresh && \
            mkdir -p {image_folderpath} && \
            force-level1-csd -s {sensors_level1} -d {daterange} -c 0,70 {image_metadata_folderpath} {image_folderpath} {queue_filepath} {aoi_filepath}"
        ],
        resources=compute_resources,
        volumes=[datasets_volume, outputs_volume],
        volume_mounts=[datasets_volume_mount, outputs_volume_mount],
        security_context=security_context,
        get_logs=True,
        dag=dag,
    )



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
