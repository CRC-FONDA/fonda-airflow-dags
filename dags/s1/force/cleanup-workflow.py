from datetime import date, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.cncf.kubernetes.operators.pod import \
    KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

OUTPUTS_DATA_PATH = "/data/outputs"

namespace = "default"

compute_resources = k8s.V1ResourceRequirements(
    requests={
        "cpu": "1000m",
        "memory": "1Gi"
    },
    limits={
        "cpu": "1000m",
        "memory": "1Gi"
    }
)

dataset_volume = k8s.V1Volume(
    name="eo-data",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="fonda-datasets"
    ),
)

dataset_volume_mount = k8s.V1VolumeMount(
    name="eo-data", mount_path="/data/input", sub_path=None, read_only=True
)

outputs_volume = k8s.V1Volume(
    name="outputs-data",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="force-airflow"
    ),
)

outputs_volume_mount = k8s.V1VolumeMount(
    name="outputs-data", mount_path=OUTPUTS_DATA_PATH, sub_path=None, read_only=False
)

security_context = k8s.V1SecurityContext(run_as_user=0)

experiment_affinity = {
    "nodeAffinity": {
        # requiredDuringSchedulingIgnoredDuringExecution means in order
        # for a pod to be scheduled on a node, the node must have the
        # specified labels. However, if labels on a node change at
        # runtime such that the affinity rules on a pod are no longer
        # met, the pod will still continue to run on the node.
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [
                {
                    "matchExpressions": [
                        {
                            # When nodepools are created in Google Kubernetes
                            # Engine, the nodes inside of that nodepool are
                            # automatically assigned the label
                            # 'cloud.google.com/gke-nodepool' with the value of
                            # the nodepool's name.
                            "key": "usedby",
                            "operator": "In",
                            # The label key's value that pods can be scheduled
                            # on.
                            "values": [
                                "vasilis",
                            ],
                        }
                    ]
                }
            ]
        }
    }
}

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
    "force-cleanup",
    default_args=default_args,
    description="Cleanup workflow for force in Airflow",
    schedule_interval="@once",
    start_date=days_ago(2),
    tags=["force-cleanup"],
    max_active_runs=1,
) as dag:

    cleanup = KubernetesPodOperator(
        name="cleanup",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        labels={"workflow": "force"},
        task_id="cleanup",
        cmds=["/bin/sh", "-c"],
        arguments=[
            """
            rm -rf $OUTPUTS/level2*
            rm -rf $OUTPUTS/masks
            rm -rf $OUTPUTS/mosaic
            rm -rf $OUTPUTS/queue_files
            rm -rf $OUTPUTS/param_files
            rm -rf $OUTPUTS/trends
            """
        ],
        security_context=security_context,
        container_resources=compute_resources,
        volumes=[dataset_volume, outputs_volume],
        volume_mounts=[dataset_volume_mount, outputs_volume_mount],
        affinity=experiment_affinity,
        env_vars={
            "OUTPUTS": OUTPUTS_DATA_PATH,
        },
        get_logs=True,
        dag=dag,
    )
