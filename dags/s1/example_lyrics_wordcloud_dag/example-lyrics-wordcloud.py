from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.pod import \
    KubernetesPodOperator
from airflow.utils.dates import days_ago
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


# helper function
def create_sublists(original_list, amount):
    sublists = [[] for _ in range(amount)]
    for i, item in enumerate(original_list):
        sublists[i % amount].append(item)
    return sublists


dag = DAG(
    'example_lyrics_wordcloud',
    default_args=default_args,
    schedule_interval='@once',
    max_active_runs=1,
    concurrency=10
)

#
# Kubernetes config: namespace, resources, volume and volume_mounts
#
namespace = "airflow"
compute_resources = k8s.V1ResourceRequirements(
    requests={
        "cpu": "200m",
        "memory": "512Mi"
    },
    limits={
        "cpu": "500m",
        "memory": "1Gi"
    }
)

#
# Use this for local testing
#
# volume = k8s.V1Volume(
#    name="lyric-wordcloud-volume",
#    host_path=k8s.V1HostPathVolumeSource(
#        path="/data",
#        type="Directory"
#    )
# )
# Define the secret reference
env_from_secret = k8s.V1EnvFromSource(
    secret_ref=k8s.V1SecretEnvSource(
        name='genius-api-key-secret'  # Name of the Kubernetes secret
    )
)


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

# lyrics-wordcloud-volume is an existing PersistentVolumeClaim in the CephFS storage
volume = k8s.V1Volume(
    name="lyrics-wordcloud-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='lyrics-wordcloud-volume')
)

# describes where to mount the volume in the pod
volume_mount = k8s.V1VolumeMount(
    name="lyrics-wordcloud-volume",
    mount_path="/results",
    sub_path=None,
    read_only=False
)

# TODO: Read from secret
env_vars = {'GENIUS_API_KEY': 'XXXXXXXXXXXXXXXXXXXXXXXXXX'}

#
# List of artists to create lyric wordlcouds for
artists = ["Rammstein", "Die Ärzte", "Die Toten Hosen", "Peter Maffay", "Nimo", "Mark Forster", "Lea"]

#
# Amount of worker pods
#
download_pods = 3  # use three worker-pods to download lyrics
wordcloud_pods = 2  # use two worker-pods to create wordclouds

#
# Start tasks definitions
#
with dag:
    # Dummy start tasks
    start = DummyOperator(task_id="Start")

    #
    # Create download_lyrics tasks
    #
    download_tasks_list = []
    download_task = 1
    for artists in create_sublists(artists, download_pods):
        names = ",".join(artists)
        download_lyrics = KubernetesPodOperator(
            name="download_data_{}".format(download_task),
            namespace=namespace,
            image="srnbckr/wordcloud",
            cmds=["python", "get_lyrics.py",
                  "--artists",
                  names,
                  "--max-songs",
                  "30"],
            task_id="download_lyrics_{}".format(download_task),
            container_resources=compute_resources,
            volumes=[volume],
            volume_mounts=[volume_mount],
            env_from=[env_from_secret],  # Use env_from instead of env_vars
            get_logs=True,
            affinity=experiment_affinity,
            dag=dag)
        download_task += 1
        # Set downstream tasks
        download_tasks_list.append(download_lyrics)

    #
    # Create create_wordcloud tasks
    #
    wordcloud_task_list = []
    wordcloud_task = 1
    for artists in create_sublists(artists, wordcloud_pods):
        names = ",".join(artists)
        create_wordcloud = KubernetesPodOperator(
            name="wordcloud_{}".format(wordcloud_task),
            namespace=namespace,
            image="srnbckr/wordcloud",
            cmds=["python", "create_wordcloud.py",
                  "--artists",
                  names],
            task_id="create_wordcloud_{}".format(wordcloud_task),
            container_resources=compute_resources,
            volumes=[volume],
            volume_mounts=[volume_mount],
            get_logs=True,
            dag=dag
        )
        wordcloud_task += 1
        wordcloud_task_list.append(create_wordcloud)

    #
    # Create merge_images tasks
    #
    merge_images = KubernetesPodOperator(
        name="merge_images",
        namespace=namespace,
        image="srnbckr/wordcloud",
        cmds=["python", "merge_images.py"],
        task_id="merge_images",
        container_resources=compute_resources,
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        dag=dag
    )

    #
    # Set dependencies
    #
    merge_images.set_upstream(wordcloud_task_list)
    for task in wordcloud_task_list:
        # create_wordcloud tasks should wait for every download_lyrics tasks to finish
        task.set_upstream(download_tasks_list)
    for task in download_tasks_list:
        # Begin with dummy task
        task.set_upstream(start)
