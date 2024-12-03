from datetime import timedelta
import random
import time
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

# Kubernetes config: namespace, resources, volume and volume_mounts
namespace = "default"

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

# Define resources for CPU-intensive tasks: 1 CPU requested and limited
cpu_intensive_resources = k8s.V1ResourceRequirements(
    requests={
        "cpu": "1",  # Request exactly 1 CPU
        "memory": "1Gi",  # Request 1Gi of memory
    },
    limits={
        "cpu": "1",  # Limit the task to 1 CPU
        "memory": "1Gi",  # Limit memory to 1Gi
    }
)

# Define resources for RAM-intensive tasks: 2 GB requested and limited
ram_intensive_resources = k8s.V1ResourceRequirements(
    requests={
        "cpu": "0.5",  # Request 0.5 CPU
        "memory": "2Gi",  # Request 2Gi of memory
    },
    limits={
        "cpu": "0.5",  # Limit the task to 0.5 CPU
        "memory": "2Gi",  # Limit memory to 2Gi
    }
)

# Define resources for combined CPU and RAM intensive tasks: 0.5 CPU and 1 GB RAM each
combined_resources = k8s.V1ResourceRequirements(
    requests={
        "cpu": "0.5",  # Request 0.5 CPU
        "memory": "1Gi",  # Request 1Gi of memory
    },
    limits={
        "cpu": "0.5",  # Limit the task to 0.5 CPU
        "memory": "1Gi",  # Limit memory to 1Gi
    }
)

# Define a security context
security_context = k8s.V1SecurityContext(run_as_user=0)

# DAG definition
default_args = {
    "owner": "Your Name",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "cpu_and_ram_intensive_tasks_with_dependencies",
    default_args=default_args,
    description="A DAG with CPU and RAM intensive tasks and dependencies",
    schedule_interval="@once",
    start_date=days_ago(1),
    max_active_tasks=5,
    tags=["example"],
) as dag:

    # CPU-intensive tasks with varying durations
    cpu_tasks = []
    for i in range(1, 4):
        cpu_task = KubernetesPodOperator(
            name=f"cpu_intensive_task_{i}",
            namespace=namespace,
            image="python:3.8-slim",
            labels={"workflow": "cpu_intensive_task"},
            task_id=f"cpu_intensive_task_{i}",
            cmds=["python", "-c"],
            affinity=experiment_affinity,
            arguments=[
                f"""
import time
import random

def cpu_bound_task():
    result = 0
    duration = random.randint(5, 15) * 60  # Randomly choose between 5 and 15 minutes
    start_time = time.time()
    while time.time() - start_time < duration:
        for i in range(1, 1000000):
            result += i
    print(f"Task ran for {{duration // 60}} minutes with result: {{result}}")

if __name__ == "__main__":
    cpu_bound_task()
                """
            ],
            security_context=security_context,
            container_resources=cpu_intensive_resources,
            get_logs=True,
            dag=dag,
        )
        cpu_tasks.append(cpu_task)

    # RAM-intensive tasks with fixed duration of 5 minutes
    ram_tasks = []
    for i in range(1, 3):
        ram_task = KubernetesPodOperator(
            name=f"ram_intensive_task_{i}",
            namespace=namespace,
            image="python:3.8-slim",
            labels={"workflow": "ram_intensive_task"},
            task_id=f"ram_intensive_task_{i}",
            cmds=["python", "-c"],
            affinity=experiment_affinity,
            arguments=[
                """
import time

def ram_bound_task():
    duration = 5 * 60  # Run for 5 minutes
    large_list = []
    # Allocate memory by filling a large list
    for _ in range(50000000):  # Adjust this number to control memory usage
        large_list.append(" " * 100)  # Each entry takes about 100 bytes
    start_time = time.time()
    while time.time() - start_time < duration:
        large_list = [x.replace(" ", " ") for x in large_list]  # Keep the memory active
    print("RAM intensive task completed")

if __name__ == "__main__":
    ram_bound_task()
                """
            ],
            security_context=security_context,
            container_resources=ram_intensive_resources,
            get_logs=True,
            dag=dag,
        )
        ram_tasks.append(ram_task)

    # Combined CPU and RAM intensive tasks that wait for all others to finish
    combined_task_1 = KubernetesPodOperator(
        name="combined_intensive_task_1",
        namespace=namespace,
        image="python:3.8-slim",
        labels={"workflow": "combined_intensive_task"},
        task_id="combined_intensive_task_1",
        cmds=["python", "-c"],
        arguments=[
            """
import time

def combined_task():
    duration = 5 * 60  # Run for 5 minutes
    large_list = []
    # Allocate memory by filling a large list
    for _ in range(25000000):  # Adjust this number to control memory usage
        large_list.append(" " * 100)  # Each entry takes about 100 bytes
    start_time = time.time()
    while time.time() - start_time < duration:
        large_list = [x.replace(" ", " ") for x in large_list]  # Keep the memory active
        for i in range(1, 1000000):  # CPU operation
            pass
    print("Combined CPU and RAM intensive task completed")

if __name__ == "__main__":
    combined_task()
            """
        ],
        security_context=security_context,
        container_resources=combined_resources,
        get_logs=True,
        dag=dag,
    )

    combined_task_2 = KubernetesPodOperator(
        name="combined_intensive_task_2",
        namespace=namespace,
        image="python:3.8-slim",
        labels={"workflow": "combined_intensive_task"},
        task_id="combined_intensive_task_2",
        cmds=["python", "-c"],
        affinity=experiment_affinity,
        arguments=[
            """
import time

def combined_task():
    duration = 5 * 60  # Run for 5 minutes
    large_list = []
    # Allocate memory by filling a large list
    for _ in range(25000000):  # Adjust this number to control memory usage
        large_list.append(" " * 100)  # Each entry takes about 100 bytes
    start_time = time.time()
    while time.time() - start_time < duration:
        large_list = [x.replace(" ", " ") for x in large_list]  # Keep the memory active
        for i in range(1, 1000000):  # CPU operation
            pass
    print("Combined CPU and RAM intensive task completed")

if __name__ == "__main__":
    combined_task()
            """
        ],
        security_context=security_context,
        container_resources=combined_resources,
        get_logs=True,
        dag=dag,
    )

    # Set dependencies: all CPU and RAM intensive tasks must complete before combined tasks run
    combined_task_1.set_upstream(cpu_tasks + ram_tasks)
    combined_task_2.set_upstream(cpu_tasks + ram_tasks)

