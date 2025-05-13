from datetime import timedelta
import random
import time
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

# Kubernetes config: namespace, resources, volume and volume_mounts
namespace = "default"

experiment_affinity = {
    "nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [
                {
                    "matchExpressions": [
                        {
                            "key": "usedby",
                            "operator": "In",
                            "values": ["vasilis"],
                        }
                    ]
                }
            ]
        }
    }
}

cpu_intensive_resources = k8s.V1ResourceRequirements(
    requests={"cpu": "1", "memory": "1Gi"},
    limits={"cpu": "1", "memory": "1Gi"}
)

ram_intensive_resources = k8s.V1ResourceRequirements(
    requests={"cpu": "0.5", "memory": "2Gi"},
    limits={"cpu": "0.5", "memory": "2Gi"}
)

combined_resources = k8s.V1ResourceRequirements(
    requests={"cpu": "0.5", "memory": "1Gi"},
    limits={"cpu": "0.5", "memory": "1Gi"}
)

security_context = k8s.V1SecurityContext(run_as_user=0)

default_args = {
    "owner": "Your Name",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
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

    cpu_tasks = []
    for i in range(1, 4):
        cpu_task = KubernetesPodOperator(
            name=f"cpu_intensive_task_{i}",
            affinity=experiment_affinity,
            namespace=namespace,
            image="python:3.8-slim",
            task_id=f"cpu_intensive_task_{i}",
            cmds=["python", "-c"],
            arguments=[
                """
import time
import random

start_time = time.time()
duration = random.randint(1, 2) * 60
while time.time() - start_time < duration:
    pass
print("CPU intensive task completed")
"""
            ],
            security_context=security_context,
            container_resources=cpu_intensive_resources,
            get_logs=True,
            dag=dag,
        )
        cpu_tasks.append(cpu_task)

    ram_tasks = []
    for i in range(1, 3):
        ram_task = KubernetesPodOperator(
            name=f"ram_intensive_task_{i}",
            affinity=experiment_affinity,
            namespace=namespace,
            image="python:3.8-slim",
            task_id=f"ram_intensive_task_{i}",
            cmds=["python", "-c"],
            arguments=[
                """
import time

start_time = time.time()
large_list = [" " * 100 for _ in range(50000000)]
duration = 1 * 60
while time.time() - start_time < duration:
    pass
print("RAM intensive task completed")
"""
            ],
            security_context=security_context,
            container_resources=ram_intensive_resources,
            get_logs=True,
            dag=dag,
        )
        ram_tasks.append(ram_task)

    combined_task_1 = KubernetesPodOperator(
        name="combined_intensive_task_1",
        namespace=namespace,
        affinity=experiment_affinity,
        image="python:3.8-slim",
        task_id="combined_intensive_task_1",
        cmds=["python", "-c"],
        arguments=[
            """
import time

start_time = time.time()
duration = 1 * 60
while time.time() - start_time < duration:
    pass
print("Combined intensive task 1 completed")
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
        affinity=experiment_affinity,
        image="python:3.8-slim",
        task_id="combined_intensive_task_2",
        cmds=["python", "-c"],
        arguments=[
            """
import time

start_time = time.time()
duration = 1 * 60
while time.time() - start_time < duration:
    pass
print("Combined intensive task 2 completed")
"""
        ],
        security_context=security_context,
        container_resources=combined_resources,
        get_logs=True,
        dag=dag,
    )

    combined_task_1.set_upstream(cpu_tasks + ram_tasks)
    combined_task_2.set_upstream(cpu_tasks + ram_tasks)

    failing_tasks = []
    for i in range(1, 4):
        failing_task = KubernetesPodOperator(
            name=f"failing_task_{i}",
            affinity=experiment_affinity,
            namespace=namespace,
            image="python:3.8-slim",
            task_id=f"failing_task_{i}",
            cmds=["python", "-c"],
            arguments=[
                """
import sys
import random

sys.exit(1)
"""
            ],
            security_context=security_context,
            container_resources=combined_resources,
            get_logs=True,
            dag=dag,
        )
        failing_task.set_upstream(cpu_tasks + ram_tasks)
        failing_tasks.append(failing_task)

