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

# Define resources for combined CPU and RAM intensive tasks: 0.5 CPU and 1 GB RAM each
combined_resources = k8s.V1ResourceRequirements(
    requests={"cpu": "0.5", "memory": "1Gi"},
    limits={"cpu": "0.5", "memory": "1Gi"}
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
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "cpu_and_ram_intensive_tasks_with_random_failing",
    default_args=default_args,
    description="A DAG with random failing tasks and dependencies",
    schedule_interval="@once",
    start_date=days_ago(1),
    max_active_tasks=5,
    tags=["example"],
) as dag:

    # 3 Randomly failing tasks with 50% failure rate
    failing_tasks = []
    for i in range(1, 4):
        failing_task = KubernetesPodOperator(
            name=f"failing_task_{i}",
            namespace=namespace,
            image="python:3.8-slim",
            labels={"workflow": "failing_task"},
            task_id=f"failing_task_{i}",
            cmds=["python", "-c"],
            affinity=experiment_affinity,
            arguments=[
                '''
import sys
import random

if random.random() < 0.5:
    print("Task failed intentionally")
    sys.exit(1)
else:
    print("Task succeeded")
                '''
            ],
            security_context=security_context,
            container_resources=combined_resources,
            get_logs=True,
            dag=dag,
        )
        failing_tasks.append(failing_task)

    # Adjust dependencies: The failing tasks should run after all CPU and RAM intensive tasks
    for failing_task in failing_tasks:
        failing_task.set_upstream(cpu_tasks + ram_tasks)

    # Set dependencies: Combined tasks depend on CPU and RAM tasks
    combined_task_1.set_upstream(cpu_tasks + ram_tasks)
    combined_task_2.set_upstream(cpu_tasks + ram_tasks)

