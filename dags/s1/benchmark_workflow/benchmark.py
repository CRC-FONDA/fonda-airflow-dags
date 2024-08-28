from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s
import time

# Kubernetes config: namespace, resources, volume and volume_mounts
namespace = "default"

# Define resources for the task: 1 CPU requested and limited
cpu_intensive_resources = k8s.V1ResourceRequirements(
    requests={
        "cpu": "1",  # Request exactly 1 CPU
        "memory": "1Gi",  # Request 1Gi of memory (adjust as needed)
    },
    limits={
        "cpu": "1",  # Limit the task to 1 CPU
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
    "cpu_intensive_task_15_min",
    default_args=default_args,
    description="A simple DAG to run a CPU intensive task for 15 minutes",
    schedule_interval="@once",
    start_date=days_ago(1),
    max_active_tasks=1,
    tags=["example"],
) as dag:

    cpu_intensive_task = KubernetesPodOperator(
        name="cpu_intensive_task_15_min",
        namespace=namespace,
        image="python:3.8-slim",
        labels={"workflow": "cpu_intensive_task"},
        task_id="cpu_intensive_task",
        cmds=["python", "-c"],
        arguments=[
            """
import time
import multiprocessing

def cpu_bound_task():
    result = 0
    start_time = time.time()
    # Run the loop for 15 minutes
    while time.time() - start_time < 15 * 60:
        for i in range(1, 1000000):
            result += i
    print(f"Result: {result}")

if __name__ == "__main__":
    cpu_bound_task()
            """
        ],
        security_context=security_context,
        container_resources=cpu_intensive_resources,
        get_logs=True,
        dag=dag,
    )

