#!/bin/bash

# Run this script from root folder of repository, i.e. ./kind/start-dev-env.sh
AIRFLOW_REPO_PATH=$(pwd)

sed -e "s|REPO_PATH|$AIRFLOW_REPO_PATH|g" kind/kind-cluster-conf.yaml | kind create cluster --name airflow --config -

# clone airflow helm chart
mkdir -p /tmp/fonda-infra
git clone https://github.com/CRC-FONDA/s1-fonda-infrastructure /tmp/fonda-infra

helm install airflow /tmp/fonda-infra/charts/airflow --values kind/airflow-helm-values.yaml

echo "Wait for all Pods to be running and start the ./kind/create-admin-user.sh script"
echo "Check with 'kubectl get pods'"
