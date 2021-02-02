#!/bin/bash

# Run this script from root folder of repository, i.e. ./kind/start-dev-env.sh
AIRFLOW_REPO_PATH=$(pwd)

sed -e "s|REPO_PATH|$AIRFLOW_REPO_PATH|g" kind/kind-cluster-conf.yaml | kind create cluster --name airflow --config -

# clone airflow helm chart
# run helm dependency update airflow/

#helm install airflow airflow-stable/airflow --values kind/airflow-helm-values.yaml