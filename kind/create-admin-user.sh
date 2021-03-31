#!/bin/bash

WEB_POD=$(kubectl get pods | grep airflow-web | awk '{ print $1 }')

kubectl exec -it $WEB_POD -- airflow users create --username admin --password admin --firstname admin --lastname admin --role Admin --email admin@admin.de

echo "User 'admin' with password 'admin' was created"
echo "The dashboard is available at http://localhost:8888"